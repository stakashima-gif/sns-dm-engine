"""
全SNS対応 自動DM ＋ 自動投稿 エンジン（マルチワークスペース版）
======================================================================
- 各メンバー = 1ワークスペース。URL /w/<member> ごとにデータ・上限・SNS認証・トークンを分離。
- 各自オープン作成：未作成の名前はトークンを設定して新規作成、以降はトークンで開く。
- DM=X/Instagram/Facebook、投稿=X/FB/IG、文面生成=Claude API（共通ANTHROPIC_API_KEY）。

保存レイアウト: DATA_DIR/ws/<member>/{meta,state,creds,send_state,campaigns,posts}.json
デプロイ: gunicorn server:app --timeout 120 --workers 1
"""

import os
import re
import time
import json
import shutil
import threading

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from requests_oauthlib import OAuth1Session
import requests

app = Flask(__name__)
CORS(app, expose_headers=["X-Member", "X-Token"])

GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v21.0")
DATA_DIR = os.environ.get("DATA_DIR", ".")
MASTER_SECRET = os.environ.get("ENGINE_SECRET", "")  # cron(/drain)用マスター
_lock = threading.Lock()

WS_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,31}$")
CRED_KEYS = ["X_API_KEY", "X_API_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_SECRET",
             "FB_PAGE_TOKEN", "FB_PAGE_ID", "IG_PAGE_TOKEN", "IG_USER_ID"]


# ============================================================
# ワークスペース別ストレージ
# ============================================================
def _today():
    return time.strftime("%Y-%m-%d")


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _hour():
    return int(time.strftime("%H"))


def ws_dir(m):
    return os.path.join(DATA_DIR, "ws", m)


def _wp(m, name):
    return os.path.join(ws_dir(m), name)


def _lw(m, name, default):
    try:
        with open(_wp(m, name), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _sw(m, name, data):
    os.makedirs(ws_dir(m), exist_ok=True)
    tmp = _wp(m, name) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, _wp(m, name))


def valid_member(m):
    return bool(m) and bool(WS_RE.match(m or ""))


def ws_exists(m):
    return _lw(m, "meta.json", None) is not None


def check_token(m, tok):
    meta = _lw(m, "meta.json", None)
    return meta is not None and bool(tok) and meta.get("token") == tok


def list_members():
    base = os.path.join(DATA_DIR, "ws")
    try:
        return [d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))]
    except Exception:
        return []


# ---- 日次送信カウンタ（ワークスペース別・ガイドライン上限ガード） ----
def sent_today(m, platform):
    return _lw(m, "send_state.json", {}).get(_today(), {}).get(platform, 0)


def bump(m, platform, n=1):
    with _lock:
        st = _lw(m, "send_state.json", {})
        day = st.setdefault(_today(), {})
        day[platform] = day.get(platform, 0) + n
        for k in list(st.keys()):
            if k != _today():
                del st[k]
        _sw(m, "send_state.json", st)


# ============================================================
# DM Sender（ワークスペースのcredsから生成）
# ============================================================
class SenderError(Exception):
    pass


class XSender:
    def __init__(self, creds):
        for k in ("X_API_KEY", "X_API_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_SECRET"):
            if not creds.get(k):
                raise SenderError(f"X認証 {k} が未設定です")
        self.session = OAuth1Session(
            creds["X_API_KEY"], client_secret=creds["X_API_SECRET"],
            resource_owner_key=creds["X_ACCESS_TOKEN"],
            resource_owner_secret=creds["X_ACCESS_SECRET"],
        )
        self._id_cache = {}

    def resolve_user_id(self, handle):
        username = handle.lstrip("@").strip()
        if username in self._id_cache:
            return self._id_cache[username]
        r = self.session.get(f"https://api.twitter.com/2/users/by/username/{username}")
        if r.status_code == 429:
            raise SenderError("レート制限(429)")
        if r.status_code != 200:
            raise SenderError(f"ユーザー解決失敗 {r.status_code}: {r.text[:160]}")
        data = r.json()
        if "data" not in data:
            raise SenderError(f"ユーザーが見つかりません: @{username}")
        uid = data["data"]["id"]
        self._id_cache[username] = uid
        return uid

    def send(self, handle, text):
        uid = self.resolve_user_id(handle)
        r = self.session.post(
            f"https://api.twitter.com/2/dm_conversations/with/{uid}/messages",
            json={"text": text})
        if r.status_code == 429:
            raise SenderError("レート制限(429)")
        if r.status_code not in (200, 201):
            raise SenderError(f"送信失敗 {r.status_code}: {r.text[:160]}")
        return r.json()


class MetaDMSender:
    """宛先(handle)は PSID/IGSID。Opt-in＋24hウィンドウ内のみ。"""

    def __init__(self, creds, token_key, id_key=None, ig=False):
        self.token = creds.get(token_key)
        if not self.token:
            raise SenderError(f"{token_key} が未設定です")
        self.node = creds.get(id_key) if id_key else "me"
        if ig and not self.node:
            raise SenderError(f"{id_key} が未設定です")

    def send(self, recipient_id, text):
        url = f"https://graph.facebook.com/{GRAPH_VERSION}/{self.node}/messages"
        payload = {"recipient": {"id": recipient_id.strip()},
                   "message": {"text": text}, "messaging_type": "RESPONSE"}
        r = requests.post(url, params={"access_token": self.token}, json=payload, timeout=30)
        if r.status_code not in (200, 201):
            raise SenderError(f"送信失敗 {r.status_code}: {r.text[:180]}")
        return r.json()


def build_sender(member, platform):
    creds = _lw(member, "creds.json", {})
    if platform == "x":
        return XSender(creds)
    if platform == "facebook":
        return MetaDMSender(creds, "FB_PAGE_TOKEN")
    if platform == "instagram":
        return MetaDMSender(creds, "IG_PAGE_TOKEN", id_key="IG_USER_ID", ig=True)
    raise SenderError(f"未対応のSNSです: {platform}（DMは x / facebook / instagram）")


# ---- ターゲット抽出（キーワード検索）----
# 公式検索APIが実用になるのは X のみ。他SNSは検索API無し/DM不可のため非対応。
EXTRACT_UNSUPPORTED = {
    "instagram": "Instagramはハッシュタグ検索のみで、抽出したユーザーへのコールドDMが規約上できないため抽出非対応です。",
    "facebook": "Facebookは公式のユーザー/投稿検索APIが無いため抽出非対応です。",
    "tiktok": "TikTokは一般ユーザー検索APIが無いため抽出非対応です。",
    "threads": "Threadsは公式検索APIが無いため抽出非対応です。",
    "linkedin": "LinkedInは公式検索APIが無いため抽出非対応です。",
}


def x_search(session, keywords, want, region="", min_followers=0):
    """X 公式APIでキーワード検索し、投稿者を抽出（要 Basic/Pro tier）。"""
    query = keywords.strip() + " -is:retweet"
    params = {
        "query": query, "max_results": 100, "expansions": "author_id",
        "user.fields": "name,username,location,public_metrics,description",
    }
    collected = {}
    pages = 0
    next_token = None
    while len(collected) < want and pages < 5:
        if next_token:
            params["next_token"] = next_token
        r = session.get("https://api.twitter.com/2/tweets/search/recent", params=params)
        if r.status_code == 429:
            raise SenderError("レート制限(429): 時間をおいて再試行してください")
        if r.status_code == 403:
            raise SenderError("検索APIの権限がありません（X APIのBasic/Proプランが必要です）")
        if r.status_code != 200:
            raise SenderError(f"検索失敗 {r.status_code}: {r.text[:180]}")
        data = r.json()
        for u in data.get("includes", {}).get("users", []):
            uid = u.get("id")
            if uid in collected:
                continue
            loc = u.get("location", "") or ""
            fol = u.get("public_metrics", {}).get("followers_count", 0)
            if region and region not in loc:
                continue
            if min_followers and fol < min_followers:
                continue
            collected[uid] = u
            if len(collected) >= want:
                break
        next_token = data.get("meta", {}).get("next_token")
        pages += 1
        if not next_token:
            break
    return [{
        "handle": "@" + u.get("username", ""),
        "name": u.get("name", ""),
        "company": (u.get("description", "") or "").replace("\n", " ")[:60],
        "region": u.get("location", "") or "",
        "followers": u.get("public_metrics", {}).get("followers_count", 0),
    } for u in collected.values()]


# ============================================================
# Poster（自動投稿）
# ============================================================
class XPoster:
    def __init__(self, creds):
        self.s = XSender(creds).session

    def post(self, text, image_url=None):
        r = self.s.post("https://api.twitter.com/2/tweets", json={"text": text})
        if r.status_code not in (200, 201):
            raise SenderError(f"投稿失敗 {r.status_code}: {r.text[:180]}")
        return r.json()


class FacebookPoster:
    def __init__(self, creds):
        self.token = creds.get("FB_PAGE_TOKEN")
        self.page = creds.get("FB_PAGE_ID")
        if not self.token or not self.page:
            raise SenderError("FB_PAGE_TOKEN / FB_PAGE_ID が未設定です")

    def post(self, text, image_url=None):
        base = f"https://graph.facebook.com/{GRAPH_VERSION}/{self.page}"
        if image_url:
            r = requests.post(f"{base}/photos", params={"access_token": self.token},
                              json={"url": image_url, "caption": text}, timeout=30)
        else:
            r = requests.post(f"{base}/feed", params={"access_token": self.token},
                              json={"message": text}, timeout=30)
        if r.status_code not in (200, 201):
            raise SenderError(f"投稿失敗 {r.status_code}: {r.text[:180]}")
        return r.json()


class InstagramPoster:
    def __init__(self, creds):
        self.token = creds.get("IG_PAGE_TOKEN")
        self.user = creds.get("IG_USER_ID")
        if not self.token or not self.user:
            raise SenderError("IG_PAGE_TOKEN / IG_USER_ID が未設定です")

    def post(self, text, image_url=None):
        if not image_url:
            raise SenderError("Instagram投稿には画像URL(image_url)が必須です")
        base = f"https://graph.facebook.com/{GRAPH_VERSION}/{self.user}"
        c = requests.post(f"{base}/media", params={"access_token": self.token},
                          json={"image_url": image_url, "caption": text}, timeout=30)
        if c.status_code not in (200, 201):
            raise SenderError(f"コンテナ作成失敗 {c.status_code}: {c.text[:180]}")
        cid = c.json().get("id")
        p = requests.post(f"{base}/media_publish", params={"access_token": self.token},
                          json={"creation_id": cid}, timeout=30)
        if p.status_code not in (200, 201):
            raise SenderError(f"公開失敗 {p.status_code}: {p.text[:180]}")
        return p.json()


_POSTERS = {"x": XPoster, "facebook": FacebookPoster, "instagram": InstagramPoster}


def build_poster(member, platform):
    if platform not in _POSTERS:
        raise SenderError(f"投稿未対応のSNSです: {platform}")
    return _POSTERS[platform](_lw(member, "creds.json", {}))


# ============================================================
# Claude API で投稿文面を生成（共通キー）
# ============================================================
_CHAR_LIMIT = {"x": 280, "instagram": 2200, "facebook": 2000, "threads": 500, "tiktok": 150}
_POST_SCHEMA = {
    "type": "object",
    "properties": {"posts": {"type": "array", "items": {
        "type": "object",
        "properties": {"text": {"type": "string"},
                       "hashtags": {"type": "array", "items": {"type": "string"}}},
        "required": ["text", "hashtags"], "additionalProperties": False}}},
    "required": ["posts"], "additionalProperties": False,
}


def generate_posts(brief, platform="x", tone="", count=3, use_hashtags=True):
    try:
        import anthropic
    except ImportError:
        raise SenderError("anthropic パッケージが未インストールです")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SenderError("ANTHROPIC_API_KEY が未設定です")
    client = anthropic.Anthropic()
    limit = _CHAR_LIMIT.get(platform, 500)
    system = ("あなたは日本語SNSマーケティングの専門コピーライターです。"
              "指示テーマに沿って、読み手の関心を引く自然で規約準拠の投稿文を作成します。"
              "過度な誇大表現・スパム的表現・誤解を招く断定は避けてください。")
    user = (f"SNS: {platform}（1投稿の目安 {limit} 文字以内）\n生成本数: {count}\n"
            f"トーン: {tone or '指定なし（自然体）'}\n"
            f"ハッシュタグ: {'付ける（textには含めずhashtags配列に）' if use_hashtags else '不要（hashtagsは空配列）'}\n\n"
            f"投稿テーマ・指示:\n{brief}\n\n{count}本の異なる切り口の投稿案を作成してください。")
    resp = client.messages.create(
        model="claude-opus-4-8", max_tokens=2000, system=system,
        output_config={"format": {"type": "json_schema", "schema": _POST_SCHEMA}},
        messages=[{"role": "user", "content": user}])
    text = next(b.text for b in resp.content if b.type == "text")
    return json.loads(text)["posts"]


# ============================================================
# 送信バッチ（/send と /drain 共用）
# ============================================================
def _run_batch(member, sender, platform, cap, min_gap, max_seconds, messages, start):
    results = []
    deferred = False
    for i, m in enumerate(messages):
        handle = (m or {}).get("handle", "")
        text = (m or {}).get("text", "")
        if sent_today(member, platform) >= cap:
            results.append({"handle": handle, "status": "skipped", "error": "daily cap reached"})
            continue
        if deferred or (results and (time.time() - start) > max_seconds):
            deferred = True
            results.append({"handle": handle, "status": "deferred"})
            continue
        try:
            sender.send(handle, text)
            bump(member, platform)
            results.append({"handle": handle, "status": "sent"})
        except SenderError as e:
            results.append({"handle": handle, "status": "error", "error": str(e)})
        except Exception as e:
            results.append({"handle": handle, "status": "error", "error": f"unexpected: {e}"})
        if i < len(messages) - 1 and min_gap > 0:
            time.sleep(min_gap)
    return results


def _fill(tpl, c):
    return (tpl or "").replace("{{name}}", c.get("name") or c.get("handle") or "").replace(
        "{{company}}", c.get("company") or "")


# ============================================================
# 認証ヘルパ
# ============================================================
def _wsauth(body):
    """POST用: body {member, token} を検証。(member, errorタプル)"""
    m = body.get("member")
    if not valid_member(m):
        return None, ("invalid member", 400)
    if not check_token(m, body.get("token")):
        return None, ("unauthorized", 401)
    return m, None


def _hdrauth():
    """GET用: X-Member / X-Token ヘッダを検証。member or None"""
    m = request.headers.get("X-Member")
    if not valid_member(m) or not check_token(m, request.headers.get("X-Token")):
        return None
    return m


def _master(body):
    """管理者: マスターシークレット照合"""
    return bool(MASTER_SECRET) and body.get("secret") == MASTER_SECRET


# ============================================================
# 配信 / 稼働
# ============================================================
def _serve_dashboard():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    if not os.path.exists(path):
        return "dashboard.html が見つかりません", 404
    return send_file(path)


@app.route("/")
def root():
    return _serve_dashboard()


@app.route("/w/<member>")
def workspace_page(member):
    return _serve_dashboard()


@app.route("/admin")
def admin_page():
    return _serve_dashboard()


@app.route("/health")
def health():
    return jsonify(ok=True, dm=["x", "facebook", "instagram"], post=list(_POSTERS.keys()))


# ---- ワークスペース作成 / 認証 ----
@app.route("/api/workspace", methods=["POST"])
def api_workspace():
    body = request.get_json(force=True, silent=True) or {}
    m = body.get("member")
    tok = body.get("token", "")
    mode = body.get("mode", "open")
    if not valid_member(m):
        return jsonify(ok=False, error="名前は英小文字・数字・_-、2〜32文字"), 400
    if not tok or len(tok) < 4:
        return jsonify(ok=False, error="トークンは4文字以上"), 400
    if mode == "create":
        if ws_exists(m):
            return jsonify(ok=False, error="この名前は既に使われています。トークンで開いてください。", exists=True), 409
        with _lock:
            _sw(m, "meta.json", {"token": tok, "created_at": _now_iso()})
        return jsonify(ok=True, created=True)
    # open
    if not ws_exists(m):
        return jsonify(ok=False, error="ワークスペースが存在しません。新規作成してください。", exists=False), 404
    if not check_token(m, tok):
        return jsonify(ok=False, error="トークンが違います"), 401
    return jsonify(ok=True)


# ---- 状態（ダッシュボードDB）----
@app.route("/api/state", methods=["GET"])
def api_state_get():
    m = _hdrauth()
    if not m:
        return jsonify(error="unauthorized"), 401
    return jsonify(_lw(m, "state.json", {}))


@app.route("/api/state", methods=["PUT"])
def api_state_put():
    m = _hdrauth()
    if not m:
        return jsonify(error="unauthorized"), 401
    state = request.get_json(force=True, silent=True) or {}
    _sw(m, "state.json", state)
    return jsonify(ok=True)


# ---- SNS認証情報（サーバー側のみ保持・値は返さない）----
@app.route("/api/creds", methods=["GET"])
def api_creds_get():
    m = _hdrauth()
    if not m:
        return jsonify(error="unauthorized"), 401
    creds = _lw(m, "creds.json", {})
    return jsonify(status={k: bool(creds.get(k)) for k in CRED_KEYS})


@app.route("/api/creds", methods=["POST"])
def api_creds_post():
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    incoming = body.get("creds", {}) or {}
    with _lock:
        creds = _lw(m, "creds.json", {})
        for k in CRED_KEYS:
            if k in incoming:
                v = (incoming.get(k) or "").strip()
                if v:
                    creds[k] = v
                elif incoming.get(k) == "":
                    creds.pop(k, None)  # 明示的な空文字でクリア
        _sw(m, "creds.json", creds)
    return jsonify(ok=True, status={k: bool(creds.get(k)) for k in CRED_KEYS})


# ---- 手動DM送信 ----
@app.route("/send", methods=["POST"])
def send():
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    platform = body.get("platform", "x")
    cap = int(body.get("cap", 40))
    min_gap = float(body.get("min_gap_sec", 8))
    max_seconds = float(body.get("max_seconds", 110))
    messages = body.get("messages", []) or []
    try:
        sender = build_sender(m, platform)
    except SenderError as e:
        return jsonify(error=str(e)), 400
    results = _run_batch(m, sender, platform, cap, min_gap, max_seconds, messages, time.time())
    return jsonify(platform=platform, results=results,
                   sent_today=sent_today(m, platform),
                   remaining=max(0, cap - sent_today(m, platform)))


@app.route("/extract", methods=["POST"])
def extract():
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    platform = body.get("platform", "x")
    keywords = (body.get("keywords", "") or "").strip()
    if not keywords:
        return jsonify(error="キーワードを入力してください"), 400
    if platform != "x":
        return jsonify(supported=False,
                       error=EXTRACT_UNSUPPORTED.get(platform, "このSNSは抽出非対応です。")), 200
    want = min(int(body.get("count", 50) or 50), 200)
    region = (body.get("region", "") or "").strip()
    min_f = int(body.get("min_followers", 0) or 0)
    try:
        sender = build_sender(m, "x")
        results = x_search(sender.session, keywords, want, region, min_f)
    except SenderError as e:
        return jsonify(error=str(e)), 400
    except Exception as e:
        return jsonify(error=f"抽出失敗: {e}"), 500
    return jsonify(supported=True, candidates=results, count=len(results))


@app.route("/status")
def status():
    m = _hdrauth()
    if not m:
        return jsonify(error="unauthorized"), 401
    pf = request.args.get("platform", "x")
    cap = int(request.args.get("cap", "40"))
    used = sent_today(m, pf)
    return jsonify(platform=pf, sent_today=used, cap=cap, remaining=max(0, cap - used))


# ---- キャンペーン ----
@app.route("/campaigns", methods=["GET"])
def campaigns_get():
    m = _hdrauth()
    if not m:
        return jsonify(error="unauthorized"), 401
    camps = _lw(m, "campaigns.json", [])
    out = [{**{k: v for k, v in c.items() if k != "recipients"},
            "total": len(c.get("recipients", [])), "sent": len(c.get("sent", []))} for c in camps]
    return jsonify(campaigns=out)


@app.route("/campaigns", methods=["POST"])
def campaigns_post():
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    with _lock:
        camps = _lw(m, "campaigns.json", [])
        camp = {
            "id": body.get("id") or f"camp_{int(time.time())}",
            "name": body.get("name", "campaign"),
            "platform": body.get("platform", "x"),
            "cap": int(body.get("cap", 40)),
            "min_gap_sec": float(body.get("min_gap_sec", 8)),
            "template": body.get("template", ""),
            "window": body.get("window", {"start": 9, "end": 19}),
            "recipients": body.get("recipients", []),
            "sent": [],
            "active": bool(body.get("active", True)),
        }
        camps = [c for c in camps if c["id"] != camp["id"]]
        camps.insert(0, camp)
        _sw(m, "campaigns.json", camps)
    return jsonify(ok=True, id=camp["id"], total=len(camp["recipients"]))


@app.route("/campaigns/<cid>", methods=["DELETE"])
def campaigns_delete(cid):
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    with _lock:
        _sw(m, "campaigns.json", [c for c in _lw(m, "campaigns.json", []) if c["id"] != cid])
    return jsonify(ok=True)


# ---- 予約投稿 ----
@app.route("/posts", methods=["GET"])
def posts_get():
    m = _hdrauth()
    if not m:
        return jsonify(error="unauthorized"), 401
    return jsonify(posts=_lw(m, "posts.json", []))


@app.route("/posts", methods=["POST"])
def posts_post():
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    with _lock:
        items = _lw(m, "posts.json", [])
        item = {"id": f"post_{int(time.time()*1000)}", "platform": body.get("platform", "x"),
                "text": body.get("text", ""), "image_url": body.get("image_url", ""),
                "publish_at": body.get("publish_at", _now_iso()),
                "status": "pending", "error": "", "created_at": _now_iso()}
        items.insert(0, item)
        _sw(m, "posts.json", items)
    return jsonify(ok=True, id=item["id"])


@app.route("/posts/<pid>", methods=["DELETE"])
def posts_delete(pid):
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    with _lock:
        _sw(m, "posts.json", [p for p in _lw(m, "posts.json", []) if p["id"] != pid])
    return jsonify(ok=True)


@app.route("/post", methods=["POST"])
def post_now():
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    try:
        build_poster(m, body.get("platform", "x")).post(body.get("text", ""), body.get("image_url") or None)
    except SenderError as e:
        return jsonify(error=str(e)), 400
    except Exception as e:
        return jsonify(error=f"unexpected: {e}"), 500
    return jsonify(ok=True)


@app.route("/generate-post", methods=["POST"])
def generate_post():
    body = request.get_json(force=True, silent=True) or {}
    m, err = _wsauth(body)
    if err:
        return jsonify(error=err[0]), err[1]
    try:
        out = generate_posts(brief=body.get("brief", ""), platform=body.get("platform", "x"),
                             tone=body.get("tone", ""), count=int(body.get("count", 3)),
                             use_hashtags=bool(body.get("hashtags", True)))
    except SenderError as e:
        return jsonify(error=str(e)), 400
    except Exception as e:
        return jsonify(error=f"生成失敗: {e}"), 500
    return jsonify(posts=out)


# ============================================================
# cron: 全ワークスペースのDMキャンペーン＋予約投稿を自動処理
# ============================================================
@app.route("/drain", methods=["POST"])
def drain():
    body = request.get_json(force=True, silent=True) or {}
    if not MASTER_SECRET or body.get("secret") != MASTER_SECRET:
        return jsonify(error="unauthorized"), 401
    max_seconds = float(body.get("max_seconds", 110))
    start = time.time()
    summary = {}
    hour = _hour()
    now = _now_iso()

    for member in list_members():
        ms = {"dm": [], "posts": []}
        # DMキャンペーン
        for camp in _lw(member, "campaigns.json", []):
            if not camp.get("active"):
                continue
            w = camp.get("window", {"start": 0, "end": 24})
            if not (w.get("start", 0) <= hour < w.get("end", 24)):
                ms["dm"].append({"id": camp["id"], "skipped": "時間帯外"})
                continue
            pf = camp["platform"]
            sent_set = set(camp.get("sent", []))
            pending = [r for r in camp.get("recipients", []) if r.get("handle") not in sent_set]
            rem = max(0, camp["cap"] - sent_today(member, pf))
            if not pending or rem <= 0:
                ms["dm"].append({"id": camp["id"], "sent": 0, "remaining_daily": rem})
                continue
            try:
                sender = build_sender(member, pf)
            except SenderError as e:
                ms["dm"].append({"id": camp["id"], "error": str(e)})
                continue
            msgs = [{"handle": r.get("handle"), "text": _fill(camp.get("template"), r)} for r in pending]
            results = _run_batch(member, sender, pf, camp["cap"], camp["min_gap_sec"],
                                 max(5, max_seconds - (time.time() - start)), msgs, start)
            ok = [r["handle"] for r in results if r["status"] == "sent"]
            if ok:
                with _lock:
                    cs = _lw(member, "campaigns.json", [])
                    for c in cs:
                        if c["id"] == camp["id"]:
                            c["sent"] = list(set(c.get("sent", []) + ok))
                    _sw(member, "campaigns.json", cs)
            ms["dm"].append({"id": camp["id"], "sent": len(ok),
                             "errors": len([r for r in results if r["status"] == "error"])})

        # 予約投稿
        items = _lw(member, "posts.json", [])
        changed = False
        for p in items:
            if p["status"] != "pending" or p.get("publish_at", now) > now:
                continue
            if (time.time() - start) > max_seconds:
                break
            try:
                build_poster(member, p["platform"]).post(p["text"], p.get("image_url") or None)
                p["status"] = "posted"
                p["posted_at"] = _now_iso()
            except Exception as e:
                p["status"] = "error"
                p["error"] = str(e)[:200]
            changed = True
            ms["posts"].append({"id": p["id"], "status": p["status"]})
        if changed:
            with _lock:
                _sw(member, "posts.json", items)

        if ms["dm"] or ms["posts"]:
            summary[member] = ms

    return jsonify(ok=True, at=_now_iso(), members=len(list_members()), summary=summary)


# ============================================================
# 管理者API（マスターシークレット認証）
# ============================================================
@app.route("/api/admin/members", methods=["POST"])
def admin_members():
    body = request.get_json(force=True, silent=True) or {}
    if not _master(body):
        return jsonify(error="unauthorized"), 401
    out = []
    for m in list_members():
        meta = _lw(m, "meta.json", {}) or {}
        state = _lw(m, "state.json", {})
        camps = _lw(m, "campaigns.json", [])
        posts = _lw(m, "posts.json", [])
        sd = _lw(m, "send_state.json", {}).get(_today(), {})
        out.append({
            "member": m, "created_at": meta.get("created_at"), "token": meta.get("token", ""),
            "contacts": len(state.get("contacts", [])), "lists": len(state.get("lists", [])),
            "campaigns": len(camps), "posts": len(posts),
            "sent_today": sum(sd.values()) if isinstance(sd, dict) else 0,
        })
    out.sort(key=lambda x: x.get("created_at") or "")
    return jsonify(members=out)


@app.route("/api/admin/create", methods=["POST"])
def admin_create():
    body = request.get_json(force=True, silent=True) or {}
    if not _master(body):
        return jsonify(error="unauthorized"), 401
    m = body.get("member")
    tok = body.get("token", "")
    if not valid_member(m):
        return jsonify(error="名前は英小文字・数字・_-、2〜32文字"), 400
    if len(tok) < 4:
        return jsonify(error="トークンは4文字以上"), 400
    if ws_exists(m):
        return jsonify(error="既に存在します"), 409
    with _lock:
        _sw(m, "meta.json", {"token": tok, "created_at": _now_iso()})
    return jsonify(ok=True)


@app.route("/api/admin/reset", methods=["POST"])
def admin_reset():
    body = request.get_json(force=True, silent=True) or {}
    if not _master(body):
        return jsonify(error="unauthorized"), 401
    m = body.get("member")
    tok = body.get("token", "")
    if not valid_member(m) or not ws_exists(m):
        return jsonify(error="ワークスペースが存在しません"), 404
    if len(tok) < 4:
        return jsonify(error="トークンは4文字以上"), 400
    with _lock:
        meta = _lw(m, "meta.json", {}) or {}
        meta["token"] = tok
        _sw(m, "meta.json", meta)
    return jsonify(ok=True)


@app.route("/api/admin/delete", methods=["POST"])
def admin_delete():
    body = request.get_json(force=True, silent=True) or {}
    if not _master(body):
        return jsonify(error="unauthorized"), 401
    m = body.get("member")
    if not valid_member(m):
        return jsonify(error="invalid member"), 400
    with _lock:
        shutil.rmtree(ws_dir(m), ignore_errors=True)
    return jsonify(ok=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
