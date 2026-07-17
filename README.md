# SNS DM ＋ 自動投稿 エンジン（マルチワークスペース版）

ダッシュボード（同梱 `dashboard.html`）を配信し、DM送信・自動投稿を実行するバックエンドです。
**メンバーごとに独立したワークスペース**を持ち、URL `.../w/<名前>` ごとにデータ・上限・SNS認証・トークンを分離します。

| 機能 | 対応SNS | 方式 |
|---|---|---|
| DM送信 | X / Instagram / Facebook | 公式API、ワークスペース別に上限監視 |
| キャンペーン自動化 | 同上 | リストをサーバー保持 → cron `/drain` が毎日自動ドレイン |
| 自動投稿 | X / Facebook / Instagram | Claude APIで文面生成 → 即時／予約投稿 |

---

## メンバーの使い方（デプロイ後）

1. `https://xxxx.onrender.com/` を開く → 名前（英小文字・数字・`_ -`）を入力
2. 初回はトークン（合言葉）を設定して**新規作成** → 自分のワークスペース `.../w/<名前>` へ
3. 「設定 → SNS認証情報」で自分のX/Meta APIキーを入力（サーバー保存・ブラウザには残らない）
4. ターゲット→リスト→送信／自動投稿。以降は同じURL＋トークンでどの端末からでも同じデータにアクセス

> 各ワークスペースは**別々のトークン**で保護され、データ・送信上限も独立です。

---

## 1. サーバー全体の環境変数（デプロイ管理者が1回）

| 変数 | 用途 |
|---|---|
| `ENGINE_SECRET` | cron `/drain` 用のマスターシークレット |
| `DATA_DIR` | ワークスペース別データの保存先（Render永続ディスク） |
| `ANTHROPIC_API_KEY` | 投稿文面の生成（全ワークスペース共通） |

各SNSのAPIキーは環境変数**ではなく**、各メンバーがダッシュボードで入力します
（`DATA_DIR/ws/<member>/creds.json` にワークスペース別保存）。

### SNS APIキーの取得（各メンバー向け案内）
- **X**: developer.x.com でアプリ作成 → 権限「Read and write and Direct message」→ API Key/Secret・Access Token/Secret（権限変更後に再生成）
- **Meta**: Facebookページのページトークン(`FB_PAGE_TOKEN`,`FB_PAGE_ID`)、Instagramプロアカウントの`IG_USER_ID`,`IG_PAGE_TOKEN`
  - ⚠️ Meta DMは原則 **Opt-in＋24hウィンドウ**、宛先は`@ハンドル`でなく**PSID/IGSID**。IG投稿は画像URL必須。

---

## 2. ローカルで試す

```bash
cd dm-engine
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export ENGINE_SECRET=local ANTHROPIC_API_KEY=... DATA_DIR=./data
python server.py            # http://localhost:5000/ を開く
```

---

## 3. Renderにデプロイ（cron込み）

1. このフォルダをGitHubにpush（`dashboard.html` も含める）
2. Render → **New → Blueprint**（`render.yaml`：webサービス＋cronの2つ）
3. 環境変数 `ENGINE_SECRET` / `ANTHROPIC_API_KEY` を設定
4. cronサービスの `ENGINE_URL` にwebサービスのURLを設定（30分ごとに全ワークスペースをドレイン）
5. free プランはスリープ＆ディスク不可のため **starter 以上**

**GitHub Actions**でcronする場合は `.github/workflows/drain.yml`（Secretsに `ENGINE_URL`, `ENGINE_SECRET`）。

---

## エンドポイント仕様

配信: `GET /`（入口）, `GET /w/<member>`（ダッシュボード）

| メソッド | パス | 認証 | 説明 |
|---|---|---|---|
| POST | `/api/workspace` | — | ワークスペース作成/認証 `{member,token,mode:create|open}` |
| GET/PUT | `/api/state` | ヘッダ | ダッシュボードDBの取得/保存（X-Member, X-Token） |
| GET/POST | `/api/creds` | ヘッダ/本文 | SNS認証情報の状態取得/保存（値は返さない） |
| POST | `/send` | member+token | 手動DMバッチ |
| GET/POST/DELETE | `/campaigns` | 同上 | キャンペーン |
| POST | `/generate-post` | 同上 | AI文面生成 |
| GET/POST/DELETE | `/posts` | 同上 | 予約投稿 |
| POST | `/post` | 同上 | 即時投稿 |
| POST | `/drain` | **マスター** | 全ワークスペースの自動処理（cron専用） |

---

## セキュリティ / データ

- 各ワークスペースは個別トークンで保護。トークンはサーバーの `meta.json` に保存。
- SNS認証情報はサーバー側のみ（ブラウザに保持しない）。
- データ（ターゲット/リスト/ログ/上限）は `DATA_DIR/ws/<member>/` にワークスペース別保存。
- 他SNS追加は `build_sender` / `_POSTERS` に実装を足すだけ。
