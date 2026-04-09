# BizRadar 開発指示書

## プロジェクト概要
東海地方（愛知・岐阜・三重）の中小製造業向け企業モニタリングSaaS。
対象企業のWebサイト変更検知・ニュース収集・アラート通知が主機能。

## 環境
- ローカル: ~/Desktop/bizradar-monitor
- 本番: Render（Starterプラン、$7/月）
- URL: bizradar-6h9o.onrender.com
- GitHub: okadahironori-tech/bizradar

## 技術スタック
- バックエンド: Python / Flask
- データベース: PostgreSQL（Render managed）
- テンプレート: Jinja2
- フロントエンド: 静的ファイル（static/）
- デプロイ設定: render.yaml

## 主要ファイル
- dashboard.py : Flaskアプリ本体（ルーティング）
- db.py : DB操作関数
- monitor.py : ニュース収集・サイト監視
- templates/ : Jinja2テンプレート
- static/ : CSS・JS等の静的ファイル
- render.yaml : Renderデプロイ設定
- requirements.txt : Pythonパッケージ一覧

## 実装済み機能
- マルチユーザー認証
- 企業単位ダッシュボード
- Webサイト変更検知（diff表示）
- 重要アラート
- メール通知
- ニュース収集（Google News RSS）
- 既読管理
- 前回ログイン以降の更新企業セクション（last_login_at）

## 開発スタイル（必ず守ること）
- 1機能ずつ実装し、都度ブラウザで動作確認してから次に進む
- 最小限の変更にとどめる。全面書き直しは絶対にしない
- 絵文字・丸数字（①②③）は使用しない
- 変更検知のdiff精度維持のため、保存済みと新規取得の両方に同一フィルタ（extract_main_content()）を適用すること
- UI/UXの細部（ボタン状態・スクロール位置・表示の一貫性）に注意する

## デプロイ手順
1. ローカルで動作確認
2. git add / git commit / git push
3. Renderが自動デプロイ（render.yamlに従う）
4. Render Logsで起動確認

## 現在のベータテスター
- 岩崎氏（中部経済新聞 編集局次長）
- 山口氏（システムオーエヌイー 代表取締役）
- 園田綾氏

## 注意事項
- オープン登録は現時点でセキュリティ上の懸念あり。無制限公開はしない
- モバイルレスポンシブ対応はPC側機能確定後に着手
- Render PostgreSQL無料プランの期限に注意（期限到来時は有料移行）
