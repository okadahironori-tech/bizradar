# ウェブサイト監視スクリプト

山一ハガネのウェブサイトを定期的にチェックして、更新があればメールで通知します。

---

## セットアップ手順

### 1. 必要なライブラリをインストール

```bash
pip install -r requirements.txt
```

### 2. Gmailのアプリパスワードを取得

通常のGmailパスワードではなく「アプリパスワード」が必要です。

1. Googleアカウントにログイン → [セキュリティ設定](https://myaccount.google.com/security)
2. 「2段階認証プロセス」を有効にする（まだの場合）
3. 「アプリパスワード」を検索 → 新しいアプリパスワードを生成
4. 生成された16桁のパスワードをコピーしておく

### 3. `monitor.py` の設定を書き換える

ファイルの上部にある `EMAIL_SETTINGS` を自分の情報に変更します：

```python
EMAIL_SETTINGS = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": "your-gmail@gmail.com",        # ← 自分のGmailアドレス
    "sender_password": "xxxx xxxx xxxx xxxx",       # ← 上で取得したアプリパスワード
    "recipient_email": "notify-to@example.com",     # ← 通知を受け取るメールアドレス
}
```

### 4. スクリプトを実行

```bash
python monitor.py
```

初回実行時は「初回記録完了」と表示され、メールは送信されません。  
2回目以降のチェックから、変更があればメールが届きます。

---

## チェック間隔の変更

`monitor.py` の `CHECK_INTERVAL_SECONDS` を変更します：

```python
CHECK_INTERVAL_SECONDS = 3600   # 1時間ごと
CHECK_INTERVAL_SECONDS = 1800   # 30分ごと
CHECK_INTERVAL_SECONDS = 86400  # 1日ごと
```

---

## バックグラウンドで常時実行する方法（Mac）

ターミナルを閉じても動かし続けたい場合：

```bash
nohup python monitor.py > monitor.log 2>&1 &
```

停止するには：
```bash
# プロセスIDを確認
ps aux | grep monitor.py

# 停止（上のコマンドで表示されたPIDを指定）
kill <PID>
```
