"""
ウェブサイト監視スクリプト
機能: サイトの内容が変わったらメールで通知する
"""

import difflib
import hashlib
import smtplib
import time
import json
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================
# 設定（ここを自分の情報に書き換えてください）
# ============================================================

EMAIL_SETTINGS = {
    "smtp_server": os.environ.get("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.environ.get("SMTP_PORT", "587")),
    "sender_email": os.environ.get("SENDER_EMAIL", ""),
    "sender_password": os.environ.get("SENDER_PASSWORD", ""),
    "recipient_email": os.environ.get("RECIPIENT_EMAIL", ""),
}

DEFAULT_CHECK_INTERVAL = 3600

# ============================================================
# ここより下は変更不要です
# ============================================================

HASH_FILE = "previous_hashes.json"
MONITOR_LOG_FILE = "monitor_log.json"
SITES_FILE = "sites.json"
CONFIG_FILE = "config.json"
CONTENT_STORE_FILE = "content_store.json"


def load_sites() -> list:
    """監視サイトリストを読み込む（[{"url":..., "name":...}]形式）"""
    if os.path.exists(SITES_FILE):
        with open(SITES_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("sites", [])
    return []


def load_config() -> dict:
    """設定ファイルを読み込む"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"check_interval_seconds": DEFAULT_CHECK_INTERVAL}


def load_previous_hashes() -> dict:
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_hashes(hashes: dict):
    with open(HASH_FILE, "w", encoding="utf-8") as f:
        json.dump(hashes, f, ensure_ascii=False, indent=2)


def load_monitor_log() -> dict:
    if os.path.exists(MONITOR_LOG_FILE):
        with open(MONITOR_LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_checks": {}, "change_history": []}


def save_monitor_log(log: dict):
    with open(MONITOR_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def load_content_store() -> dict:
    if os.path.exists(CONTENT_STORE_FILE):
        with open(CONTENT_STORE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_content_store(store: dict):
    with open(CONTENT_STORE_FILE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)


def get_page_content(url: str):
    """ウェブページのテキスト内容を取得する"""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SiteMonitor/1.0)"}
    try:
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)
    except requests.RequestException as e:
        print(f"[エラー] {url} の取得に失敗しました: {e}")
        return None


def compute_hash(content: str) -> str:
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def compute_diff_summary(old_content: str, new_content: str) -> list:
    """変更箇所のサマリーを生成する（最大10件）"""
    old_lines = old_content.splitlines()
    new_lines = new_content.splitlines()
    diff = difflib.unified_diff(old_lines, new_lines, lineterm="", n=0)
    changes = []
    for line in diff:
        if line.startswith("+") and not line.startswith("+++"):
            text = line[1:].strip()
            if text:
                changes.append({"type": "added", "text": text})
        elif line.startswith("-") and not line.startswith("---"):
            text = line[1:].strip()
            if text:
                changes.append({"type": "removed", "text": text})
        if len(changes) >= 10:
            break
    return changes


def send_email(url: str, site_name: str = ""):
    """変更を検知したらメールで通知する"""
    now = datetime.now().strftime("%Y年%m月%d日 %H:%M")
    label = site_name if site_name else url
    subject = f"【サイト更新通知】{label} が更新されました"
    body = f"""
{label} に変更が検出されました。

対象URL: {url}
検出日時: {now}

以下のリンクからサイトをご確認ください:
{url}

---
このメールはウェブサイト監視スクリプトにより自動送信されました。
"""
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SETTINGS["sender_email"]
    msg["To"] = EMAIL_SETTINGS["recipient_email"]
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(EMAIL_SETTINGS["smtp_server"], EMAIL_SETTINGS["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_SETTINGS["sender_email"], EMAIL_SETTINGS["sender_password"])
            server.send_message(msg)
        print(f"[通知] メールを送信しました → {EMAIL_SETTINGS['recipient_email']}")
    except smtplib.SMTPException as e:
        print(f"[エラー] メール送信に失敗しました: {e}")


def check_single_site(url: str, site_name: str = ""):
    """単一URLをチェックしてログを更新する"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"  確認中: {url}")

    previous_hashes = load_previous_hashes()
    log = load_monitor_log()
    content_store = load_content_store()

    content = get_page_content(url)

    if content is None:
        log["last_checks"][url] = {"timestamp": now_str, "status": "error"}
    else:
        new_hash = compute_hash(content)

        if url not in previous_hashes:
            print(f"  → 初回記録完了: {url}")
            log["last_checks"][url] = {"timestamp": now_str, "status": "new"}
        elif previous_hashes[url] != new_hash:
            print(f"  → 変更を検出しました！: {url}")
            old_content = content_store.get(url, "")
            diff_summary = compute_diff_summary(old_content, content) if old_content else []
            send_email(url, site_name)
            log["last_checks"][url] = {"timestamp": now_str, "status": "changed"}
            log["change_history"].insert(0, {
                "timestamp": now_str,
                "url": url,
                "name": site_name,
                "diff": diff_summary,
            })
        else:
            print(f"  → 変更なし")
            log["last_checks"][url] = {"timestamp": now_str, "status": "ok"}

        previous_hashes[url] = new_hash
        content_store[url] = content[:30000]

    save_hashes(previous_hashes)
    save_monitor_log(log)
    save_content_store(content_store)


def check_all_sites():
    """全URLをチェックして変更があれば通知する"""
    print(f"\n[チェック開始] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    sites = load_sites()
    for site in sites:
        check_single_site(site["url"], site.get("name", ""))

    print(f"[チェック完了]")


def main():
    print("=" * 50)
    print("ウェブサイト監視スクリプト 起動")
    sites = load_sites()
    print(f"監視対象: {[s['url'] for s in sites]}")
    config = load_config()
    interval = config.get("check_interval_seconds", DEFAULT_CHECK_INTERVAL)
    print(f"チェック間隔: {interval // 60} 分ごと")
    print("=" * 50)

    while True:
        check_all_sites()
        config = load_config()
        interval = config.get("check_interval_seconds", DEFAULT_CHECK_INTERVAL)
        print(f"[待機中] 次回チェックまで {interval // 60} 分待機します...")
        time.sleep(interval)


if __name__ == "__main__":
    main()
