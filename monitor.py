"""
ウェブサイト監視スクリプト
機能: サイトの内容が変わったらメールで通知する
"""

import difflib
import hashlib
import smtplib
import time
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

import time as time_module
from urllib.parse import quote

import db
import feedparser
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
    "smtp_server":    os.environ.get("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port":      int(os.environ.get("SMTP_PORT", "587")),
    "sender_email":   os.environ.get("SENDER_EMAIL", ""),
    "sender_password": os.environ.get("SENDER_PASSWORD", ""),
    "recipient_email": os.environ.get("RECIPIENT_EMAIL", ""),
}

DEFAULT_CHECK_INTERVAL = 3600

# ============================================================
# ここより下は変更不要です
# ============================================================


def _rss_entry_link(entry) -> str:
    """feedparser の entry から記事URLを取り出す（Google News の形式差に対応）"""
    url = (entry.get("link") or "").strip()
    if url:
        return url
    for link in entry.get("links", []):
        rel = (link.get("rel") or "").lower()
        if rel in ("alternate", "self") and link.get("href"):
            return str(link["href"]).strip()
    return ""


def fetch_news_articles(keyword: str) -> list:
    """Google News RSSからキーワード関連記事を取得する（最新20件）

    urllib 経由の feedparser 直取得はサーバー環境でブロックされやすいため、
    requests で本文を取得してからパースする。
    """
    rss_url = f"https://news.google.com/rss/search?q={quote(keyword)}&hl=ja&gl=JP&ceid=JP:ja"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }
    try:
        response = requests.get(rss_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"RSSの取得に失敗しました: {e}") from e

    feed = feedparser.parse(response.content)
    if feed.bozo and getattr(feed, "bozo_exception", None):
        print(f"  [警告] RSSの解析に問題があります: {feed.bozo_exception}")
    if not feed.entries:
        print(f"  [警告] RSSの記事が0件です (HTTP {response.status_code}, keyword={keyword!r})")

    articles = []
    for entry in feed.entries[:20]:
        title = entry.get("title", "").strip()
        url = _rss_entry_link(entry)
        source = ""
        if hasattr(entry, "source"):
            source = entry.source.get("title", "")
        if not source and " - " in title:
            title, source = title.rsplit(" - ", 1)
            title = title.strip()
            source = source.strip()
        published = ""
        if entry.get("published_parsed"):
            dt = datetime.fromtimestamp(time_module.mktime(entry.published_parsed))
            published = dt.strftime("%Y-%m-%d %H:%M")
        else:
            published = entry.get("published", "")
        articles.append({
            "keyword":   keyword,
            "title":     title,
            "url":       url,
            "source":    source,
            "published": published,
        })
    return articles


def send_news_email(keyword: str, articles: list):
    """新着ニュース記事をメールで通知する"""
    now = datetime.now().strftime("%Y年%m月%d日 %H:%M")
    subject = f"【ニュース新着通知】「{keyword}」の新着記事 {len(articles)} 件"
    lines = [
        f"「{keyword}」に関する新着記事が {len(articles)} 件見つかりました。",
        "",
        f"検出日時: {now}",
        "",
    ]
    for i, a in enumerate(articles[:10], 1):
        lines.append(f"{i}. {a['title']}")
        if a.get("source"):
            lines.append(f"   出典: {a['source']}")
        if a.get("published"):
            lines.append(f"   日時: {a['published']}")
        lines.append(f"   {a['url']}")
        lines.append("")
    lines += ["---", "このメールはBizRadarにより自動送信されました。"]

    msg = MIMEMultipart()
    msg["From"]    = EMAIL_SETTINGS["sender_email"]
    msg["To"]      = EMAIL_SETTINGS["recipient_email"]
    msg["Subject"] = subject
    msg.attach(MIMEText("\n".join(lines), "plain", "utf-8"))
    try:
        with smtplib.SMTP(EMAIL_SETTINGS["smtp_server"], EMAIL_SETTINGS["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_SETTINGS["sender_email"], EMAIL_SETTINGS["sender_password"])
            server.send_message(msg)
        print(f"[通知] ニュースメールを送信しました → {EMAIL_SETTINGS['recipient_email']}")
    except smtplib.SMTPException as e:
        print(f"[エラー] ニュースメール送信に失敗しました: {e}")


def check_single_keyword(keyword: str, user_id=None):
    """単一キーワードのニュースをチェックしてDBを更新する"""
    print(f"[ニュースチェック] キーワード: {keyword} (user_id={user_id})")
    if user_id is None:
        print("  [エラー] user_id が必要です")
        return

    seen_urls = db.load_article_seen_urls(user_id)
    try:
        articles = fetch_news_articles(keyword)
    except Exception as e:
        print(f"  [エラー] 取得失敗: {e}")
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_articles = []
    for article in articles:
        url = article["url"]
        if url and url not in seen_urls:
            article["found_at"] = now_str
            new_articles.append(article)
            seen_urls.add(url)

    if new_articles:
        print(f"  → {len(new_articles)} 件の新着記事")
        send_news_email(keyword, new_articles)
        db.insert_articles(new_articles, user_id)
    else:
        print(f"  → 新着なし")


def check_all_keywords():
    """全キーワードのニュースをチェックして新着があれば通知する（ユーザーごとに分離）"""
    kw_with_users = db.load_all_keywords_with_users()
    if not kw_with_users:
        return

    print(f"[ニュースチェック開始] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # user_id ごとにキーワードをグループ化
    user_keywords: dict = {}
    for user_id, keyword in kw_with_users:
        user_keywords.setdefault(user_id, []).append(keyword)

    for user_id, keywords in user_keywords.items():
        seen_urls = db.load_article_seen_urls(user_id)

        for keyword in keywords:
            if not keyword:
                continue
            print(f"  キーワード: {keyword} (user_id={user_id})")
            try:
                articles = fetch_news_articles(keyword)
            except Exception as e:
                print(f"  [エラー] 取得失敗: {e}")
                continue

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_articles = []
            for article in articles:
                url = article["url"]
                if url and url not in seen_urls:
                    article["found_at"] = now_str
                    new_articles.append(article)
                    seen_urls.add(url)

            if new_articles:
                print(f"  → {len(new_articles)} 件の新着記事")
                send_news_email(keyword, new_articles)
                db.insert_articles(new_articles, user_id)
            else:
                print(f"  → 新着なし")

    print(f"[ニュースチェック完了]")


def get_page_content(url: str):
    """ウェブページのテキスト内容を取得する。戻り値: (content or None, error_message or None)"""
    headers = {
        "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    }
    try:
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines), None
    except requests.exceptions.SSLError:
        error = "SSL証明書エラー"
    except requests.exceptions.ConnectTimeout:
        error = "接続タイムアウト"
    except requests.exceptions.ReadTimeout:
        error = "読み込みタイムアウト"
    except requests.exceptions.ConnectionError as e:
        msg = str(e)
        if "Connection refused" in msg:
            error = "接続拒否 (Connection refused)"
        elif "Name or service not known" in msg or "getaddrinfo failed" in msg or "nodename nor servname" in msg:
            error = "ホスト名を解決できません (DNS エラー)"
        else:
            error = "接続エラー"
    except requests.exceptions.HTTPError as e:
        error = f"HTTP {e.response.status_code} エラー"
    except requests.exceptions.TooManyRedirects:
        error = "リダイレクトが多すぎます"
    except requests.RequestException as e:
        error = f"取得エラー ({type(e).__name__})"
    print(f"[エラー] {url} の取得に失敗しました: {error}")
    return None, error


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
    now   = datetime.now().strftime("%Y年%m月%d日 %H:%M")
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
    msg["From"]    = EMAIL_SETTINGS["sender_email"]
    msg["To"]      = EMAIL_SETTINGS["recipient_email"]
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
    """単一URLをチェックしてDBを更新する"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"  確認中: {url}")

    previous_hashes = db.load_hashes()
    log             = db.load_monitor_log()
    content_store   = db.load_content_store()

    content, error = get_page_content(url)

    if content is None:
        log["last_checks"][url] = {"timestamp": now_str, "status": "error", "error": error or "不明なエラー"}
    else:
        new_hash = compute_hash(content)

        if url not in previous_hashes:
            print(f"  → 初回記録完了: {url}")
            log["last_checks"][url] = {"timestamp": now_str, "status": "new"}
        elif previous_hashes[url] != new_hash:
            print(f"  → 変更を検出しました！: {url}")
            old_content  = content_store.get(url, "")
            diff_summary = compute_diff_summary(old_content, content) if old_content else []
            send_email(url, site_name)
            log["last_checks"][url] = {"timestamp": now_str, "status": "changed"}
            log["change_history"].insert(0, {
                "timestamp": now_str,
                "url":       url,
                "name":      site_name,
                "diff":      diff_summary,
            })
        else:
            print(f"  → 変更なし")
            log["last_checks"][url] = {"timestamp": now_str, "status": "ok"}

        previous_hashes[url] = new_hash
        content_store[url]   = content[:30000]

    db.save_hashes(previous_hashes)
    db.save_monitor_log(log)
    db.save_content_store(content_store)


def check_all_sites():
    """全URLをチェックして変更があれば通知する"""
    print(f"\n[チェック開始] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sites = db.load_sites()
    for site in sites:
        check_single_site(site["url"], site.get("name", ""))
    print(f"[チェック完了]")


def main():
    print("=" * 50)
    print("ウェブサイト監視スクリプト 起動")
    sites = db.load_sites()
    print(f"監視対象: {[s['url'] for s in sites]}")
    config   = db.load_config()
    interval = config.get("check_interval_seconds", DEFAULT_CHECK_INTERVAL)
    print(f"チェック間隔: {interval // 60} 分ごと")
    print("=" * 50)

    while True:
        check_all_sites()
        check_all_keywords()
        config   = db.load_config()
        interval = config.get("check_interval_seconds", DEFAULT_CHECK_INTERVAL)
        print(f"[待機中] 次回チェックまで {interval // 60} 分待機します...")
        time.sleep(interval)


if __name__ == "__main__":
    main()
