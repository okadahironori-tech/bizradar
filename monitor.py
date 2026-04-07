"""
ウェブサイト監視スクリプト
機能: サイトの内容が変わったらメールで通知する
"""

import difflib
import hashlib
import re
import smtplib
import time
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))

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

def _sanitize_text(value) -> str:
    """DB/メール投入前に制御文字（NULなど）を除去して安全にする。"""
    if value is None:
        return ""
    # PostgreSQL/psycopg2 は NUL (0x00) を含む文字列を拒否することがある
    return str(value).replace("\x00", "").strip()


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
    print(f"  [Google News] 取得開始: keyword={keyword!r}")
    try:
        response = requests.get(rss_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  [Google News] 取得失敗: keyword={keyword!r} error={e}")
        raise RuntimeError(f"RSSの取得に失敗しました: {e}") from e

    feed = feedparser.parse(response.content)
    if feed.bozo and getattr(feed, "bozo_exception", None):
        print(f"  [警告] RSSの解析に問題があります: {feed.bozo_exception}")
    if not feed.entries:
        print(f"  [警告] RSSの記事が0件です (HTTP {response.status_code}, keyword={keyword!r})")
    else:
        print(f"  [Google News] 取得完了: keyword={keyword!r} HTTP={response.status_code} 件数={len(feed.entries)}")

    articles = []
    for entry in feed.entries[:20]:
        title = _sanitize_text(entry.get("title", ""))
        url = _sanitize_text(_rss_entry_link(entry))
        source = ""
        if hasattr(entry, "source"):
            source = _sanitize_text(entry.source.get("title", ""))
        if not source and " - " in title:
            title, source = title.rsplit(" - ", 1)
            title = _sanitize_text(title)
            source = _sanitize_text(source)
        published = ""
        if entry.get("published_parsed"):
            dt = datetime.fromtimestamp(time_module.mktime(entry.published_parsed), tz=timezone.utc).astimezone(JST)
            published = dt.strftime("%Y-%m-%d %H:%M")
        else:
            published = entry.get("published", "")
        published = _sanitize_text(published)
        articles.append({
            "keyword":   keyword,
            "title":     title,
            "url":       url,
            "source":    source,
            "published": published,
        })
    return articles


def send_digest_email(user_email: str, articles_by_keyword: dict, alert_kws: set = None):
    """ダイジェストメールを送信する。
    articles_by_keyword: {keyword: [article, ...], ...}
    alert_kws: アラートキーワードの小文字セット
    """
    import html as _html
    if alert_kws is None:
        alert_kws = set()
    total = sum(len(v) for v in articles_by_keyword.values())
    if total == 0:
        return
    now = datetime.now(JST).strftime("%Y年%m月%d日 %H:%M")

    # 重要記事の総数を集計（件名用）
    total_alert = sum(
        1 for arts in articles_by_keyword.values()
        for a in arts if _is_alert(a.get("title", ""), alert_kws)
    )
    alert_prefix = "【重要あり】" if total_alert > 0 else ""
    subject = f"{alert_prefix}【BizRadar ダイジェスト】本日の新着記事 {total} 件"

    # 各キーワードブロック内でも重要記事を上に並び替え
    sections_html = ""
    for keyword, arts in articles_by_keyword.items():
        if not arts:
            continue
        kw_esc = _html.escape(keyword)
        important = [a for a in arts if _is_alert(a.get("title", ""), alert_kws)]
        normal    = [a for a in arts if not _is_alert(a.get("title", ""), alert_kws)]
        sorted_arts = important + normal
        rows = ""
        for a in sorted_arts:
            title_esc = _html.escape(a.get("title", ""))
            url_esc   = _html.escape(a.get("url", ""))
            source    = _html.escape(a.get("source", ""))
            published = _html.escape(a.get("published", ""))
            meta_parts = []
            if source:
                meta_parts.append(f"出典: {source}")
            if published:
                meta_parts.append(f"日時: {published}")
            meta_html = "　".join(meta_parts)
            is_alert_art = _is_alert(a.get("title", ""), alert_kws)
            alert_badge = (
                '<span style="background:#dc2626;color:#fff;font-size:0.75em;'
                'font-weight:700;padding:1px 7px;border-radius:4px;margin-right:6px;'
                'vertical-align:middle">重要</span>'
            ) if is_alert_art else ""
            row_bg = 'background:#fff5f5;' if is_alert_art else ''
            rows += (
                f'<tr style="{row_bg}">'
                f'<td style="padding:8px 4px;vertical-align:top">'
                f'<div style="font-weight:600">{alert_badge}{title_esc}</div>'
                f'<div style="font-size:0.82em;color:#6b7280;margin-top:2px">{meta_html}</div>'
                f'<div style="margin-top:4px">'
                f'<a href="{url_esc}" style="color:#2563eb;text-decoration:none">記事を読む →</a>'
                f'</div>'
                f'</td>'
                f'</tr>'
            )
        sections_html += (
            f'<div style="margin-bottom:24px">'
            f'<div style="background:#e3f2fd;color:#1565c0;font-weight:700;'
            f'font-size:0.85em;padding:4px 10px;border-radius:6px;'
            f'display:inline-block;margin-bottom:8px">{kw_esc}</div>'
            f'<table style="width:100%;border-collapse:collapse">{rows}</table>'
            f'</div>'
        )

    alert_banner = ""
    if total_alert > 0:
        alert_banner = (
            '<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:6px;'
            f'padding:8px 14px;margin-bottom:16px;font-size:0.88em;color:#991b1b;">'
            f'⚠️ 重要アラート: {total_alert} 件の重要記事が含まれています'
            '</div>'
        )

    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:600px;margin:0 auto;padding:16px">
<h2 style="font-size:1.1em;margin-bottom:4px">BizRadar ダイジェスト</h2>
<p style="color:#6b7280;font-size:0.85em;margin-top:0">集計日時: {now} ／ 新着記事 {total} 件</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin:12px 0">
{alert_banner}
{sections_html}
<hr style="border:none;border-top:1px solid #e5e7eb;margin-top:24px">
<p style="color:#9ca3af;font-size:0.78em">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""

    msg = MIMEMultipart()
    msg["From"]    = formataddr(("BizRadar", EMAIL_SETTINGS["sender_email"]))
    msg["To"]      = user_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP(EMAIL_SETTINGS["smtp_server"], EMAIL_SETTINGS["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_SETTINGS["sender_email"], EMAIL_SETTINGS["sender_password"])
            server.send_message(msg)
        print(f"[通知] ダイジェストメールを送信しました → {user_email}")
    except smtplib.SMTPException as e:
        print(f"[エラー] ダイジェストメール送信に失敗しました: {e}")


def send_digest_for_user(user_id: int):
    """ユーザーの未通知記事をダイジェストメールで送信する"""
    unnotified = db.load_unnotified_articles(user_id)
    if not unnotified:
        print(f"[ダイジェスト] user_id={user_id} 未通知記事なし")
        return

    # キーワードごとにグループ化（notify_enabled=True のみ）
    articles_by_keyword: dict = {}
    seen_titles: set = set()
    for a in unnotified:
        if not a.get("notify_enabled", True):
            continue
        kw = a.get("keyword", "")
        title = a.get("title", "")
        key = f"{kw}::{title}"
        if key in seen_titles:
            continue
        seen_titles.add(key)
        articles_by_keyword.setdefault(kw, []).append(a)

    # ユーザーのメールアドレスを取得
    user = db.get_user_by_id(user_id)
    if not user:
        return
    user_email = user.get("email", "") or EMAIL_SETTINGS["recipient_email"]

    if articles_by_keyword:
        alert_kws = db.get_alert_keywords_set(user_id)
        send_digest_email(user_email, articles_by_keyword, alert_kws=alert_kws)

    # 送信有無にかかわらず全未通知を通知済みにする（再送防止）
    db.mark_all_unnotified_notified(user_id)
    print(f"[ダイジェスト] user_id={user_id} 通知済みにマーク完了")


def _is_alert(title: str, alert_kws: set) -> bool:
    """記事タイトルにアラートキーワードが含まれるか判定する"""
    t = title.lower()
    return any(kw in t for kw in alert_kws)


def _article_row_html(i: int, a: dict, alert: bool) -> str:
    """メール用記事行 HTML を生成する（重要ラベル付き）"""
    import html as _html
    title_esc = _html.escape(a.get("title", ""))
    url_esc   = _html.escape(a.get("url", ""))
    source    = _html.escape(a.get("source", ""))
    published = _html.escape(a.get("published", ""))
    meta_parts = []
    if source:
        meta_parts.append(f"出典: {source}")
    if published:
        meta_parts.append(f"日時: {published}")
    meta_html = "　".join(meta_parts)
    alert_badge = (
        '<span style="background:#dc2626;color:#fff;font-size:0.75em;'
        'font-weight:700;padding:1px 7px;border-radius:4px;margin-right:6px;'
        'vertical-align:middle">重要</span>'
    ) if alert else ""
    row_bg = 'background:#fff5f5;' if alert else ''
    return (
        f'<tr style="{row_bg}">'
        f'<td style="padding:8px 4px;vertical-align:top;color:#6b7280;font-size:0.85em">{i}</td>'
        f'<td style="padding:8px 4px">'
        f'<div style="font-weight:600">{alert_badge}{title_esc}</div>'
        f'<div style="font-size:0.85em;color:#6b7280;margin-top:2px">{meta_html}</div>'
        f'<div style="margin-top:4px">'
        f'<a href="{url_esc}" style="color:#2563eb;text-decoration:none">記事を読む →</a>'
        f'</div>'
        f'</td>'
        f'</tr>'
    )


def send_news_email(keyword: str, articles: list, user_id: int = None):
    """新着ニュース記事をメールで通知する（HTMLメール）"""
    import html as _html
    # タイトルで重複排除（同一タイトルは最初の1件のみ送信）
    seen_titles: set = set()
    unique_articles = []
    for a in articles:
        t = a.get("title", "")
        if t not in seen_titles:
            seen_titles.add(t)
            unique_articles.append(a)
    articles = unique_articles
    if not articles:
        return

    alert_kws = db.get_alert_keywords_set(user_id) if user_id else set()

    # 重要記事を上、通常記事を下に並び替え
    important = [a for a in articles if _is_alert(a.get("title", ""), alert_kws)]
    normal    = [a for a in articles if not _is_alert(a.get("title", ""), alert_kws)]
    sorted_articles = important + normal
    has_alert = bool(important)

    now = datetime.now(JST).strftime("%Y年%m月%d日 %H:%M")
    alert_prefix = "【重要あり】" if has_alert else ""
    subject = f"{alert_prefix}【ニュース新着通知】「{keyword}」の新着記事 {len(sorted_articles)} 件"

    rows_html = ""
    for i, a in enumerate(sorted_articles[:10], 1):
        rows_html += _article_row_html(i, a, _is_alert(a.get("title", ""), alert_kws))

    alert_banner = ""
    if has_alert:
        alert_banner = (
            '<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:6px;'
            'padding:8px 14px;margin-bottom:16px;font-size:0.88em;color:#991b1b;">'
            f'⚠️ 重要アラート: {len(important)} 件の重要記事が含まれています'
            '</div>'
        )

    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:600px;margin:0 auto;padding:16px">
<h2 style="font-size:1.1em;margin-bottom:4px">
  「{_html.escape(keyword)}」の新着記事 {len(sorted_articles)} 件
</h2>
<p style="color:#6b7280;font-size:0.85em;margin-top:0">検出日時: {now}</p>
{alert_banner}
<table style="width:100%;border-collapse:collapse">
{rows_html}
</table>
<hr style="margin-top:24px;border:none;border-top:1px solid #e5e7eb">
<p style="color:#9ca3af;font-size:0.78em">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""

    msg = MIMEMultipart()
    msg["From"]    = formataddr(("BizRadar", EMAIL_SETTINGS["sender_email"]))
    msg["To"]      = EMAIL_SETTINGS["recipient_email"]
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))
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

    seen_urls   = db.load_article_seen_urls(user_id)
    seen_titles = db.load_article_seen_titles(user_id)
    try:
        articles = fetch_news_articles(keyword)
    except Exception as e:
        print(f"  [エラー] 取得失敗: {e}")
        return

    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    new_articles = []
    for article in articles:
        url        = article["url"]
        title_key  = f"{keyword}::{article.get('title', '')}"
        if url and url not in seen_urls and title_key not in seen_titles:
            article["found_at"] = now_str
            new_articles.append(article)
            seen_urls.add(url)
            seen_titles.add(title_key)

    if new_articles:
        print(f"  → {len(new_articles)} 件の新着記事")
        db.insert_articles(new_articles, user_id)
        notify_ok = db.is_keyword_notify_enabled(user_id, keyword)
        print(f"  [通知チェック] keyword={keyword!r} user_id={user_id} notify_enabled={notify_ok}")
        if notify_ok:
            timing = db.get_user_notify_timing(user_id)
            if timing == "immediate":
                send_news_email(keyword, new_articles, user_id=user_id)
                db.mark_articles_notified_by_urls(user_id, [a["url"] for a in new_articles])
            else:
                print(f"  [ダイジェスト待機] タイミング={timing} のため送信保留")
        else:
            print(f"  [スキップ] 通知OFFのためメール送信をスキップします")
    else:
        print(f"  → 新着なし")


def check_all_keywords():
    """全キーワードのニュースをチェックして新着があれば通知する（ユーザーごとに分離）"""
    kw_with_users = db.load_all_keywords_with_users()
    if not kw_with_users:
        return

    print(f"[ニュースチェック開始] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}")

    # user_id ごとにキーワードをグループ化（notify_enabled 付き）
    user_keywords: dict = {}
    for user_id, keyword, notify_enabled in kw_with_users:
        user_keywords.setdefault(user_id, []).append((keyword, notify_enabled))

    for user_id, keywords in user_keywords.items():
        if user_id is None:
            print(f"  [スキップ] user_id=None のキーワードは処理しません")
            continue
        seen_urls   = db.load_article_seen_urls(user_id)
        seen_titles = db.load_article_seen_titles(user_id)

        for keyword, _notify_enabled_cached in keywords:
            if not keyword:
                continue
            print(f"  キーワード: {keyword} (user_id={user_id})")
            try:
                articles = fetch_news_articles(keyword)
            except Exception as e:
                print(f"  [エラー] 取得失敗: {e}")
                continue

            now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
            new_articles = []
            for article in articles:
                url       = article["url"]
                title_key = f"{keyword}::{article.get('title', '')}"
                if url and url not in seen_urls and title_key not in seen_titles:
                    article["found_at"] = now_str
                    new_articles.append(article)
                    seen_urls.add(url)
                    seen_titles.add(title_key)

            if new_articles:
                print(f"  → {len(new_articles)} 件の新着記事")
                db.insert_articles(new_articles, user_id)
                # 通知設定はDBから直接確認する（キャッシュ値に頼らない）
                notify_ok = db.is_keyword_notify_enabled(user_id, keyword)
                print(f"  [通知チェック] keyword={keyword!r} user_id={user_id} notify_enabled={notify_ok}")
                if notify_ok:
                    timing = db.get_user_notify_timing(user_id)
                    if timing == "immediate":
                        send_news_email(keyword, new_articles, user_id=user_id)
                        db.mark_articles_notified_by_urls(user_id, [a["url"] for a in new_articles])
                    else:
                        print(f"  [ダイジェスト待機] タイミング={timing} のため送信保留")
                else:
                    print(f"  [スキップ] 通知OFFのためメール送信をスキップします")
            else:
                print(f"  → 新着なし")

    print(f"[ニュースチェック完了]")


_NOISE_TAGS = ["script", "style", "nav", "footer", "header", "aside", "iframe", "noscript"]
_NOISE_RE = re.compile(
    r"pager|pagination|pagenav|page.nav|"
    r"ranking|popular|recommend|related|"
    r"\bad\b|ads|advertisement|banner|"
    r"breadcrumb|sitemap|sns|share|"
    r"counter|access.?count|"
    r"menu|gnav|\bnav\b|global.nav|sidebar",
    re.IGNORECASE,
)
_CONTENT_RE = re.compile(r"content|main|news.?list|article.?list|entry", re.IGNORECASE)


def extract_main_content(soup):
    """ノイズ要素を除外して主要コンテンツのテキストを返す"""
    # ① タグ名で除去（role="navigation" も含む）
    for tag in soup(_NOISE_TAGS):
        tag.decompose()
    for tag in soup.find_all(attrs={"role": "navigation"}):
        tag.decompose()

    # ② class/id のノイズパターンで除去
    noise_tags = [
        tag for tag in soup.find_all(True)
        if _NOISE_RE.search(" ".join(tag.get("class", [])))
        or _NOISE_RE.search(tag.get("id") or "")
    ]
    for tag in noise_tags:
        tag.decompose()

    # ③ 主要コンテンツ領域を優先抽出
    main = (
        soup.find("main")
        or soup.find("article")
        or soup.find("section", class_=_CONTENT_RE)
        or soup.find("div", class_=_CONTENT_RE)
        or soup.find(id=_CONTENT_RE)
    )
    target = main or soup.find("body") or soup

    # ④ 短すぎる行（3文字以下）を除去してテキスト化
    return _normalize_lines(target.get_text(separator="\n"))


def _normalize_lines(text: str) -> str:
    """テキストの各行を正規化する（strip + 3文字以下の行を除去）"""
    return "\n".join(ln.strip() for ln in text.splitlines() if len(ln.strip()) > 3)


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
        text = extract_main_content(soup)
        return text, None
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


_DIFF_DATE_RE = re.compile(
    r"^\d{4}[年./\-]\d{1,2}[月./\-]\d{1,2}"  # 2024年4月7日 / 2024-04-07
    r"|^\d{1,2}[月/]\d{1,2}日?$"              # 4月7日
)

# ナビゲーション・カテゴリ文字列判定：以下のいずれも含まない行は除外
# （全角スペース・読点・句点・中黒・半角スペース・半角数字・半角英字）
_DIFF_NAV_RE = re.compile(r"[　、。・ \da-zA-Z]")


def _is_nav(text: str) -> bool:
    """True = ナビゲーション/カテゴリ文字列（有意義な区切り文字を含まない）"""
    return not bool(_DIFF_NAV_RE.search(text))


def compute_diff_summary(old_content: str, new_content: str, _debug_url: str = "") -> list:
    """変更箇所のサマリーを生成する（追加 最大20件・削除 最大5件）
    保存段階で日付行・ナビゲーション行・5文字未満の行を除外し、有意義な行で枠を埋める。
    """
    old_lines = old_content.splitlines()
    new_lines = new_content.splitlines()
    diff = difflib.unified_diff(old_lines, new_lines, lineterm="", n=0)

    # --- デバッグ用カウンター ---
    raw_added = 0
    raw_removed = 0
    skip_short_a = 0
    skip_short_r = 0
    skip_date_a = 0
    skip_date_r = 0
    skip_nav_a = 0
    skip_nav_r = 0
    skip_nav_examples = []
    # ----------------------------

    added = []
    removed = []
    for line in diff:
        if line.startswith("+") and not line.startswith("+++"):
            text = line[1:].strip()
            if not text:
                continue
            raw_added += 1
            if len(text) < 5:
                skip_short_a += 1; continue
            if _DIFF_DATE_RE.match(text):
                skip_date_a += 1; continue
            if _is_nav(text):
                skip_nav_a += 1
                if len(skip_nav_examples) < 5:
                    skip_nav_examples.append(f"+{text}")
                continue
            if len(added) < 20:
                added.append({"type": "added", "text": text})
        elif line.startswith("-") and not line.startswith("---"):
            text = line[1:].strip()
            if not text:
                continue
            raw_removed += 1
            if len(text) < 5:
                skip_short_r += 1; continue
            if _DIFF_DATE_RE.match(text):
                skip_date_r += 1; continue
            if _is_nav(text):
                skip_nav_r += 1
                if len(skip_nav_examples) < 5:
                    skip_nav_examples.append(f"-{text}")
                continue
            if len(removed) < 5:
                removed.append({"type": "removed", "text": text})
        if len(added) >= 20 and len(removed) >= 5:
            break

    # --- デバッグログ出力 ---
    label = f"[diff:{_debug_url}]" if _debug_url else "[diff]"
    print(f"{label} 生diff: added={raw_added}, removed={raw_removed}")
    print(f"{label} 除外(added):  短い={skip_short_a}, 日付={skip_date_a}, nav={skip_nav_a}")
    print(f"{label} 除外(removed): 短い={skip_short_r}, 日付={skip_date_r}, nav={skip_nav_r}")
    print(f"{label} 保存: added={len(added)}, removed={len(removed)}")
    if skip_nav_examples:
        print(f"{label} nav除外サンプル: {skip_nav_examples}")
    # -----------------------

    return added + removed


def send_email(url: str, site_name: str = ""):
    """変更を検知したらメールで通知する"""
    now   = datetime.now(JST).strftime("%Y年%m月%d日 %H:%M")
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
    msg["From"]    = formataddr(("BizRadar", EMAIL_SETTINGS["sender_email"]))
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
    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
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
            old_content  = _normalize_lines(content_store.get(url, ""))
            diff_summary = compute_diff_summary(old_content, content, _debug_url=url) if old_content else []
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
    print(f"\n[チェック開始] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}")
    sites = db.load_sites_for_monitor()
    for site in sites:
        check_single_site(site["url"], site.get("name", ""))
    print(f"[チェック完了]")


def main():
    print("=" * 50)
    print("ウェブサイト監視スクリプト 起動")
    sites = db.load_sites_for_monitor()
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
