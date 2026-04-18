"""
ウェブサイトモニタースクリプト
機能: サイトの内容が変わったらメールで通知する
"""

import base64
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
    """feedparser の entry から記事URLを取り出す（Google News の形式差異に対応）"""
    # まず summary/description フィールドの <a href> から実際のURLを取得する
    for field in ("summary", "description", "content"):
        text = ""
        if field == "content":
            content_list = entry.get("content", [])
            if content_list:
                text = content_list[0].get("value", "")
        else:
            text = entry.get(field, "")
        if text:
            import re
            hrefs = re.findall(r'href=["\']([^"\']+)["\']', text)
            for href in hrefs:
                if "news.google.com" not in href and href.startswith("http"):
                    return href.strip()
    # フォールバック: entry.link から取得
    url = (entry.get("link") or "").strip()
    if url:
        return url
    for link in entry.get("links", []):
        if link.get("rel") in ("alternate", "self"):
            return link.get("href", "").strip()
    return ""


def _resolve_google_news_url(url: str) -> str:
    """Google News RSS URL から実際の記事URLを解決する。

    解決の優先順:
      1. レガシー base64 形式の抽出（~2022 年の古い記事に有効・高速）
      2. googlenewsdecoder ライブラリ（新フォーマット対応）
      3. HTTP リダイレクト追跡（フォールバック）
      4. 全て失敗した場合は news.google.com のURLをそのまま返す
    """
    if "news.google.com" not in url:
        return url

    # 1) レガシー base64 形式の抽出（古い記事向け）
    try:
        import base64, re
        match = re.search(r'articles/([A-Za-z0-9_\-]+)', url)
        if match:
            encoded = match.group(1)
            padding = 4 - len(encoded) % 4
            if padding != 4:
                encoded += '=' * padding
            try:
                decoded = base64.urlsafe_b64decode(encoded)
                candidates = re.findall(rb'https?://[^\x00-\x1f\x7f\s"\'<>]{15,}', decoded)
                for candidate in candidates:
                    candidate_str = candidate.decode('utf-8', errors='ignore')
                    if 'google.com' not in candidate_str:
                        return candidate_str.rstrip('.')
            except Exception:
                pass
    except Exception:
        pass

    # 2) googlenewsdecoder による解決（新フォーマット対応）
    try:
        from googlenewsdecoder import gnewsdecoder
        result = gnewsdecoder(url, interval=1)
        if isinstance(result, dict) and result.get("status"):
            decoded_url = result.get("decoded_url") or ""
            if decoded_url and "news.google.com" not in decoded_url:
                return decoded_url
    except ImportError:
        # ライブラリ未インストール時は静かに次のステップへ
        pass
    except Exception as e:
        print(f"  [警告] googlenewsdecoder 失敗: {e}")

    # 3) HTTP リダイレクト追跡（最後のフォールバック）
    try:
        import requests
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        resp = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
        final_url = resp.url
        if 'news.google.com' not in final_url:
            return final_url
    except Exception:
        pass
    return url


def _try_parse_uncertain_published(published: str) -> str:
    """'~' 始まりの不確実 published 文字列を RFC822/2822 としてパース試行する。
    成功した場合は 'YYYY-MM-DD HH:MM' (JST) に整形し、'~' を除去して返す。
    失敗した場合は元の文字列をそのまま返す（'~' 付きのまま）。

    例:
      '~Mon, 01 Jan 2024 12:00:00 GMT' → '2024-01-01 21:00'
      '~Fri, 12 Apr 2024 10:30:00 +0900' → '2024-04-12 10:30'
      '~2024-04-12T10:30' → '~2024-04-12T10:30' （RFC822 でないのでそのまま）
    """
    if not published or not published.startswith("~"):
        return published
    raw = published.lstrip("~").strip()
    if not raw:
        return published
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if dt is None:
            return published
        # タイムゾーン不明の場合は UTC とみなす（RFC822 は UT/GMT を想定しているため安全側）
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        jst_dt = dt.astimezone(JST)
        return jst_dt.strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError, OverflowError):
        return published


def _fetch_article_published_date(url: str) -> str:
    """記事ページから実際の発行日時を取得する。取得できなければ空文字を返す。"""
    if "news.google.com" in url:
        return ""
    try:
        resp = requests.get(
            url, timeout=5, stream=True,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        )
        resp.encoding = resp.apparent_encoding
        html = resp.text[:50000]  # 先頭50KBのみ読む
        from bs4 import BeautifulSoup
        import json as _json
        soup = BeautifulSoup(html, 'html.parser')

        # 1. JSON-LD の datePublished
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = _json.loads(script.string or '')
                if isinstance(data, list):
                    data = data[0]
                date = data.get('datePublished') or data.get('dateCreated')
                if date:
                    return date
            except Exception:
                pass

        # 2. meta タグ各種
        for attr, name in [
            ('property', 'article:published_time'),
            ('name', 'pubdate'),
            ('name', 'date'),
            ('name', 'DC.date'),
            ('property', 'og:updated_time'),
        ]:
            tag = soup.find('meta', attrs={attr: name})
            if tag and tag.get('content'):
                return tag['content']

        # 3. time タグ
        time_tag = soup.find('time', attrs={'datetime': True})
        if time_tag:
            return time_tag['datetime']

    except Exception:
        pass
    return ""


def _verify_and_repair_published(published_str: str, url: str) -> tuple:
    """RSS の published 文字列を検証し、必要なら元記事から取得して差し替える。
    - 戻り値: (確定済み JST 文字列, date_verified)
    - 未来 / 30日以上前 / パース不能なら _fetch_article_published_date(url) で再取得
    - 取得成功: 新しい JST 文字列 + True
    - 取得失敗 or パース不能: 元の文字列 (空なら現在時刻) + False
    - 妥当な範囲内: 元の文字列 + True
    """
    now = datetime.now(JST)
    # 既存 JST 文字列をパース試行（"~" プレフィックスは除去してから）
    pub = None
    if published_str:
        try:
            pub = datetime.strptime(published_str.lstrip("~").strip(), "%Y-%m-%d %H:%M")
            pub = pub.replace(tzinfo=JST)
        except Exception:
            pub = None

    needs_check = (pub is None) or (pub > now) or (pub < now - timedelta(days=30))

    if needs_check:
        fetched_str = _fetch_article_published_date(url) if url else ""
        if fetched_str:
            fetched_dt = None
            try:
                # ISO8601 (meta/JSON-LD で一般的)
                fetched_dt = datetime.fromisoformat(fetched_str.replace("Z", "+00:00"))
            except Exception:
                try:
                    # RFC822 (feedparser.parse 内では処理されないケース向け)
                    import email.utils as _eu
                    tup = _eu.parsedate_tz(fetched_str)
                    if tup:
                        ts = _eu.mktime_tz(tup)
                        fetched_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                except Exception:
                    fetched_dt = None
            if fetched_dt:
                if fetched_dt.tzinfo:
                    fetched_dt = fetched_dt.astimezone(JST)
                else:
                    fetched_dt = fetched_dt.replace(tzinfo=JST)
                return fetched_dt.strftime("%Y-%m-%d %H:%M"), True
        # 再取得失敗 or パース不能: 元値を維持（空なら now を入れる）
        fallback = published_str if published_str else now.strftime("%Y-%m-%d %H:%M")
        return fallback, False

    return published_str, True


def _send_slack_notification(webhook_url: str, message: str) -> tuple:
    """Slack Incoming Webhook にメッセージを送信する。(ok: bool, error: str) を返す。"""
    if not webhook_url:
        return False, "webhook_url is empty"
    try:
        resp = requests.post(webhook_url, json={"text": message}, timeout=10)
        if 200 <= resp.status_code < 300:
            return True, ""
        err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        print(f"[slack] send failed: {err}")
        return False, err
    except Exception as e:
        print(f"[slack] send failed: {e}")
        return False, str(e)


def _is_notify_day(user_id: int) -> bool:
    """現在の曜日(JST)がユーザーの通知曜日に含まれるか判定する。"""
    try:
        days_str = db.get_user_notify_days(user_id)
    except Exception:
        return True
    allowed = {d.strip() for d in days_str.split(",") if d.strip()}
    if not allowed:
        return True
    today = str(datetime.now(JST).weekday())
    # Python weekday: 0=月 ... 6=日 → DB: 0=日 1=月 ... 6=土
    py_to_db = {"0": "1", "1": "2", "2": "3", "3": "4", "4": "5", "5": "6", "6": "0"}
    return py_to_db.get(today, today) in allowed


def _send_line_notification(line_user_id: str, message: str) -> tuple:
    """LINE Messaging API の Push Message エンドポイントに送信する。(ok, error) を返す。"""
    if not line_user_id:
        return False, "line_user_id is empty"
    token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
    if not token:
        print("[line] send failed: LINE_CHANNEL_ACCESS_TOKEN not set")
        return False, "LINE_CHANNEL_ACCESS_TOKEN not set"
    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "to": line_user_id,
                "messages": [{"type": "text", "text": message}],
            },
            timeout=10,
        )
        if 200 <= resp.status_code < 300:
            return True, ""
        err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        print(f"[line] send failed: {err}")
        return False, err
    except Exception as e:
        print(f"[line] send failed: {e}")
        return False, str(e)




def _score_article_importance(title: str, plan: str) -> dict:
    """記事タイトルの重要度と主役企業名を返す。
    戻り値: {"importance": "high"/"medium"/"low", "primary_company": "企業名" or None}
    business/pro プラン以外は AI 未呼び出し。
    """
    result = {"importance": "low", "primary_company": None}
    if plan not in ("business", "pro"):
        return result
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        print(f"[importance] low: {title[:30]} (no API key)")
        return result
    try:
        import anthropic
    except ImportError:
        print(f"[importance] low: {title[:30]} (anthropic not installed)")
        return result
    prompt = (
        "次のニュースタイトルを重要度で分類してください。\n\n"
        "high（重要）: 決算・業績発表、人事（就任・退任・解任）、M&A・経営統合・買収、倒産・民事再生、リコール・重大事故、工場閉鎖・大規模リストラ\n"
        "medium（注目）: 業務提携・合弁、新規事業参入、新製品・新サービス発表、受注・契約締結\n"
        "low（通常）: セミナー・展示会・発表大会への参加・出展、プレスリリース・お知らせ、サイト更新、定例報告、イベント開催告知、表彰・受賞、インタビュー・コラム・解説記事\n\n"
        "注意：PR TIMESやプレスリリース配信サービス由来と思われる記事はlowを優先してください。\n"
        "注意：タイトルに「〜大会」「〜フェスタ」「〜セミナー」「〜展」が含まれる場合はlowにしてください。\n\n"
        f"タイトル: {title}\n\n"
        "以下のJSON形式で回答してください。他の文字は不要です。\n"
        '{"importance": "high または medium または low", "primary_company": "主役企業名（日本語）。特定企業の話でなければnull"}'
    )
    try:
        import json as _json
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        text = ""
        for block in msg.content:
            if getattr(block, "type", None) == "text":
                text += block.text or ""
        text = text.strip()
        try:
            parsed = _json.loads(text)
            imp = parsed.get("importance", "low")
            if imp in ("high", "medium", "low"):
                result["importance"] = imp
            pc = parsed.get("primary_company")
            if pc and pc != "null" and isinstance(pc, str):
                result["primary_company"] = pc.strip()
        except _json.JSONDecodeError:
            text_lower = text.lower()
            for level in ("high", "medium", "low"):
                if level in text_lower:
                    result["importance"] = level
                    break
        print(f"[importance] {result['importance']}: {title[:30]} company={result['primary_company']!r}")
    except Exception as e:
        print(f"[importance] API error title={title[:30]!r}: {e}")
    return result


def _resolve_primary_company_id(company_name: str | None, user_id: int) -> int | None:
    """AI が返した企業名をユーザーの登録企業と照合し company_id を返す。"""
    if not company_name:
        return None
    try:
        companies = db.load_companies(user_id)
    except Exception:
        return None
    cn = company_name.strip()
    for c in companies:
        name = c.get("name", "")
        if cn in name or name in cn:
            return c["id"]
    return None


def _summarize_article(title: str, url: str, plan: str) -> str:
    """記事本文を取得し Claude Haiku で 3 行以内に要約する。
    business/pro プラン以外は空文字を返す（AI 未呼び出し）。
    本文取得失敗・API 失敗時も空文字にフォールバックする。最大 200 文字で truncate。
    """
    if plan not in ("business", "pro"):
        return ""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""
    try:
        import anthropic
    except ImportError:
        return ""

    # 本文取得（失敗しても続行してタイトルのみで要約する）
    content = ""
    try:
        resp = requests.get(
            url, timeout=10, verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            },
        )
        resp.encoding = resp.apparent_encoding
        soup = BeautifulSoup(resp.text, "html.parser")
        content = extract_main_content(soup, url)
    except Exception as e:
        print(f"[summary] fetch failed {url}: {e}")

    if len(content) < 100:
        prompt = (
            "次のニュース記事を3行以内で要約してください。日本語で簡潔に。\n"
            f"タイトル: {title}\n"
            "要約のみ回答してください。"
        )
    else:
        prompt = (
            "次のニュース記事を3行以内で要約してください。日本語で簡潔に。\n"
            f"タイトル: {title}\n"
            f"本文: {content[:1000]}\n"
            "要約のみ回答してください。"
        )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = ""
        for block in msg.content:
            if getattr(block, "type", None) == "text":
                text += block.text or ""
        summary = text.strip()
        if len(summary) > 200:
            summary = summary[:200]
        print(f"[summary] {title[:30]}")
        return summary
    except Exception as e:
        print(f"[summary] API error {title[:30]!r}: {e}")
        return ""


def fetch_youtube_videos(channel_id: str, keyword: str) -> list:
    """YouTube RSS フィードから新着動画を取得する（最新10件）"""
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BizRadar/1.0)",
    }
    print(f"  [YouTube] 取得開始: channel_id={channel_id}")
    try:
        response = requests.get(rss_url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  [YouTube] 取得失敗: channel_id={channel_id} error={e}")
        return []

    feed = feedparser.parse(response.content)
    if not feed.entries:
        print(f"  [YouTube] 動画なし: channel_id={channel_id}")
        return []
    print(f"  [YouTube] 取得完了: channel_id={channel_id} 件数={len(feed.entries)}")

    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    articles = []
    for entry in feed.entries[:10]:
        title = _sanitize_text(entry.get("title", ""))
        url = _sanitize_text(entry.get("link", ""))
        published = ""
        if entry.get("published_parsed"):
            dt = datetime.fromtimestamp(
                time_module.mktime(entry.published_parsed), tz=timezone.utc
            ).astimezone(JST)
            published = dt.strftime("%Y-%m-%d %H:%M")
        if not title or not url:
            continue
        articles.append({
            "keyword":       keyword,
            "title":         title,
            "url":           url,
            "source":        "YouTube",
            "published":     published or now_str[:16],
            "found_at":      now_str,
            "date_verified": True,
            "importance":    "low",
            "summary":       "",
        })
    return articles


def _is_old_unverified(published: str, date_verified: bool) -> bool:
    """date_verified=False かつ published が30日以上前の JST 日付なら True を返す。
    パース不能な日付はスキップ対象外（False）として扱う。
    """
    if date_verified:
        return False
    raw = (published or "").lstrip("~").strip()
    if not raw:
        return False
    try:
        pub = datetime.strptime(raw, "%Y-%m-%d %H:%M").replace(tzinfo=JST)
    except Exception:
        return False
    return pub < datetime.now(JST) - timedelta(days=30)


def fetch_news_articles(keyword: str, user_plan: str = "basic") -> list:
    """Google News RSSからキーワード関連記事を取得する（最新20件）

    urllib 経由の feedparser 直取得はサーバー環境でブロックされやすいため、
    requests で本文を取得してからパースする。
    """
    rss_url = f"https://news.google.com/rss/search?q={quote(keyword)}&hl=ja&gl=JP&ceid=JP:ja"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
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
        url = _sanitize_text(_resolve_google_news_url(_rss_entry_link(entry)))
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
            published = "~" + entry.get("published", "")
        published = _sanitize_text(published)
        # RSSから発行日時が取れない場合、記事ページから補完
        if not published.lstrip("~") and url and "news.google.com" not in url:
            try:
                fetched = _fetch_article_published_date(url)
                if fetched:
                    published = _sanitize_text("~" + fetched[:16])
            except Exception:
                pass
        # '~' 付き published を RFC822 としてパース試行（成功時のみ '~' を除去）
        published = _try_parse_uncertain_published(published)
        # 未来 / 30日以上前 / パース不能 のときは元記事から日付を再取得
        published, date_verified = _verify_and_repair_published(published, url)
        # 再取得に失敗した30日以上前の記事は保存しない（古い記事の紛れ込みを防ぐ）
        if _is_old_unverified(published, date_verified):
            print(f"[fetch] skip old unverified: {url}")
            continue
        articles.append({
            "keyword":       keyword,
            "title":         title,
            "url":           url,
            "source":        source,
            "published":     published,
            "date_verified": date_verified,
            "importance":    "low",
            "summary":       "",
            "_primary_company": None,
        })
    return articles


def fetch_bing_news_articles(keyword: str, user_plan: str = "basic") -> list:
    """Bing News RSSからキーワード関連記事を取得する（最新20件）

    Yahoo!ニュースのキーワードRSS（2020年8月廃止）の代替ソースとして使用する。
    失敗しても例外を上位に伝播せず空リストを返す（Google News に影響を与えない）。
    BingリダイレクトURLからクエリパラメータで実際の記事URLを抽出する。
    """
    from urllib.parse import urlparse, parse_qs as _parse_qs
    rss_url = f"https://www.bing.com/news/search?q={quote(keyword)}&format=rss&mkt=ja-JP"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    }
    print(f"  [Bing News] 取得開始: keyword={keyword!r}")
    try:
        response = requests.get(rss_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  [Bing News] 取得失敗（スキップ）: keyword={keyword!r} error={e}")
        db.update_source_health("bing_news", False, str(e))
        return []

    feed = feedparser.parse(response.content)
    if feed.bozo and getattr(feed, "bozo_exception", None):
        print(f"  [Bing News][警告] RSSの解析に問題があります: {feed.bozo_exception}")
    if not feed.entries:
        print(f"  [Bing News][警告] RSSの記事が0件です (HTTP {response.status_code}, keyword={keyword!r})")
    else:
        print(f"  [Bing News] 取得完了: keyword={keyword!r} HTTP={response.status_code} 件数={len(feed.entries)}")

    def _extract_actual_url(bing_url: str) -> str:
        """Bingリダイレクトリンクから実際の記事URLを抽出する"""
        try:
            qs = _parse_qs(urlparse(bing_url).query)
            actual = qs.get("url", [""])[0]
            return actual if actual else bing_url
        except Exception:
            return bing_url

    articles = []
    for entry in feed.entries[:20]:
        title = _sanitize_text(entry.get("title", ""))
        raw_url = _sanitize_text(_rss_entry_link(entry))
        url = _extract_actual_url(raw_url) if raw_url else raw_url
        source = "Bing News"
        published = ""
        if entry.get("published_parsed"):
            dt = datetime.fromtimestamp(time_module.mktime(entry.published_parsed), tz=timezone.utc).astimezone(JST)
            published = dt.strftime("%Y-%m-%d %H:%M")
        else:
            published = "~" + entry.get("published", "")
        published = _sanitize_text(published)
        # 発行日時が取得できていない場合、記事ページから取得を試みる
        if not published.lstrip("~") and url and "news.google.com" not in url:
            try:
                fetched_date = _fetch_article_published_date(url)
                if fetched_date:
                    published = "~" + fetched_date[:16]
            except Exception:
                pass
        # '~' 付き published を RFC822 としてパース試行（成功時のみ '~' を除去）
        published = _try_parse_uncertain_published(published)
        if title and url:
            published, date_verified = _verify_and_repair_published(published, url)
            # 再取得に失敗した30日以上前の記事は保存しない（古い記事の紛れ込みを防ぐ）
            if _is_old_unverified(published, date_verified):
                print(f"[fetch] skip old unverified: {url}")
                continue
            articles.append({
                "keyword":       keyword,
                "title":         title,
                "url":           url,
                "source":        source,
                "published":     published,
                "date_verified": date_verified,
                "importance":    "low",
                "summary":       "",
                "_primary_company": None,
            })
    db.update_source_health("bing_news", True)
    return articles


def fetch_prtimes_articles(keyword: str, user_plan: str = "basic") -> list:
    """PR TIMES 全件フィードからキーワードに一致するプレスリリースを返す。

    旧検索エンドポイント (rss/search.rss) は廃止されたため、
    全件フィード (index.rdf) を取得してタイトル・本文でキーワードフィルタリングする。
    取得に失敗しても例外を上位に伝播せず空リストを返す。
    """
    rss_url = "https://prtimes.jp/index.rdf"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    }
    print(f"  [PR TIMES] 取得開始: keyword={keyword!r}")
    try:
        response = requests.get(rss_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  [PR TIMES] 取得失敗（スキップ）: keyword={keyword!r} error={e}")
        db.update_source_health("prtimes", False, str(e))
        return []

    feed = feedparser.parse(response.content)
    if feed.bozo and getattr(feed, "bozo_exception", None):
        print(f"  [PR TIMES][警告] RSSの解析に問題があります: {feed.bozo_exception}")
    if not feed.entries:
        print(f"  [PR TIMES][警告] RSSの記事が0件です (HTTP {response.status_code}, keyword={keyword!r})")
    else:
        print(f"  [PR TIMES] 取得完了: keyword={keyword!r} HTTP={response.status_code} 全件数={len(feed.entries)}")

    kw_lower = keyword.lower()
    articles = []
    for entry in feed.entries:
        title   = _sanitize_text(entry.get("title", ""))
        summary = entry.get("summary", "") or ""
        # タイトルまたは本文にキーワードが含まれるもののみ対象
        if kw_lower not in title.lower() and kw_lower not in summary.lower():
            continue
        url = _sanitize_text(_rss_entry_link(entry))
        published = ""
        if entry.get("published_parsed"):
            dt = datetime.fromtimestamp(time_module.mktime(entry.published_parsed), tz=timezone.utc).astimezone(JST)
            published = dt.strftime("%Y-%m-%d %H:%M")
        else:
            published = "~" + entry.get("published", "")
        published = _sanitize_text(published)
        # 発行日時が取得できていない場合、記事ページから取得を試みる
        if not published.lstrip("~") and url and "news.google.com" not in url:
            try:
                fetched_date = _fetch_article_published_date(url)
                if fetched_date:
                    published = "~" + fetched_date[:16]
            except Exception:
                pass
        # '~' 付き published を RFC822 としてパース試行（成功時のみ '~' を除去）
        published = _try_parse_uncertain_published(published)
        if title and url:
            published, date_verified = _verify_and_repair_published(published, url)
            articles.append({
                "keyword":       keyword,
                "title":         title,
                "url":           url,
                "source":        "PR TIMES",
                "published":     published,
                "date_verified": date_verified,
                "importance":    "low",
                "_primary_company": None,
            })
        if len(articles) >= 20:
            break
    print(f"  [PR TIMES] キーワードフィルタ後: keyword={keyword!r} 件数={len(articles)}")
    db.update_source_health("prtimes", True)
    return articles


# 後方互換エイリアス（旧名称で呼んでいる箇所がある場合に備える）
fetch_yahoo_news_articles = fetch_bing_news_articles

_SOURCE_NAMES = {
    "google_news": "Google News",
    "bing_news":   "Bing News",
    "prtimes":     "PR TIMES",
}
_CONSECUTIVE_FAIL_THRESHOLD = 3

# 類似度計算時に除去する企業接頭辞・各種括弧・空白（半角/全角）
_TITLE_CLEAN_RE = re.compile(r"株式会社|（株）|\(株\)|有限会社|「|」|【|】|『|』|\s")
_GROUP_SIMILARITY_THRESHOLD = 0.75


def _calc_title_similarity(title1: str, title2: str) -> float:
    """2つのタイトルの類似度を 0.0〜1.0 で返す。
    比較前に企業接頭辞（株式会社/（株）/有限会社）、各種括弧（「」【】『』）、空白を除去する。
    """
    t1 = _TITLE_CLEAN_RE.sub("", title1 or "")
    t2 = _TITLE_CLEAN_RE.sub("", title2 or "")
    if not t1 or not t2:
        return 0.0
    return difflib.SequenceMatcher(None, t1, t2).ratio()


def _group_duplicate_articles(user_id: int):
    """直近7日間の未グループ記事をタイトル類似度でグルーピングする。
    類似度 >= 0.75 を同一ニュースとみなし、最古の記事を代表として group_id にまとめる。
    """
    try:
        rows = db.load_articles_for_grouping(user_id, days=7)
    except Exception as e:
        print(f"[group] user_id={user_id} load失敗: {e}")
        return
    if not rows:
        print(f"[group] user_id={user_id} grouped=0")
        return

    # 既にグループ化済みの代表を起点に、古い順で未処理記事を突き合わせる
    reps = [
        {"id": r["id"], "title": r["title"] or ""}
        for r in rows
        if r.get("group_id") is not None and r.get("is_representative")
    ]
    grouped = 0
    for r in rows:
        if r.get("group_id") is not None:
            continue
        matched = None
        for rep in reps:
            if _calc_title_similarity(r["title"] or "", rep["title"]) >= _GROUP_SIMILARITY_THRESHOLD:
                matched = rep
                break
        if matched:
            try:
                db.add_duplicate_to_group(r["id"], matched["id"])
                grouped += 1
            except Exception as e:
                print(f"[group] user_id={user_id} dup更新失敗 id={r['id']}: {e}")
        else:
            try:
                db.set_article_as_representative(r["id"])
                reps.append({"id": r["id"], "title": r["title"] or ""})
            except Exception as e:
                print(f"[group] user_id={user_id} rep更新失敗 id={r['id']}: {e}")
    print(f"[group] user_id={user_id} grouped={grouped}")


def send_system_error_email(errors: list):
    """システムエラーを管理者に通知するメールを送信する"""
    import html as _html
    now = datetime.now(JST).strftime("%Y年%m月%d日 %H:%M")
    subject = "【BizRadar】システムエラーが発生しました"
    errors_html = "".join(f"<li>{_html.escape(e)}</li>" for e in errors)
    salutation = db.get_salutation_for_email(EMAIL_SETTINGS["recipient_email"])
    body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:600px;margin:0 auto;padding:16px">
<p>{salutation}</p>
<h2 style="font-size:1.1em;color:#dc2626">⚠ システムエラーが発生しました</h2>
<p style="color:#6b7280;font-size:0.85em">発生日時: {now}</p>
<ul style="line-height:1.8">
{errors_html}
</ul>
<hr style="margin-top:24px;border:none;border-top:1px solid #e5e7eb">
<p style="color:#9ca3af;font-size:0.78em">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""

    msg = MIMEMultipart()
    msg["From"]    = formataddr(("BizRadar", EMAIL_SETTINGS["sender_email"]))
    msg["To"]      = EMAIL_SETTINGS["recipient_email"]
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html", "utf-8"))
    try:
        with smtplib.SMTP(EMAIL_SETTINGS["smtp_server"], EMAIL_SETTINGS["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_SETTINGS["sender_email"], EMAIL_SETTINGS["sender_password"])
            server.send_message(msg)
        print(f"[システムエラー通知] メールを送信しました: {errors}")
    except smtplib.SMTPException as e:
        print(f"[エラー] システムエラーメール送信失敗: {e}")


def check_and_notify_source_errors():
    """ソース連続失敗を確認し、未通知のエラーを管理者にメール通知する（24時間に1回まで）"""
    try:
        health = db.get_source_health()
    except Exception as e:
        print(f"[check_and_notify_source_errors] DB取得失敗: {e}")
        return

    errors_to_notify = []
    for source, data in health.items():
        if data.get("consecutive_failures", 0) < _CONSECUTIVE_FAIL_THRESHOLD:
            continue
        notified_at = data.get("error_notified_at")
        if notified_at is not None:
            elapsed = (datetime.now(timezone.utc) - notified_at).total_seconds()
            if elapsed < 86400:
                continue
        name = _SOURCE_NAMES.get(source, source)
        fails = data.get("consecutive_failures", 0)
        errors_to_notify.append((source, f"{name}の収集が停止しています（{fails}回連続失敗）"))

    if errors_to_notify:
        messages = [msg for _, msg in errors_to_notify]
        send_system_error_email(messages)
        for source, _ in errors_to_notify:
            try:
                db.set_source_error_notified(source)
            except Exception as e:
                print(f"[check_and_notify_source_errors] 通知済み更新失敗: {e}")


def send_digest_email(user_email: str, articles_by_keyword: dict, alert_kws: set = None, user_name: str = ""):
    """ダイジェストメールを送信する。
    articles_by_keyword: {keyword: [article, ...], ...}
    alert_kws: アラートキーワードの小文字セット（フォールバック用）
    各記事に呼び出し元が is_alert を事前付与している場合はそれを優先して使用する
    （per-company 判定を外側で済ませておくため）。
    """
    import html as _html
    if alert_kws is None:
        alert_kws = set()

    def _art_alert(a):
        if "is_alert" in a:
            return bool(a["is_alert"])
        return _is_alert(a.get("title", ""), alert_kws)

    total = sum(len(v) for v in articles_by_keyword.values())
    if total == 0:
        return
    now = datetime.now(JST).strftime("%Y年%m月%d日 %H:%M")

    # 重要記事の総数を集計（件名用）
    total_alert = sum(
        1 for arts in articles_by_keyword.values()
        for a in arts if _art_alert(a)
    )
    alert_prefix = "【重要あり】" if total_alert > 0 else ""
    subject = f"{alert_prefix}【BizRadar ダイジェスト】本日の新着記事 {total} 件"

    # 各キーワードブロック内でも重要記事を上に並び替え
    sections_html = ""
    for keyword, arts in articles_by_keyword.items():
        if not arts:
            continue
        kw_esc = _html.escape(keyword)
        important = [a for a in arts if _art_alert(a)]
        normal    = [a for a in arts if not _art_alert(a)]
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
            is_alert_art = _art_alert(a)
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

    salutation = db.get_salutation_for_email(user_email)
    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:600px;margin:0 auto;padding:16px">
<p style="margin-bottom:12px">{salutation}</p>
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
    if not _is_notify_day(user_id):
        print(f"[ダイジェスト] user_id={user_id} 今日は通知対象外の曜日です")
        return
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
        user_name = user.get("name", "") or ""

        # per-company アラート判定を各記事に事前付与する
        # （send_digest_email 側は記事の "is_alert" を優先して参照する）
        kw_rows = db.load_keywords_with_company(user_id)
        kw_to_company = {
            k["keyword"]: k["company_id"]
            for k in kw_rows if k.get("company_id")
        }
        per_cid_alert: dict = {}
        for e in db.get_all_company_alert_keywords_for_user(user_id):
            per_cid_alert.setdefault(e["company_id"], set()).add(e["keyword"].lower())
        for _kw, _arts in articles_by_keyword.items():
            cid = kw_to_company.get(_kw)
            effective = alert_kws | per_cid_alert.get(cid, set())
            for _a in _arts:
                _a["is_alert"] = _is_alert(_a.get("title", ""), effective)

        send_digest_email(user_email, articles_by_keyword, alert_kws=alert_kws, user_name=user_name)

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
    # ユーザー情報を取得して宛名・送信先を決定
    user_email = ""
    user_name = ""
    if user_id:
        user = db.get_user_by_id(user_id)
        if user:
            user_email = user.get("email", "")
            user_name = user.get("name", "") or ""
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
    # 該当キーワードが紐づく企業の重要アラートキーワードも併用（per-company 判定）
    if user_id:
        cid = db.get_user_keyword_company_id(user_id, keyword)
        if cid:
            alert_kws = alert_kws | {
                e["keyword"].lower()
                for e in db.get_company_alert_keywords(cid)
            }

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

    salutation = db.get_salutation_for_email(user_email or EMAIL_SETTINGS["recipient_email"])
    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:600px;margin:0 auto;padding:16px">
<p style="margin-bottom:12px">{salutation}</p>
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

    recipient = user_email or EMAIL_SETTINGS["recipient_email"]
    msg = MIMEMultipart()
    msg["From"]    = formataddr(("BizRadar", EMAIL_SETTINGS["sender_email"]))
    msg["To"]      = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP(EMAIL_SETTINGS["smtp_server"], EMAIL_SETTINGS["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_SETTINGS["sender_email"], EMAIL_SETTINGS["sender_password"])
            server.send_message(msg)
        print(f"[通知] ニュースメールを送信しました → {recipient}")
    except smtplib.SMTPException as e:
        print(f"[エラー] ニュースメール送信に失敗しました: {e}")


def check_single_keyword(keyword: str, user_id=None):
    """単一キーワードのニュースをチェックしてDBを更新する"""
    print(f"[ニュースチェック] キーワード: {keyword} (user_id={user_id})")
    if user_id is None:
        print("  [エラー] user_id が必要です")
        return

    db.add_running_task("keyword_check", keyword)
    seen_urls   = db.load_article_seen_urls(user_id)
    seen_titles = db.load_article_seen_titles(user_id)
    user_plan = (db.get_user_by_id(user_id) or {}).get("plan", "basic")
    try:
        google_articles = fetch_news_articles(keyword, user_plan)
        db.update_source_health("google_news", True)
    except Exception as e:
        print(f"  [エラー] Google News 取得失敗（Bing/PR TIMESで続行）: {e}")
        db.update_source_health("google_news", False, str(e))
        google_articles = []
    yahoo_articles = fetch_bing_news_articles(keyword, user_plan)
    prtimes_articles = fetch_prtimes_articles(keyword, user_plan)
    articles = google_articles + yahoo_articles + prtimes_articles

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
        for _a in new_articles:
            score = _score_article_importance(_a.get("title", ""), user_plan)
            _a["importance"] = score["importance"]
            _a["primary_company_id"] = _resolve_primary_company_id(
                score["primary_company"] or _a.pop("_primary_company", None), user_id)
        db.insert_articles(new_articles, user_id)
        try:
            _group_duplicate_articles(user_id)
        except Exception as e:
            print(f"  [警告] グルーピング失敗 user_id={user_id}: {e}")
        notify_ok = db.is_keyword_notify_enabled(user_id, keyword)
        print(f"  [通知チェック] keyword={keyword!r} user_id={user_id} notify_enabled={notify_ok}")
        if notify_ok:
            kw_cid = db.get_user_keyword_company_id(user_id, keyword)
            if kw_cid and not db.is_company_notify_enabled(user_id, kw_cid):
                print(f"  [スキップ] 企業通知OFFのためスキップ")
            elif not _is_notify_day(user_id):
                print(f"  [スキップ] 今日は通知対象外の曜日です")
            elif kw_cid and db.is_company_instant(user_id, kw_cid):
                send_news_email(keyword, new_articles, user_id=user_id)
                db.mark_articles_notified_by_urls(user_id, [a["url"] for a in new_articles])
            else:
                print(f"  [ダイジェスト待機] 送信保留")
        else:
            print(f"  [スキップ] 通知OFFのためメール送信をスキップします")
    else:
        print(f"  → 新着なし")
    db.remove_running_task("keyword_check", keyword)


def check_all_keywords():
    """全キーワードのニュースをチェックして新着があれば通知する（ユーザーごとに分離）"""
    kw_with_users = db.load_all_keywords_with_users()
    if not kw_with_users:
        return

    print(f"[ニュースチェック開始] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}")

    # user_id ごとにキーワードをグループ化（notify_enabled 付き）
    user_keywords: dict = {}
    for user_id, keyword, notify_enabled, keyword_id, company_id in kw_with_users:
        user_keywords.setdefault(user_id, []).append(
            (keyword, notify_enabled, keyword_id, company_id)
        )

    for user_id, keywords in user_keywords.items():
        if user_id is None:
            print(f"  [スキップ] user_id=None のキーワードは処理しません")
            continue
        seen_urls   = db.load_article_seen_urls(user_id)
        seen_titles = db.load_article_seen_titles(user_id)
        exclude_kws = {e["keyword"].lower() for e in db.get_exclude_keywords(user_id)}
        user_plan = (db.get_user_by_id(user_id) or {}).get("plan", "basic")

        for keyword, _notify_enabled_cached, keyword_id, company_id in keywords:
            if not keyword:
                continue
            print(f"  キーワード: {keyword} (user_id={user_id})")
            db.add_running_task("keyword_check", keyword)
            # 企業単位の除外ワード（ユーザー全体の exclude_kws と併用）。
            # 紐づけなし（company_id=None）のキーワードには適用しない。
            company_exclude_words = (
                {e["exclude_word"].lower() for e in db.get_company_exclude_words(company_id)}
                if company_id else set()
            )
            try:
                google_articles = fetch_news_articles(keyword, user_plan)
                db.update_source_health("google_news", True)
            except Exception as e:
                import traceback
                print(f"  [エラー] Google News 取得失敗（Bing/PR TIMESで続行） user_id={user_id} keyword={keyword!r}: {e}")
                print(f"  [トレース] {traceback.format_exc()}")
                db.update_source_health("google_news", False, str(e))
                google_articles = []

            try:
                yahoo_articles = fetch_bing_news_articles(keyword, user_plan)
            except Exception as e:
                import traceback
                print(f"  [エラー] Yahoo/Bing News 取得失敗 user_id={user_id} keyword={keyword!r}: {e}")
                print(f"  [トレース] {traceback.format_exc()}")
                yahoo_articles = []

            try:
                prtimes_articles = fetch_prtimes_articles(keyword, user_plan)
            except Exception as e:
                import traceback
                print(f"  [エラー] PR TIMES 取得失敗 user_id={user_id} keyword={keyword!r}: {e}")
                print(f"  [トレース] {traceback.format_exc()}")
                prtimes_articles = []

            articles = google_articles + yahoo_articles + prtimes_articles

            now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
            new_articles = []
            for article in articles:
                url       = article["url"]
                title     = article.get("title", "")
                title_key = f"{keyword}::{title}"
                # 除外キーワードが含まれる記事はスキップ
                # ユーザー全体の除外（exclude_kws） + 企業単位の除外（company_exclude_words）
                tl = title.lower()
                if exclude_kws and any(ex in tl for ex in exclude_kws):
                    continue
                if company_exclude_words and any(ex in tl for ex in company_exclude_words):
                    continue
                if url and url not in seen_urls and title_key not in seen_titles:
                    article["found_at"] = now_str
                    new_articles.append(article)
                    seen_urls.add(url)
                    seen_titles.add(title_key)

            if new_articles:
                print(f"  → {len(new_articles)} 件の新着記事")
                for _a in new_articles:
                    score = _score_article_importance(_a.get("title", ""), user_plan)
                    _a["importance"] = score["importance"]
                    _a["primary_company_id"] = _resolve_primary_company_id(
                        score["primary_company"] or _a.pop("_primary_company", None), user_id)
                try:
                    db.insert_articles(new_articles, user_id)
                except Exception as e:
                    import traceback
                    print(f"  [エラー] DB保存失敗 user_id={user_id} keyword={keyword!r}: {e}")
                    print(f"  [トレース] {traceback.format_exc()}")
                    db.remove_running_task("keyword_check", keyword)
                    continue
                # 企業通知OFF なら送信をスキップ
                if company_id and not db.is_company_notify_enabled(user_id, company_id):
                    print(f"  [スキップ] 企業通知OFFのためスキップ company_id={company_id}")
                elif not db.is_keyword_notify_enabled(user_id, keyword):
                    print(f"  [スキップ] 通知OFFのためメール送信をスキップします")
                elif not _is_notify_day(user_id):
                    print(f"  [スキップ] 今日は通知対象外の曜日です")
                elif company_id and db.is_company_instant(user_id, company_id):
                    try:
                        send_news_email(keyword, new_articles, user_id=user_id)
                        db.mark_articles_notified_by_urls(user_id, [a["url"] for a in new_articles])
                    except Exception as e:
                        import traceback
                        print(f"  [エラー] メール送信失敗 user_id={user_id} keyword={keyword!r}: {e}")
                        print(f"  [トレース] {traceback.format_exc()}")
                else:
                    print(f"  [ダイジェスト待機] 送信保留")
            else:
                print(f"  → 新着なし")
            db.remove_running_task("keyword_check", keyword)

        # ユーザー単位で 1 回、直近7日分の記事を重複グルーピング
        try:
            _group_duplicate_articles(user_id)
        except Exception as e:
            print(f"  [警告] グルーピング失敗 user_id={user_id}: {e}")

        # YouTube RSS 収集（company_youtube_channels テーブルから）
        try:
            yt_rows = db.load_all_youtube_channels_for_user(user_id)
            # company_id → 最初のキーワードをキャッシュ
            _yt_kw_cache: dict = {}
            for row in yt_rows:
                cid = row["company_id"]
                ch_id = row["channel_id"]
                if cid not in _yt_kw_cache:
                    kw_list = db.load_company_keywords(user_id, cid)
                    _yt_kw_cache[cid] = kw_list[0]["keyword"] if kw_list else None
                keyword = _yt_kw_cache[cid]
                if not keyword:
                    continue
                yt_articles = fetch_youtube_videos(ch_id, keyword)
                new_yt = [a for a in yt_articles if a["url"] not in seen_urls]
                for a in new_yt:
                    seen_urls.add(a["url"])
                if new_yt:
                    print(f"  [YouTube] {row['company_name']}({ch_id}): {len(new_yt)} 件の新着動画")
                    db.insert_articles(new_yt, user_id)
        except Exception as e:
            print(f"  [YouTube] 収集エラー user_id={user_id}: {e}")

    print(f"[ニュースチェック完了]")
    check_and_notify_source_errors()


def check_keywords_for_user(user_id: int) -> dict:
    """指定ユーザーのキーワードのみ収集・通知処理する（管理者デバッグ用）。
    check_all_keywords() と同一ロジックだが単一ユーザーに限定。
    """
    result = {"keywords": 0, "new_articles": 0, "notifications": 0}
    kw_with_users = db.load_all_keywords_with_users()
    keywords = [
        (kw, ne, kid, cid) for uid, kw, ne, kid, cid in kw_with_users
        if uid == user_id
    ]
    if not keywords:
        return result

    print(f"[ユーザーチェック開始] user_id={user_id} keywords={len(keywords)}")
    seen_urls   = db.load_article_seen_urls(user_id)
    seen_titles = db.load_article_seen_titles(user_id)
    exclude_kws = {e["keyword"].lower() for e in db.get_exclude_keywords(user_id)}
    user_plan = (db.get_user_by_id(user_id) or {}).get("plan", "basic")

    for keyword, _ne, keyword_id, company_id in keywords:
        if not keyword:
            continue
        result["keywords"] += 1
        print(f"  キーワード: {keyword} (user_id={user_id})")
        db.add_running_task("keyword_check", keyword)
        company_exclude_words = (
            {e["exclude_word"].lower() for e in db.get_company_exclude_words(company_id)}
            if company_id else set()
        )
        try:
            google_articles = fetch_news_articles(keyword, user_plan)
            db.update_source_health("google_news", True)
        except Exception as e:
            print(f"  [エラー] Google News 取得失敗（続行）: {e}")
            db.update_source_health("google_news", False, str(e))
            google_articles = []
        try:
            yahoo_articles = fetch_bing_news_articles(keyword, user_plan)
        except Exception:
            yahoo_articles = []
        try:
            prtimes_articles = fetch_prtimes_articles(keyword, user_plan)
        except Exception:
            prtimes_articles = []

        articles = google_articles + yahoo_articles + prtimes_articles
        now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
        new_articles = []
        for article in articles:
            url = article["url"]
            title = article.get("title", "")
            title_key = f"{keyword}::{title}"
            tl = title.lower()
            if exclude_kws and any(ex in tl for ex in exclude_kws):
                continue
            if company_exclude_words and any(ex in tl for ex in company_exclude_words):
                continue
            if url and url not in seen_urls and title_key not in seen_titles:
                article["found_at"] = now_str
                new_articles.append(article)
                seen_urls.add(url)
                seen_titles.add(title_key)

        if new_articles:
            print(f"  -> {len(new_articles)} 件の新着記事")
            result["new_articles"] += len(new_articles)
            for _a in new_articles:
                score = _score_article_importance(_a.get("title", ""), user_plan)
                _a["importance"] = score["importance"]
                _a["primary_company_id"] = _resolve_primary_company_id(
                    score["primary_company"] or _a.pop("_primary_company", None), user_id)
            try:
                db.insert_articles(new_articles, user_id)
            except Exception as e:
                print(f"  [エラー] DB保存失敗: {e}")
                db.remove_running_task("keyword_check", keyword)
                continue
            if company_id and not db.is_company_notify_enabled(user_id, company_id):
                pass
            elif not db.is_keyword_notify_enabled(user_id, keyword):
                pass
            elif not _is_notify_day(user_id):
                pass
            elif company_id and db.is_company_instant(user_id, company_id):
                try:
                    send_news_email(keyword, new_articles, user_id=user_id)
                    db.mark_articles_notified_by_urls(user_id, [a["url"] for a in new_articles])
                    result["notifications"] += 1
                except Exception as e:
                    print(f"  [エラー] メール送信失敗: {e}")
        db.remove_running_task("keyword_check", keyword)

    try:
        _group_duplicate_articles(user_id)
    except Exception:
        pass
    try:
        yt_rows = db.load_all_youtube_channels_for_user(user_id)
        _yt_kw_cache: dict = {}
        for row in yt_rows:
            cid = row["company_id"]
            ch_id = row["channel_id"]
            if cid not in _yt_kw_cache:
                kw_list = db.load_company_keywords(user_id, cid)
                _yt_kw_cache[cid] = kw_list[0]["keyword"] if kw_list else None
            kw = _yt_kw_cache[cid]
            if not kw:
                continue
            yt_articles = fetch_youtube_videos(ch_id, kw)
            new_yt = [a for a in yt_articles if a["url"] not in seen_urls]
            for a in new_yt:
                seen_urls.add(a["url"])
            if new_yt:
                result["new_articles"] += len(new_yt)
                db.insert_articles(new_yt, user_id)
    except Exception as e:
        print(f"  [YouTube] 収集エラー: {e}")

    print(f"[ユーザーチェック完了] user_id={user_id} {result}")
    return result


    # 保持期間を超えた古い記事を削除（found_at が30日より前）
    try:
        deleted = db.delete_old_articles(days=90)
        if deleted > 0:
            print(f"[保持期間] 90日以上前の記事を {deleted} 件削除しました")
    except Exception as e:
        import traceback
        print(f"[エラー] 古い記事の削除に失敗: {e}")
        print(traceback.format_exc())


_NOISE_TAGS = ["script", "style", "nav", "footer", "header", "aside", "iframe", "noscript"]
_NOISE_RE = re.compile(
    r"pager|pagination|pagenav|page.nav|"
    r"ranking|popular|recommend|related|"
    r"\bad\b|ads|advertisement|banner|"
    r"tracking|analytics|cookie|"
    r"breadcrumb|pankuzu|sitemap|sns|share|"
    r"counter|access.?count|"
    r"menu|gnav|\bnav\b|global.nav|local.nav|sidebar|"
    r"\bcategory\b|tag.?list|article.?category|cat.?label|"
    r"area.?nav|region.?nav|topic.?nav|"
    r"pagetop|page.top|back.?to.?top|totop",
    re.IGNORECASE,
)
_CONTENT_RE = re.compile(r"content|main|news.?list|article.?list|entry", re.IGNORECASE)


def extract_main_content(soup, url: str = ""):
    """ノイズ要素を除外して主要コンテンツのテキストを返す。
    結果が 50 文字未満の場合は空文字を返す（呼び出し元でタイトルのみ表示する想定）。
    """
    # ① script/style/noscript を含むタグ名で除去、および role 属性によるナビ・ヘッダー・フッター系除去
    for tag in soup(_NOISE_TAGS):
        tag.decompose()
    for role in ("navigation", "banner", "contentinfo", "complementary"):
        for tag in soup.find_all(attrs={"role": role}):
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

    # ④ プレーンテキスト化 → 残存HTMLタグを除去 → 行正規化
    raw_text = target.get_text(separator="\n")
    raw_text = re.sub(r"<[^>]+>", "", raw_text)  # 稀に残るタグを保険として除去
    text = _normalize_lines(raw_text)

    # ⑤ 連続する空行を1つに圧縮し、先頭・末尾の空白を除去
    text = re.sub(r"\n{2,}", "\n", text).strip()

    # ⑥ 抽出結果が極端に短い場合は空文字を返す
    if len(text) < 50:
        print(f"[extract] too short: {url}")
        return ""

    return text


def _normalize_lines(text: str) -> str:
    """テキストの各行を正規化する（タブ除去・連続空白圧縮・strip + 3文字以下の行を除去）"""
    lines = []
    for ln in text.splitlines():
        ln = ln.replace("\t", " ")          # タブ→スペース
        ln = " ".join(ln.split())           # 連続空白を1つに圧縮（strip込み）
        if len(ln) > 3:
            lines.append(ln)
    return "\n".join(lines)


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
        text = extract_main_content(soup, url)
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


_PAGE_PATTERNS = [
    "{base}/page/{n}/",
    "{base}/page/{n}",
    "{base}?page={n}",
    "{base}?p={n}",
    "{base}?paged={n}",
]


def _fetch_additional_pages(base_url: str, max_pages: int, base_content: str) -> str:
    """2ページ目以降のコンテンツを取得して結合する。"""
    extra = ""
    base_stripped = base_url.rstrip("/")
    for n in range(2, max_pages + 1):
        found = False
        for pattern in _PAGE_PATTERNS:
            page_url = pattern.format(base=base_stripped, n=n)
            try:
                content, err = get_page_content(page_url)
                if content and content != base_content and len(content) > 50:
                    extra += "\n" + content
                    print(f"  [ページ取得] {page_url} → OK")
                    found = True
                    break
            except Exception:
                continue
        if not found:
            print(f"  [ページ取得] {n}ページ目: 有効なURLが見つかりません")
            break
    return extra


def compute_hash(content: str) -> str:
    return hashlib.md5(content.encode("utf-8")).hexdigest()


_DIFF_DATE_RE = re.compile(
    r"^\d{4}[年./\-]\d{1,2}[月./\-]\d{1,2}"  # 2024年4月7日 / 2024-04-07
    r"|^\d{1,2}[月/]\d{1,2}日?$"              # 4月7日
)

# 行内の日付・時刻を正規化して差分比較からノイズを除外するための正規表現。
# 例: 「2024年3月15日」「2025/4/1」「2025-04-01」「10:30」「23:59:59」「最終更新日 2025-04-01」
_DATE_NORMALIZE_RE = re.compile(
    r"\d{4}\s*[年./\-]\s*\d{1,2}\s*[月./\-]\s*\d{1,2}\s*日?"  # 年月日
    r"|\d{1,2}\s*[月/]\s*\d{1,2}\s*日?"                        # 月日のみ
    r"|\d{1,2}:\d{2}(?::\d{2})?"                                # 時刻 HH:MM[:SS]
)
# 残りの数字（カンマ区切り含む）を 0 に潰して数値だけの変化を無視する
_NUMBER_NORMALIZE_RE = re.compile(r"[\d,]*\d")


def _normalize_for_diff(text: str) -> str:
    """差分比較用に日付・時刻を <DATE> に、その他の数字を 0 に正規化する。
    行表示には使わない（diff の等価判定専用）。
    """
    text = _DATE_NORMALIZE_RE.sub("<DATE>", text)
    text = _NUMBER_NORMALIZE_RE.sub("0", text)
    return text

# ナビゲーション・カテゴリ文字列判定：以下のいずれも含まない行は除外
# （全角スペース・読点・句点・中黒・半角スペース・半角数字・半角英字・ひらがな・カタカナ）
# ひらがな/カタカナを含む行は文章的な見出しとして通す
_DIFF_NAV_RE = re.compile(r"[　、。・ \da-zA-Zぁ-んァ-ン]")

# 区切り文字によるナビメニュー判定
# 「/」「／」「・」「|」「｜」「　（全角スペース）」で区切られた短い語の羅列をナビとみなす
_NAV_SEPARATOR_RE = re.compile(r"[/／・|｜　]")


def _is_nav_separator_list(text: str) -> bool:
    """区切り文字で2語以上に分割でき、各パーツが短い語のみの場合 True を返す"""
    parts = [p.strip() for p in _NAV_SEPARATOR_RE.split(text) if p.strip()]
    if len(parts) < 2:
        return False
    # 各パーツが平均10文字以下ならナビ列とみなす（長いメニューも拾えるよう全体長は制限しない）
    avg_len = sum(len(p) for p in parts) / len(parts)
    # 2語の場合は各パーツが8文字以下の場合のみナビとみなす
    if len(parts) == 2 and avg_len > 8:
        return False
    return avg_len <= 10


def _is_nav(text: str) -> bool:
    """True = ナビゲーション/カテゴリ文字列"""
    # 既存判定: 有意義な文字種を含まない漢字だけの行
    if not bool(_DIFF_NAV_RE.search(text)):
        return True
    # 追加判定: 区切り文字で分割された短い語の羅列
    if _is_nav_separator_list(text):
        return True
    return False


def compute_diff_summary(old_content: str, new_content: str, _debug_url: str = "") -> list:
    """変更箇所のサマリーを生成する（追加 最大20件・削除 最大5件）
    保存段階で日付行・ナビゲーション行・5文字未満の行を除外し、有意義な行で枠を埋める。
    """
    old_lines = old_content.splitlines()
    new_lines = new_content.splitlines()
    diff = difflib.unified_diff(old_lines, new_lines, lineterm="", n=0)

    # 日付/数字のみ差異の行を抑制するため、反対側の正規化行セットを作成
    normalized_old_set = {_normalize_for_diff(ln.strip()) for ln in old_lines if ln.strip()}
    normalized_new_set = {_normalize_for_diff(ln.strip()) for ln in new_lines if ln.strip()}

    # --- デバッグ用カウンター ---
    raw_added = 0
    raw_removed = 0
    skip_short_a = 0
    skip_short_r = 0
    skip_date_a = 0
    skip_date_r = 0
    skip_nav_a = 0
    skip_nav_r = 0
    skip_norm_a = 0
    skip_norm_r = 0
    skip_nav_examples = []
    # ----------------------------

    added = []
    removed = []
    seen_added = set()
    seen_removed = set()
    for line in diff:
        if line.startswith("+") and not line.startswith("+++"):
            text = line[1:].strip()
            if not text:
                continue
            raw_added += 1
            if len(text) < 5:
                skip_short_a += 1; continue
            tokens = text.replace('\u3000', ' ').split()
            if len(tokens) >= 2 and all(len(t) <= 8 for t in tokens):
                skip_short_a += 1; continue
            if _DIFF_DATE_RE.match(text):
                skip_date_a += 1; continue
            if _is_nav(text):
                skip_nav_a += 1
                if len(skip_nav_examples) < 5:
                    skip_nav_examples.append(f"+{text}")
                continue
            if _normalize_for_diff(text) in normalized_old_set:
                skip_norm_a += 1; continue
            if text in seen_added:
                continue
            seen_added.add(text)
            if len(added) < 20:
                added.append({"type": "added", "text": text})
        elif line.startswith("-") and not line.startswith("---"):
            text = line[1:].strip()
            if not text:
                continue
            raw_removed += 1
            if len(text) < 5:
                skip_short_r += 1; continue
            tokens = text.replace('\u3000', ' ').split()
            if len(tokens) >= 2 and all(len(t) <= 8 for t in tokens):
                skip_short_r += 1; continue
            if _DIFF_DATE_RE.match(text):
                skip_date_r += 1; continue
            if _is_nav(text):
                skip_nav_r += 1
                if len(skip_nav_examples) < 5:
                    skip_nav_examples.append(f"-{text}")
                continue
            if _normalize_for_diff(text) in normalized_new_set:
                skip_norm_r += 1; continue
            if text in seen_removed:
                continue
            seen_removed.add(text)
            if len(removed) < 5:
                removed.append({"type": "removed", "text": text})
        if len(added) >= 20 and len(removed) >= 5:
            break

    # --- デバッグログ出力 ---
    label = f"[diff:{_debug_url}]" if _debug_url else "[diff]"
    print(f"{label} 生diff: added={raw_added}, removed={raw_removed}")
    print(f"{label} 除外(added):  短い={skip_short_a}, 日付={skip_date_a}, nav={skip_nav_a}, 正規化一致={skip_norm_a}")
    print(f"{label} 除外(removed): 短い={skip_short_r}, 日付={skip_date_r}, nav={skip_nav_r}, 正規化一致={skip_norm_r}")
    print(f"{label} 保存: added={len(added)}, removed={len(removed)}")
    if skip_nav_examples:
        print(f"{label} nav除外サンプル: {skip_nav_examples}")
    # -----------------------

    return added + removed


def send_site_change_email(user_email: str, changed_sites: list):
    """変更を検知したサイトをまとめてユーザーに通知する。
    changed_sites: [{"url": "...", "name": "..."}, ...]
    """
    import html as _html
    if not changed_sites or not user_email:
        return
    now = datetime.now(JST).strftime("%Y年%m月%d日 %H:%M")
    salutation = db.get_salutation_for_email(user_email)
    rows_html = ""
    for s in changed_sites:
        label = _html.escape(s.get("name") or s["url"])
        url_esc = _html.escape(s["url"])
        rows_html += (
            f'<div style="padding:10px 0;border-bottom:1px solid #f0f2f5;">'
            f'<div style="font-weight:600;color:#1a1a2e;">{label}</div>'
            f'<div style="margin-top:4px;">'
            f'<a href="{url_esc}" style="color:#3949ab;text-decoration:none;">{url_esc}</a></div>'
            f'</div>'
        )
    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:600px;margin:0 auto;padding:16px">
<p>{salutation}</p>
<h2 style="font-size:1.1em;margin-bottom:4px;">モニタリングサイトの更新を検知しました</h2>
<p style="color:#6b7280;font-size:0.85em;margin-top:0;">検出日時: {now}</p>
{rows_html}
<hr style="border:none;border-top:1px solid #e5e7eb;margin-top:24px">
<p style="color:#9ca3af;font-size:0.78em;">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""

    msg = MIMEMultipart()
    msg["From"]    = formataddr(("BizRadar", EMAIL_SETTINGS["sender_email"]))
    msg["To"]      = user_email
    msg["Subject"] = "【BizRadar】モニタリングサイトの更新を検知しました"
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP(EMAIL_SETTINGS["smtp_server"], EMAIL_SETTINGS["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_SETTINGS["sender_email"], EMAIL_SETTINGS["sender_password"])
            server.send_message(msg)
        print(f"[通知] サイト変更メールを送信しました → {user_email} ({len(changed_sites)}件)")
    except smtplib.SMTPException as e:
        print(f"[エラー] サイト変更メール送信に失敗しました: {e}")


def check_single_site(url: str, site_name: str = "", max_pages: int = 1) -> bool:
    """単一URLをチェックしてDBを更新する。変更があれば True を返す。"""
    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  確認中: {url}")
    changed = False

    previous_hashes = db.load_hashes()
    log             = db.load_monitor_log()
    content_store   = db.load_content_store()

    content, error = get_page_content(url)
    if content and max_pages > 1:
        extra = _fetch_additional_pages(url, max_pages, content)
        if extra:
            content = content + extra

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
            log["last_checks"][url] = {"timestamp": now_str, "status": "changed"}
            log["change_history"].insert(0, {
                "timestamp": now_str,
                "url":       url,
                "name":      site_name,
                "diff":      diff_summary,
            })
            changed = True
        else:
            print(f"  → 変更なし")
            log["last_checks"][url] = {"timestamp": now_str, "status": "ok"}

        previous_hashes[url] = new_hash
        content_store[url]   = (content or "").replace("\x00", "")[:30000]

    db.save_hashes(previous_hashes)
    db.save_monitor_log(log)
    db.save_content_store(content_store)
    return changed


def check_and_notify_site_errors():
    """モニターサイトのエラーを確認し、未通知の場合に管理者へメール通知する（24時間に1回まで）"""
    try:
        error_count = db.count_error_sites()
    except Exception as e:
        print(f"[check_and_notify_site_errors] DB取得失敗: {e}")
        return
    if error_count == 0:
        return

    try:
        health = db.get_source_health()
    except Exception:
        health = {}

    site_health = health.get("site_errors", {})
    notified_at = site_health.get("error_notified_at")
    if notified_at is not None:
        elapsed = (datetime.now(timezone.utc) - notified_at).total_seconds()
        if elapsed < 86400:
            return

    send_system_error_email([f"モニターサイトの取得エラーが {error_count} 件あります"])
    try:
        db.set_source_error_notified("site_errors")
    except Exception as e:
        print(f"[check_and_notify_site_errors] 通知済み更新失敗: {e}")


def check_all_sites():
    """全URLをチェックして変更があれば通知する（ユーザー別にまとめ送信）"""
    print(f"\n[チェック開始] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}")
    sites = db.load_sites_for_monitor()

    # user_id ごとに変更サイトを集約
    user_changes: dict = {}  # {user_id: [{"url":..., "name":...}, ...]}
    for site in sites:
        if not site.get("enabled", True):
            continue
        if not site.get("company_notify_enabled", True):
            continue
        changed = check_single_site(site["url"], site.get("name", ""), max_pages=site.get("max_pages", 1))
        if changed:
            uid = site.get("user_id")
            if uid:
                user_changes.setdefault(uid, []).append({
                    "url": site["url"],
                    "name": site.get("name", ""),
                })

    # ユーザーごとにまとめてメール送信
    for uid, changed_sites in user_changes.items():
        try:
            user = db.get_user_by_id(uid)
            if not user:
                continue
            email = user.get("email", "")
            if email:
                send_site_change_email(email, changed_sites)
        except Exception as e:
            print(f"[エラー] サイト変更通知失敗 user_id={uid}: {e}")

    print(f"[チェック完了]")
    check_and_notify_site_errors()


def main():
    print("=" * 50)
    print("ウェブサイトモニタースクリプト 起動")
    sites = db.load_sites_for_monitor()
    print(f"モニター対象: {[s['url'] for s in sites]}")
    config   = db.load_config()
    interval = config.get("check_interval_seconds", DEFAULT_CHECK_INTERVAL)
    print(f"チェック間隔: {interval // 60} 分ごと")
    print("=" * 50)

    while True:
        check_all_sites()
        # キーワードチェックは Render Cron Job (scripts/run_keyword_check.py) に移管
        # check_all_keywords()
        config   = db.load_config()
        interval = config.get("check_interval_seconds", DEFAULT_CHECK_INTERVAL)
        print(f"[待機中] 次回チェックまで {interval // 60} 分待機します...")
        time.sleep(interval)


def check_listed_company_urls():
    """listed_companies の website_url 死活確認（週1回のバックグラウンドジョブ想定）。
    - ステータスコード 200-299: url_status='ok'
    - リダイレクトが発生（resp.history 非空）: 最終到達 URL を website_url に上書き
    - それ以外・例外: url_status='error'
    - 1件ごとに time.sleep(1) でレートリミット
    - 終了後 err_count > 0 なら管理者へメール通知
    """
    rows = db.load_listed_companies_with_url()
    total = len(rows)
    print(f"[url_check] start total={total}")
    ok_count = 0
    err_count = 0
    updated_url_count = 0
    error_rows = []
    for i, row in enumerate(rows, 1):
        code = row.get("securities_code")
        name = row.get("company_name") or ""
        url = (row.get("website_url") or "").strip()
        if not url:
            continue
        final_url = None
        status = "error"
        try:
            resp = requests.get(
                url, timeout=10, allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; BizRadar-url-check/1.0)"},
            )
            if 200 <= resp.status_code < 300:
                status = "ok"
                # リダイレクトで URL が変わっていれば最終到達 URL に差し替え
                if resp.history and resp.url and resp.url.rstrip("/") != url.rstrip("/"):
                    final_url = resp.url
                    updated_url_count += 1
            else:
                print(
                    f"[url_check] HTTP {resp.status_code} code={code} "
                    f"name={name!r} url={url}"
                )
        except Exception as e:
            print(f"[url_check] error code={code} name={name!r} url={url} err={e}")
        try:
            db.update_listed_company_url_check(code, status, final_url)
            if status == "ok":
                ok_count += 1
            else:
                err_count += 1
                error_rows.append({
                    "securities_code": code,
                    "company_name": name,
                    "website_url": url,
                })
        except Exception as e:
            print(f"[url_check] DB update failed code={code} err={e}")
        if i % 50 == 0:
            print(f"[url_check] progress {i}/{total} ok={ok_count} err={err_count}")
        time.sleep(1)
    print(
        f"[url_check] done total={total} ok={ok_count} err={err_count} "
        f"url_updated={updated_url_count}"
    )

    # AI による URL 自動修正: エラー企業の公式サイトを Claude に推定させ、検証後に更新
    if err_count > 0 and error_rows:
        fixed_codes = _ai_fix_error_urls(error_rows)
        if fixed_codes:
            error_rows = [r for r in error_rows if r.get("securities_code") not in fixed_codes]
            ok_count += len(fixed_codes)
            err_count -= len(fixed_codes)

    # error があれば管理者メール通知（DB から最新 summary を取る）
    if err_count > 0:
        try:
            summary = db.get_url_check_summary()
        except Exception:
            summary = {"ok": ok_count, "error": err_count, "unchecked": 0}
        try:
            _send_url_check_error_email(err_count, summary, error_rows[:20])
        except Exception as e:
            print(f"[url_check] email send failed: {e}")


def _ai_suggest_official_url(company_name: str) -> str:
    """Claude Haiku 4.5 に公式サイトURLを推定させる。取得できなければ空文字を返す。"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""
    try:
        import anthropic
    except ImportError:
        print("[url_check][ai_fix] anthropic library not installed")
        return ""
    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": f"{company_name}の公式サイトURLを1つだけ答えてください。URLのみ回答し、説明は不要です。",
            }],
        )
        text = ""
        for block in msg.content:
            if getattr(block, "type", None) == "text":
                text += block.text or ""
        m = re.search(r"https?://\S+", text)
        if m:
            # 末尾の句読点や閉じ括弧を除去
            return m.group(0).rstrip(".,;)'\"<>")
    except Exception as e:
        print(f"[url_check][ai_fix] Anthropic API error name={company_name!r}: {e}")
    return ""


def _ai_fix_error_urls(error_rows: list) -> set:
    """error_rows 各件に対し Claude で URL 推定→検証→更新。修正成功した securities_code の set を返す。"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        print("[url_check][ai_fix] ANTHROPIC_API_KEY 未設定のためスキップ")
        return set()
    print(f"[url_check][ai_fix] start attempts={len(error_rows)}")
    fixed = set()
    for row in error_rows:
        code = row.get("securities_code")
        name = row.get("company_name", "")
        if not code or not name:
            time.sleep(2)
            continue
        ai_url = _ai_suggest_official_url(name)
        if not ai_url:
            print(f"[url_check][ai_fix] no URL from AI code={code} name={name!r}")
            time.sleep(2)
            continue
        try:
            resp = requests.get(
                ai_url, timeout=10, allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; BizRadar-url-check/1.0)"},
            )
            if 200 <= resp.status_code < 300:
                final = resp.url if (resp.history and resp.url) else ai_url
                try:
                    db.update_listed_company_url_check(code, "ok", final)
                    fixed.add(code)
                    print(f"[url_check][ai_fix] fixed code={code} name={name!r} url={final}")
                except Exception as e:
                    print(f"[url_check][ai_fix] DB update failed code={code} err={e}")
            else:
                print(
                    f"[url_check][ai_fix] HTTP {resp.status_code} code={code} "
                    f"name={name!r} url={ai_url}"
                )
        except Exception as e:
            print(f"[url_check][ai_fix] request failed code={code} url={ai_url} err={e}")
        time.sleep(2)
    print(
        f"[url_check][ai_fix] done fixed={len(fixed)} / "
        f"attempted={len(error_rows)}"
    )
    return fixed


def _send_url_check_error_email(err_count: int, summary: dict, error_rows: list):
    """URL 死活チェック結果で error があれば管理者に通知する"""
    import html as _html
    now = datetime.now(JST).strftime("%Y年%m月%d日 %H:%M")
    subject = f"[BizRadar] URLエラー検出: {err_count}件"
    ok = summary.get("ok", 0)
    err = summary.get("error", err_count)
    unchecked = summary.get("unchecked", 0)
    rows_html = "".join(
        f"<tr>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee'>{_html.escape(r.get('company_name',''))}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #eee;word-break:break-all'>"
        f"<a href='{_html.escape(r.get('website_url',''))}' style='color:#3949ab'>"
        f"{_html.escape(r.get('website_url',''))}</a></td>"
        f"</tr>"
        for r in error_rows
    )
    more_note = (
        f"<p style='color:#9ca3af;font-size:0.82em'>※ 全 {err} 件のうち 20 件まで表示。"
        f"残りは管理画面でご確認ください。</p>"
        if err > len(error_rows) else ""
    )
    salutation = db.get_salutation_for_email(EMAIL_SETTINGS["recipient_email"])
    body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:640px;margin:0 auto;padding:16px">
<p>{salutation}</p>
<h2 style="font-size:1.1em;color:#dc2626">⚠ URLエラー検出: {err_count}件</h2>
<p style="color:#6b7280;font-size:0.85em">チェック日時: {now}</p>
<p style="font-size:0.95em">ok: {ok} 件 / error: {err} 件 / unchecked: {unchecked} 件</p>
<table style="width:100%;border-collapse:collapse;font-size:0.88em;margin-top:12px">
  <thead>
    <tr style="background:#f9fafb;color:#6b7280;text-align:left">
      <th style="padding:6px 10px;border-bottom:1px solid #e5e7eb">企業名</th>
      <th style="padding:6px 10px;border-bottom:1px solid #e5e7eb">website_url</th>
    </tr>
  </thead>
  <tbody>
{rows_html}
  </tbody>
</table>
{more_note}
<hr style="margin-top:24px;border:none;border-top:1px solid #e5e7eb">
<p style="color:#9ca3af;font-size:0.78em">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""
    msg = MIMEMultipart()
    msg["From"]    = formataddr(("BizRadar", EMAIL_SETTINGS["sender_email"]))
    msg["To"]      = EMAIL_SETTINGS["recipient_email"]
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html", "utf-8"))
    try:
        with smtplib.SMTP(EMAIL_SETTINGS["smtp_server"], EMAIL_SETTINGS["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_SETTINGS["sender_email"], EMAIL_SETTINGS["sender_password"])
            server.send_message(msg)
        print(f"[url_check] 管理者へエラー通知メール送信: {err_count} 件")
    except smtplib.SMTPException as e:
        print(f"[url_check] メール送信失敗: {e}")


if __name__ == "__main__":
    main()
