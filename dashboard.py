"""
モニターダッシュボード
monitor.py のモニターデータをブラウザで確認できるWebアプリ（マルチユーザー対応）
"""

import difflib
import logging
from urllib.parse import urlparse as _urlparse
import os
import re
import sys
import unicodedata
import threading
import time
from datetime import datetime, timezone, timedelta
from functools import wraps

JST = timezone(timedelta(hours=9))


def _now_jst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(JST).strftime(fmt)


def _utc_to_jst(ts: str) -> str:
    """UTC文字列（'YYYY-MM-DD HH:MM:SS' または 'YYYY-MM-DD HH:MM'）をJSTに変換する"""
    if not ts or ts == "未チェック":
        return ts
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc).astimezone(JST)
            return dt.strftime(fmt)
        except ValueError:
            continue
    return ts


def _classify_site_error(error: str) -> tuple:
    """site.error テキストからエラーラベルと CSS クラスを返す。
    Returns (label, css_class)
    """
    if not error:
        return ("取得エラー", "badge-error")
    e = error.lower()
    if "timeout" in e or "timed out" in e or "time out" in e:
        return ("タイムアウト", "badge-error-warn")
    if "ssl" in e or "certificate" in e:
        return ("SSL エラー", "badge-error")
    if "403" in e or "forbidden" in e or ("access" in e and "denied" in e):
        return ("アクセス拒否", "badge-error")
    if "404" in e or "not found" in e:
        return ("ページ不明", "badge-error")
    if "connectionerror" in e or "connection" in e or "network" in e or "failed to connect" in e:
        return ("取得失敗", "badge-error")
    return ("不明エラー", "badge-error")
from flask import Flask, flash, render_template, jsonify, request, redirect, url_for, session, send_from_directory
from dotenv import load_dotenv

import db

load_dotenv()

logger = logging.getLogger(__name__)
_gunicorn_error = logging.getLogger("gunicorn.error")
if _gunicorn_error.handlers:
    logger.handlers = _gunicorn_error.handlers
    logger.setLevel(_gunicorn_error.level)
else:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _extract_domain(url: str) -> str:
    """URL からドメイン（netloc）を返す。"""
    try:
        return _urlparse(url).netloc.lower()
    except Exception:
        return ""


def _normalize_title_for_dedup(title: str) -> str:
    """重複判定専用のタイトル正規化（DB保存や表示には一切影響しない）。

    手順:
      1) NFKC 正規化で全角英数字・記号を半角に統一
         （｜→|, （）→(), ＝→=, 全角英数字→半角英数字 など）
      2) 末尾・冒頭にある媒体名マーカーを除去（最大3回ループで入れ子にも対応）
         - 冒頭/末尾: 【媒体名】
         - 末尾のみ: (媒体名) / |媒体名 / =媒体名 / "- 媒体名"（スペース必須）
      3) 前後空白の除去
      4) 小文字化

    安全策: 正規化後の文字列が2文字以下になる場合は、元タイトルを
    小文字化して返す（誤って無関係な記事を束ねないため）。
    """
    if not title:
        return ""
    # 1) NFKC 正規化（全角→半角などを統一）
    s = unicodedata.normalize("NFKC", title)
    # 2) 媒体名マーカーを最大3回まで繰り返し除去
    for _ in range(3):
        prev = s
        # 冒頭: 【媒体名】
        s = re.sub(r'^\s*【[^】]{1,30}】\s*', '', s)
        # 末尾: 【媒体名】
        s = re.sub(r'\s*【[^】]{1,30}】\s*$', '', s)
        # 末尾: (媒体名)
        s = re.sub(r'\s*\([^)]{1,30}\)\s*$', '', s)
        # 末尾: |媒体名 / | 媒体名
        s = re.sub(r'\s*\|\s*[^|]+$', '', s)
        # 末尾: =媒体名
        s = re.sub(r'\s*=\s*[^=]+$', '', s)
        # 末尾: "- 媒体名"（スペース必須、日付の "2024-12-15" と区別）
        s = re.sub(r'\s+-\s+[^-]+$', '', s)
        if s == prev:
            break
    # 3) 前後空白除去 4) 小文字化
    s = s.strip().lower()
    # 安全策: 2文字以下になった場合は元タイトルを使う
    if len(s) <= 2:
        return title.strip().lower()
    return s


def _flag_articles_alert(user_id, articles, kw_company_map=None):
    """記事リストに is_alert / alert_matches を設定する（per-company 判定）。
    各記事の keyword から company_id を解決し、ユーザー全体のアラートキーワードと
    その企業の company_alert_keywords を合算して記事タイトルにマッチさせる。
    未紐づけキーワードの記事はユーザー全体のアラートのみで判定する。
    """
    user_entries = db.load_alert_keywords(user_id)
    user_map = {e["keyword"].lower(): e["keyword"] for e in user_entries}

    # company_id → 結合済み {lower: original}
    company_maps: dict = {}
    for e in db.get_all_company_alert_keywords_for_user(user_id):
        cid = e["company_id"]
        m = company_maps.get(cid)
        if m is None:
            m = dict(user_map)
            company_maps[cid] = m
        m[e["keyword"].lower()] = e["keyword"]

    if kw_company_map is None:
        kw_rows = db.load_keywords_with_company(user_id)
        kw_company_map = {
            k["keyword"]: k["company_id"]
            for k in kw_rows if k.get("company_id")
        }

    for a in articles:
        cid = kw_company_map.get(a.get("keyword", ""))
        m = company_maps.get(cid, user_map)
        title_lower = a.get("title", "").lower()
        matched = [m[k] for k in m if k in title_lower]
        a["is_alert"] = bool(matched)
        a["alert_matches"] = matched


def _deduplicate_articles(articles, threshold=0.80):
    """同一キーワード内でタイトル類似度が threshold 以上の記事を重複排除する。
    Yahoo!ニュースを最優先で残し、次点で古い記事を残す。
    比較には媒体名を除去した正規化タイトルを使う（_normalize_title_for_dedup）。"""
    from collections import defaultdict

    def _sim(a, b):
        return difflib.SequenceMatcher(None, a, b).ratio()

    def _sort_key(art):
        # Yahoo含むソースを最優先（0）、それ以外（1）
        is_yahoo = 0 if "yahoo" in (art.get("source", "") or "").lower() else 1
        published = art.get("published", "") or ""
        return (is_yahoo, published)

    by_kw = defaultdict(list)
    for idx, art in enumerate(articles):
        by_kw[art.get("keyword", "")].append(idx)

    keep = set()
    for kw, indices in by_kw.items():
        group = sorted([(idx, articles[idx]) for idx in indices], key=lambda x: _sort_key(x[1]))
        # グループ内で正規化タイトルを事前計算（N² 比較中の再計算を回避）
        normalized = {
            idx: _normalize_title_for_dedup(art.get("title", "") or "")
            for idx, art in group
        }
        removed = set()
        for i, (idx_a, art_a) in enumerate(group):
            if idx_a in removed:
                continue
            keep.add(idx_a)
            title_a = normalized[idx_a]
            for idx_b, art_b in group[i + 1:]:
                if idx_b in removed or idx_b in keep:
                    continue
                title_b = normalized[idx_b]
                if _sim(title_a, title_b) >= threshold:
                    removed.add(idx_b)

    return [art for idx, art in enumerate(articles) if idx in keep]

app = Flask(__name__)

# SECRET_KEY は必須。未設定なら起動失敗させる（デフォルト値フォールバック廃止）
_secret = os.environ.get("SECRET_KEY", "").strip()
if not _secret:
    raise RuntimeError(
        "SECRET_KEY 環境変数が設定されていません。"
        "Render のダッシュボード → Service → Environment から SECRET_KEY を設定してください。"
    )
app.secret_key = _secret

# セッションCookie のセキュリティ属性
# - SECURE: Render (HTTPS) では True、ローカル HTTP では False に自動切替
# - HTTPONLY: JS からのアクセスを禁止
# - SAMESITE: Lax でCSRF耐性を向上（通常のリンク遷移は許可、クロスサイトPOSTはブロック）
_is_prod = bool(os.environ.get("RENDER"))
app.config.update(
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
    SESSION_COOKIE_SECURE=_is_prod,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

# ---- リバースプロキシ配下の実IP取得（Render LB 1段を信頼）----
if _is_prod:
    from werkzeug.middleware.proxy_fix import ProxyFix  # noqa: E402
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

# ---- CSRF 保護 ----
# 全 POST/PUT/DELETE/PATCH エンドポイントに自動でCSRFトークン検証を適用
from flask_wtf.csrf import CSRFProtect, CSRFError, generate_csrf  # noqa: E402
csrf = CSRFProtect(app)


@app.context_processor
def inject_csrf_token():
    """テンプレート内で {{ csrf_token() }} として利用できるようにする"""
    return {"csrf_token": generate_csrf}


@app.context_processor
def inject_is_pro():
    """全テンプレートで {{ is_pro }} を参照可能にする（ハンバーガーメニュー等）。
    明示的に render_template(is_pro=...) を渡しているビューはそちらが優先される。"""
    uid = session.get("user_id")
    if not uid:
        return {"is_pro": False}
    try:
        u = db.get_user_by_id(uid)
        return {"is_pro": bool(u and u.get("plan") == "pro")}
    except Exception:
        return {"is_pro": False}


@app.context_processor
def inject_tdnet_banner():
    """TDnet API エラー時にユーザー属性に応じたバナー HTML を注入する"""
    banner = ""
    try:
        status = db.get_system_status("tdnet_status") or "ok"
    except Exception:
        status = "ok"
    if status == "error":
        is_admin = bool(session.get("is_admin"))
        is_pro = False
        uid = session.get("user_id")
        if uid and not is_admin:
            try:
                u = db.get_user_by_id(uid)
                is_pro = bool(u and u.get("plan") == "pro")
            except Exception:
                is_pro = False
        if is_admin:
            banner = (
                '<div style="background:#fef2f2;color:#991b1b;border:1px solid #fca5a5;'
                'border-radius:8px;padding:10px 14px;font-size:0.88rem;margin:12px auto;'
                'max-width:1040px;">⚠ TDnet APIエラーが発生しています</div>'
            )
        elif is_pro:
            banner = (
                '<div style="background:#fffbeb;color:#92400e;border:1px solid #fcd34d;'
                'border-radius:8px;padding:10px 14px;font-size:0.88rem;margin:12px auto;'
                'max-width:1040px;">現在TDnet情報の取得に問題が発生しています</div>'
            )
    return {"tdnet_banner": banner}


@app.errorhandler(CSRFError)
def _handle_csrf_error(e):
    """CSRFトークン切れ・不一致時はログイン画面へ誘導"""
    from flask import redirect, url_for
    return redirect(url_for("login"))


# ---- レート制限 (Flask-Limiter) ----
from flask_limiter import Limiter  # noqa: E402
from flask_limiter.util import get_remote_address  # noqa: E402

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per hour", "30 per minute"],
    storage_uri="memory://",   # gunicorn --workers 1 構成なのでメモリで十分
    headers_enabled=True,
)


@app.after_request
def set_security_headers(response):
    """セキュリティ関連の HTTP レスポンスヘッダを付与する"""
    if _is_prod:
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response


@app.errorhandler(429)
def _ratelimit_handler(e):
    """レート制限到達時はログイン画面に誘導してメッセージ表示"""
    from flask import redirect, url_for, render_template
    msg = "リクエストが多すぎます。しばらく待ってから再度お試しください。"
    try:
        return render_template("login.html", error=msg, next=""), 429
    except Exception:
        return msg, 429


# ---- ブルートフォース対策 ----
# ログイン失敗はクライアント毎の Flask session で管理する
# （Render の LB 環境で IP が毎リクエスト変わる問題を回避）
_LOGIN_MAX_FAILS = 5
_LOGIN_LOCK_SEC = 15 * 60  # ロック期間

# DB 初期化（テーブル作成 + マイグレーション）
# Render 新インスタンス起動直後の一時的な接続失敗に備えてリトライする。
try:
    db.init_db()
    print("[INFO] データベース初期化完了", file=sys.stderr)
except Exception as _e:
    print(f"[WARNING] 起動時DB初期化に失敗しました（接続はリクエスト毎に試みます）: {_e}", file=sys.stderr)


def _start_digest_scheduler():
    """毎分チェックして JST 8:00/18:00 にダイジェストを送信するバックグラウンドスレッド"""
    from datetime import timezone, timedelta
    jst = timezone(timedelta(hours=9))

    def _run():
        sent_today: dict = {}  # {(user_id, hour): date} — 当日分の送信記録
        while True:
            try:
                now = datetime.now(jst)
                today = now.date()
                hour  = now.hour
                users = db.get_users_for_digest_hour(hour)
                for uid in users:
                    key = (uid, hour)
                    if sent_today.get(key) != today:
                        try:
                            import monitor as _m
                            _m.send_digest_for_user(uid)
                            sent_today[key] = today
                        except Exception as e:
                            logger.error("ダイジェスト送信エラー user_id=%s: %s", uid, e)
            except Exception as e:
                logger.error("ダイジェストスケジューラエラー: %s", e)
            time.sleep(60)

    t = threading.Thread(target=_run, daemon=True, name="digest-scheduler")
    t.start()


_start_digest_scheduler()


def _send_tdnet_alert(to_email: str, disclosures: list):
    """新規 TDnet 開示情報をメール通知する"""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import html as _html

    smtp_server   = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port     = int(os.environ.get("SMTP_PORT", "587"))
    sender_email  = os.environ.get("SENDER_EMAIL", "")
    sender_pass   = os.environ.get("SENDER_PASSWORD", "")
    if not sender_email or not sender_pass:
        logger.error("メール送信設定が不足しています (SENDER_EMAIL / SENDER_PASSWORD)")
        return
    if not disclosures:
        return

    rows_html = ""
    for d in disclosures:
        company = _html.escape(d.get("company_name") or "")
        title   = _html.escape(d.get("title") or "")
        pubdate = d.get("disclosed_at")
        pubdate_s = pubdate.strftime("%Y-%m-%d %H:%M") if hasattr(pubdate, "strftime") else _html.escape(str(pubdate or ""))
        url     = _html.escape(d.get("document_url") or "")
        rows_html += (
            f'<div style="padding:12px 0;border-bottom:1px solid #e5e7eb;">'
            f'<div style="font-weight:700;color:#1a1a2e">{company}</div>'
            f'<div style="margin-top:4px">{title}</div>'
            f'<div style="color:#6b7280;font-size:0.85em;margin-top:4px">開示日時: {pubdate_s}</div>'
            f'<div style="margin-top:6px"><a href="{url}" style="color:#3949ab">PDFを見る →</a></div>'
            f'</div>'
        )

    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:600px;margin:0 auto;padding:16px">
<h2 style="font-size:1.1em">BizRadar 適時開示情報</h2>
<p style="color:#4a4a6a">以下の企業から新しい適時開示情報があります。</p>
{rows_html}
<hr style="border:none;border-top:1px solid #e5e7eb;margin-top:24px">
<p style="color:#9ca3af;font-size:0.78em">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""

    msg = MIMEMultipart()
    from email.utils import formataddr as _formataddr
    msg["From"]    = _formataddr(("BizRadar", sender_email))
    msg["To"]      = to_email
    msg["Subject"] = "【BizRadar】適時開示情報があります"
    msg["X-Mailer"] = "BizRadar"
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_pass)
            server.send_message(msg)
        logger.info("[tdnet-alert] sent to=%s count=%d", to_email, len(disclosures))
    except smtplib.SMTPException as e:
        logger.error("[tdnet-alert] send failed to=%s err=%s", to_email, e)


def _notify_tdnet_new(new_doc_ids: list):
    """新規保存された document_id について、Proユーザーの登録企業にマッチする分をメール通知する"""
    if not new_doc_ids:
        logger.info("[tdnet-notify] skipped: new_doc_ids is empty")
        return
    # 新規開示の詳細を取得（securities_code 付き）
    new_items = db.get_tdnet_by_document_ids(new_doc_ids)
    if not new_items:
        logger.info(
            "[tdnet-notify] skipped: new_items is empty (new_doc_ids=%d)",
            len(new_doc_ids),
        )
        return
    pro_users = db.get_pro_users()
    if not pro_users:
        logger.info(
            "[tdnet-notify] skipped: no pro users (new_items=%d)",
            len(new_items),
        )
        return
    logger.info(
        "[tdnet-notify] start: new_doc_ids=%d new_items=%d pro_users=%d",
        len(new_doc_ids), len(new_items), len(pro_users),
    )
    for u in pro_users:
        uid = u["id"]
        email = u["email"]
        try:
            # 該当ユーザーが受け取れる全開示を取得し、その中から新規分だけに絞る
            user_items = db.get_tdnet_for_user(uid)
            user_doc_ids = {i.get("document_id") for i in user_items}
            matched = [i for i in new_items if i.get("document_id") in user_doc_ids]
            logger.info(
                "[tdnet-notify] uid=%s email=%s user_items=%d matched=%d",
                uid, email, len(user_items), len(matched),
            )
            if matched:
                _send_tdnet_alert(email, matched)
        except Exception as e:
            logger.error("[tdnet-alert] user_id=%s error=%s", uid, e)


def _send_simple_mail(to_email: str, subject: str, html_body: str):
    """汎用 SMTP メール送信（TDnet API エラー・復旧通知用）"""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.utils import formataddr as _formataddr

    smtp_server   = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port     = int(os.environ.get("SMTP_PORT", "587"))
    sender_email  = os.environ.get("SENDER_EMAIL", "")
    sender_pass   = os.environ.get("SENDER_PASSWORD", "")
    if not sender_email or not sender_pass or not to_email:
        return
    msg = MIMEMultipart()
    msg["From"]    = _formataddr(("BizRadar", sender_email))
    msg["To"]      = to_email
    msg["Subject"] = subject
    msg["X-Mailer"] = "BizRadar"
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_pass)
            server.send_message(msg)
    except Exception as e:
        logger.error("[simple-mail] send failed to=%s err=%s", to_email, e)


def _notify_tdnet_service_error(error_msg: str):
    """TDnet API 連続エラー発生時に管理者+Proユーザーへ通知"""
    subj = "【BizRadar】TDnet情報の取得に問題が発生しています"
    body = (
        "<p>現在TDnet適時開示情報の取得に問題が発生しています。"
        "復旧次第ご連絡します。</p>"
        f"<p style='color:#9ca3af;font-size:0.85em'>エラー: {error_msg}</p>"
    )
    recipients = {u["email"] for u in db.get_admin_users()}
    recipients |= {u["email"] for u in db.get_pro_users()}
    for email in recipients:
        _send_simple_mail(email, subj, body)
    logger.info("[tdnet-service] error notification sent to %d users", len(recipients))


def _notify_tdnet_service_recovery():
    """TDnet API 復旧時に管理者+Proユーザーへ通知"""
    subj = "【BizRadar】TDnet情報の取得が復旧しました"
    body = "<p>TDnet適時開示情報の取得が正常に復旧しました。</p>"
    recipients = {u["email"] for u in db.get_admin_users()}
    recipients |= {u["email"] for u in db.get_pro_users()}
    for email in recipients:
        _send_simple_mail(email, subj, body)
    logger.info("[tdnet-service] recovery notification sent to %d users", len(recipients))


# 連続失敗カウンタ（gunicorn --workers 1 前提）
_tdnet_error_count = 0


def _run_tdnet_cycle():
    """TDnet 取得の1サイクル。エラー時は3連続で通知、復旧時も通知。"""
    global _tdnet_error_count
    try:
        new_ids = db.fetch_and_save_tdnet()
    except db.TdnetFetchError as e:
        _tdnet_error_count += 1
        logger.error("[tdnet] fetch failed (#%d): %s", _tdnet_error_count, e)
        if _tdnet_error_count >= 3:
            # 3回連続失敗 → 状態が未 error なら遷移させて通知
            try:
                prev = db.get_system_status("tdnet_status")
                if prev != "error":
                    db.set_system_status("tdnet_status", "error")
                    db.set_system_status("tdnet_error_msg", str(e))
                    _notify_tdnet_service_error(str(e))
            except Exception as ee:
                logger.error("[tdnet] failed to record error status: %s", ee)
        return
    # 取得成功
    _tdnet_error_count = 0
    try:
        prev = db.get_system_status("tdnet_status")
        if prev == "error":
            db.set_system_status("tdnet_status", "ok")
            db.set_system_status("tdnet_error_msg", "")
            _notify_tdnet_service_recovery()
    except Exception as e:
        logger.error("[tdnet] failed to record ok status: %s", e)
    _notify_tdnet_new(new_ids)


def _start_tdnet_scheduler():
    """TDnet 適時開示情報を定期取得するバックグラウンドスレッド（15分間隔・起動時即時1回）。
    while ループ全体を try/except で保護し、いかなる Exception が発生しても
    スレッドが終了しないようにする（従来は sleep 中の例外でスレッドが死んでいた）。
    """
    def _run():
        try:
            _run_tdnet_cycle()  # 起動時に1回
        except Exception:
            logger.exception("[tdnet-scheduler] 初回取得で例外発生（継続します）")
        while True:
            try:
                time.sleep(15 * 60)
                _run_tdnet_cycle()
            except Exception:
                logger.exception("[tdnet-scheduler] サイクル内で例外発生（継続します）")
                # 連続失敗時のログスパム抑制＆短クールダウン
                try:
                    time.sleep(60)
                except Exception:
                    pass

    t = threading.Thread(target=_run, daemon=True, name="tdnet-scheduler")
    t.start()
    logger.info("[tdnet-scheduler] thread started name=%s", t.name)


_start_tdnet_scheduler()


def _fetch_and_update_listed_companies():
    import requests, csv, io
    try:
        import pykakasi
    except ImportError:
        logger.error("[jpx] pykakasi not installed")
        return
    try:
        logger.info("[jpx] downloading listed companies data...")
        r = requests.get(
            "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            timeout=30
        )
        if r.status_code != 200:
            logger.error(f"[jpx] download failed: {r.status_code}")
            return
        content = r.content.decode('cp932', errors='replace')
        reader = csv.reader(io.StringIO(content), delimiter='\t')
        kks = pykakasi.kakasi()
        rows = []
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if len(row) < 3:
                continue
            try:
                market = row[0].strip()
                code = row[1].strip().zfill(4)
                name = row[2].strip()
                if not code or not name or not code.isdigit():
                    continue
                result = kks.convert(name)
                kana = ''.join([item['hira'] for item in result])
                rows.append({'securities_code': code, 'company_name': name, 'company_name_kana': kana, 'market': market})
            except Exception:
                continue
        if rows:
            db.upsert_listed_companies(rows)
            logger.info(f"[jpx] upserted {len(rows)} companies")
        else:
            logger.warning("[jpx] no rows parsed")
    except Exception as e:
        logger.error(f"[jpx] error: {e}")


def _start_jpx_scheduler():
    import threading, time
    def _run():
        _fetch_and_update_listed_companies()
        while True:
            time.sleep(7 * 24 * 60 * 60)
            _fetch_and_update_listed_companies()
    t = threading.Thread(target=_run, name='jpx-scheduler', daemon=True)
    t.start()
    logger.info("[jpx-scheduler] thread started")


_start_jpx_scheduler()


def _next_monday_6am_jst():
    """次の月曜日 JST 06:00 を返す"""
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    days_ahead = (0 - now.weekday()) % 7  # 月曜=0
    target = now.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)
    if target <= now:
        target += timedelta(days=7)
    return target


def _start_securities_master_scheduler():
    """JPX 上場銘柄一覧を毎週月曜 06:00 JST に取得するバックグラウンドスレッド（起動時の即時実行なし）"""
    def _run():
        while True:
            jst = timezone(timedelta(hours=9))
            next_run = _next_monday_6am_jst()
            sleep_sec = (next_run - datetime.now(jst)).total_seconds()
            time.sleep(max(60, sleep_sec))
            try:
                db.fetch_and_save_securities_master()
            except Exception as e:
                logger.error("[securities_master] weekly fetch error: %s", e)

    t = threading.Thread(target=_run, daemon=True, name="securities-master-scheduler")
    t.start()


_start_securities_master_scheduler()


@app.before_request
def track_user_activity():
    user_id = session.get("user_id")
    if not user_id:
        return
    last_active = db.get_user_last_active(user_id)
    if last_active is None or (datetime.now(timezone.utc) - last_active).total_seconds() >= 900:
        db.update_last_active(user_id)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login", next=request.path))
        if not session.get("is_admin"):
            flash("管理者のみアクセスできます", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute", methods=["POST"])
def login():
    next_url = request.form.get("next") or request.args.get("next", "")
    if request.method == "POST":
        now = time.time()

        # ロック確認
        locked_until = session.get("login_locked_until", 0)
        if locked_until > now:
            remaining_sec = int(locked_until - now)
            remaining_min = max(1, remaining_sec // 60)
            flash(f"{remaining_min}分後に再度お試しください。", "danger")
            return render_template("login.html", next=next_url)

        # ロック解除済みならリセット
        if locked_until > 0 and locked_until <= now:
            session.pop("login_fail_count", None)
            session.pop("login_fail_first_at", None)
            session.pop("login_locked_until", None)

        email = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        user = db.get_user_by_email(email)
        if user and db.verify_user_password(user, password):
            # 認証成功 → 失敗カウントをクリア
            session.pop("login_fail_count", None)
            session.pop("login_fail_first_at", None)
            session.pop("login_locked_until", None)
            session.permanent = True
            session["user_id"] = user["id"]
            session["email"] = user["email"]
            session["is_admin"] = user["is_admin"]
            db.update_last_login(user["id"])
            return redirect(next_url if next_url.startswith("/") else url_for("index"))

        # 認証失敗
        fail_count = session.get("login_fail_count", 0) + 1
        session["login_fail_count"] = fail_count
        if "login_fail_first_at" not in session:
            session["login_fail_first_at"] = now

        remaining = max(0, _LOGIN_MAX_FAILS - fail_count)

        if fail_count >= _LOGIN_MAX_FAILS:
            session["login_locked_until"] = now + _LOGIN_LOCK_SEC
            flash("ログイン試行が多すぎます。15分後に再度お試しください。", "danger")
        else:
            flash(f"メールアドレスまたはパスワードが正しくありません（残り{remaining}回）", "danger")

        return render_template("login.html", next=next_url)
    return render_template("login.html", next=next_url)


@app.route("/register", methods=["GET", "POST"])
@limiter.limit("3 per hour", methods=["POST"])
def register():
    error = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")

        if not email or "@" not in email:
            error = "有効なメールアドレスを入力してください"
        elif len(password) < 6:
            error = "パスワードは6文字以上で入力してください"
        elif password != confirm:
            error = "パスワードが一致しません"
        elif db.get_user_by_email(email):
            error = "このメールアドレスはすでに登録されています"
        else:
            try:
                user_id = db.create_user(email, password)
                user = db.get_user_by_id(user_id)
                session.permanent = True
                session["user_id"] = user["id"]
                session["email"] = user["email"]
                session["is_admin"] = user["is_admin"]
                flash("アカウントを作成しました", "success")
                return redirect(url_for("index"))
            except Exception as e:
                error = f"登録に失敗しました: {e}"
    return render_template("register.html", error=error)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    user_id   = session["user_id"]
    log       = db.load_monitor_log(user_id)
    hashes    = db.load_hashes()
    config    = db.load_config()
    site_list = db.load_sites_with_company(user_id)
    running    = db.get_running_task_statuses()
    site_statuses  = running.get("site_check", {})
    collecting_kws = set(running.get("keyword_collect", {}).keys())

    sites = []
    for s in site_list:
        url = s["url"]
        check_info = log["last_checks"].get(url, {})
        error_text = check_info.get("error", "")
        status     = check_info.get("status", "unknown")
        error_label, error_cls = _classify_site_error(error_text) if status == "error" else ("", "")
        sites.append({
            "url":         url,
            "name":        s.get("name", ""),
            "company_id":  s.get("company_id"),
            "last_check":  check_info.get("timestamp", "未チェック"),
            "status":      status,
            "error":       error_text,
            "error_label": error_label,
            "error_cls":   error_cls,
            "hash":        hashes.get(url, "-"),
            "checking":    site_statuses.get(url) == "running",
        })

    change_history = log.get("change_history", [])[:50]
    interval = config.get("check_interval_seconds", 3600)

    kw_entries = db.load_keywords(user_id)
    keywords   = [k["keyword"] for k in kw_entries]
    collecting = collecting_kws

    # キーワード → 企業ID マッピング（企業紐づけがあるもののみ）
    kw_with_company = db.load_keywords_with_company(user_id)
    kw_company_map  = {k["keyword"]: k["company_id"] for k in kw_with_company if k.get("company_id")}

    articles_data = db.load_articles_data(user_id)
    all_articles  = articles_data.get("articles", [])
    # 重複排除（同一キーワード内でタイトル類似度が高い記事はYahoo優先で1件に集約）
    all_articles  = _deduplicate_articles(all_articles)
    articles      = all_articles[:300]
    keyword_counts = {}
    for a in all_articles:
        kw = a.get("keyword", "")
        keyword_counts[kw] = keyword_counts.get(kw, 0) + 1

    # 記事に重要フラグ付与（per-company 判定）・published は既存のまま
    _flag_articles_alert(user_id, all_articles, kw_company_map)
    for a in all_articles:
        a["published"] = a.get("published", "")

    # ---- サマリー集計 ---- 重複排除後の all_articles をカウント
    # 重要アラートと通常未読を独立してカウント（未読記事からアラート該当分は除外）
    total_unread       = sum(1 for a in all_articles if not a.get("is_read"))
    alert_count        = sum(1 for a in all_articles if a.get("is_alert") and not a.get("is_read"))
    unread_count       = max(0, total_unread - alert_count)
    error_site_count   = sum(1 for s in sites if s["status"] == "error")
    today_company_list = db.load_active_companies_today(user_id)

    # ---- システムエラーバナー ----
    _SOURCE_DISPLAY = {"google_news": "Google News", "bing_news": "Bing News", "prtimes": "PR TIMES"}
    system_errors = []
    try:
        source_health = db.get_source_health()
        for src, health in source_health.items():
            if health.get("consecutive_failures", 0) >= 3:
                name = _SOURCE_DISPLAY.get(src, src)
                system_errors.append(f"{name}の収集が停止しています")
    except Exception:
        pass
    if error_site_count > 0:
        system_errors.append(f"モニターサイトの取得エラーが {error_site_count} 件あります")
    today_companies    = len(today_company_list)

    # ---- 前回確認時以降の更新企業 ----
    prev_active_at = db.get_user_prev_active(user_id)
    if prev_active_at:
        prev_login_company_list = db.load_active_companies_since(user_id, prev_active_at)
    else:
        prev_login_company_list = []
    prev_login_at = prev_active_at

    total_companies = len(db.load_companies(user_id))

    # TDnet 開示情報（Pro プランのみ、最新5件）+ カード用メトリクス
    tdnet_disclosures = []
    tdnet_today_count = 0
    tdnet_prev_count = 0
    _user = db.get_user_by_id(user_id) or {}
    is_pro = (_user.get("plan") == "pro")
    if is_pro:
        all_tdnet = db.get_tdnet_for_user(user_id)
        tdnet_disclosures = all_tdnet[:5]
        today_str = datetime.now(JST).strftime("%Y-%m-%d")
        tdnet_today_count = sum(
            1 for d in all_tdnet
            if d.get("disclosed_at") and str(d["disclosed_at"])[:10] == today_str
        )
        if prev_active_at:
            tdnet_prev_count = sum(
                1 for d in all_tdnet
                if d.get("disclosed_at") and (d["disclosed_at"].replace(tzinfo=None) if getattr(d["disclosed_at"], "tzinfo", None) else d["disclosed_at"]) >= prev_active_at.replace(tzinfo=None)
            )
        else:
            tdnet_prev_count = tdnet_today_count

    dashboard_settings = db.get_dashboard_settings(user_id)

    return render_template(
        "index.html",
        total_companies=total_companies,
        sites=sites,
        change_history=change_history,
        now=_now_jst_str(),
        check_interval=interval,
        keywords=keywords,
        keyword_entries=kw_entries,
        kw_company_map=kw_company_map,
        articles=articles,
        keyword_counts=keyword_counts,
        keyword_collecting=collecting,
        user_email=session.get("email", ""),
        is_admin=session.get("is_admin", False),
        notify_timing=db.get_user_notify_timing(user_id),
        summary_unread=unread_count,
        summary_alert=alert_count,
        summary_error_sites=error_site_count,
        summary_today_companies=today_companies,
        today_company_list=today_company_list,
        prev_login_company_list=prev_login_company_list,
        prev_login_at=prev_login_at,
        system_errors=system_errors,
        tdnet_disclosures=tdnet_disclosures,
        tdnet_today_count=tdnet_today_count,
        tdnet_prev_count=tdnet_prev_count,
        dashboard_settings=dashboard_settings,
    )


@app.route("/add_site", methods=["POST"])
@login_required
def add_site():
    user_id = session["user_id"]
    url  = request.form.get("url", "").strip()
    name = request.form.get("name", "").strip()

    if not url:
        flash("URLを入力してください", "error")
        return redirect(url_for("management", _anchor="keywords-section"))

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    sites = db.load_sites(user_id)
    if any(s["url"] == url for s in sites):
        flash(f"すでに登録済みです: {url}", "error")
        return redirect(url_for("management", _anchor="keywords-section"))

    # 同一ドメインの既存サイトがあれば警告（登録はブロックしない）
    new_domain = _extract_domain(url)
    if new_domain:
        same_domain = [s for s in sites if _extract_domain(s["url"]) == new_domain]
        for s in same_domain:
            flash(f"このドメインはすでに登録されています：{s['url']}", "warning")

    sites.append({"url": url, "name": name})
    db.save_sites(sites, user_id)
    flash(f"追加しました: {name if name else url}", "success")
    return redirect(url_for("management", _anchor="keywords-section"))


@app.route("/api/add_site", methods=["POST"])
@login_required
def api_add_site():
    user_id = session["user_id"]
    url  = request.form.get("url", "").strip()
    name = request.form.get("name", "").strip()

    if not url:
        return jsonify({"success": False, "message": "URLを入力してください"})

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    sites = db.load_sites(user_id)
    if any(s["url"] == url for s in sites):
        return jsonify({"success": False, "message": f"すでに登録済みです: {url}"})

    # 同一ドメインの警告（登録はブロックしない）
    warnings = []
    new_domain = _extract_domain(url)
    if new_domain:
        same_domain = [s for s in sites if _extract_domain(s["url"]) == new_domain]
        for s in same_domain:
            warnings.append(f"このドメインはすでに登録されています：{s['url']}")

    sites.append({"url": url, "name": name})
    db.save_sites(sites, user_id)
    return jsonify({
        "success": True,
        "url": url,
        "name": name,
        "warnings": warnings,
    })


@app.route("/api/delete_site", methods=["POST"])
@login_required
def api_delete_site():
    user_id = session["user_id"]
    url = request.form.get("url", "").strip()
    if not url:
        return jsonify({"success": False, "message": "URLが不正です"})
    deleted = db.delete_site_by_url(user_id, url)
    if not deleted:
        return jsonify({"success": False, "message": "該当サイトが見つかりません"})
    return jsonify({"success": True})


@app.route("/api/toggle_site", methods=["POST"])
@login_required
def api_toggle_site():
    user_id = session["user_id"]
    url = request.form.get("url", "").strip()
    if not url:
        return jsonify({"success": False, "message": "URLが不正です"})
    new_enabled = db.toggle_site_enabled(user_id, url)
    if new_enabled is None:
        return jsonify({"success": False, "message": "該当サイトが見つかりません"})
    return jsonify({"success": True, "enabled": new_enabled})


@app.route("/remove_site", methods=["POST"])
@login_required
def remove_site():
    user_id = session["user_id"]
    url = request.form.get("url", "").strip()
    sites = db.load_sites(user_id)
    new_sites = [s for s in sites if s["url"] != url]
    if len(new_sites) == len(sites):
        flash("該当URLが見つかりません", "error")
        return redirect(request.referrer or url_for("management"))
    db.save_sites(new_sites, user_id)
    flash(f"削除しました: {url}", "success")
    return redirect(request.referrer or url_for("management"))


@app.route("/update_site_name", methods=["POST"])
@login_required
def update_site_name():
    user_id = session["user_id"]
    url = request.form.get("url", "").strip()
    name = request.form.get("name", "").strip()

    # psycopg2 は NUL を含む文字列で失敗することがあるため除去して安全化
    name = name.replace("\x00", "").strip()

    if not url:
        flash("URLが不正です", "error")
        return redirect(request.referrer or url_for("management"))

    # そのユーザーが登録しているURLか確認（URL固定の保証）
    if not any(s["url"] == url for s in db.load_sites(user_id)):
        flash("該当URLが見つかりません", "error")
        return redirect(request.referrer or url_for("management"))

    ok = db.update_site_name(user_id=user_id, url=url, name=name)
    if not ok:
        flash("会社名の更新に失敗しました", "error")
        return redirect(request.referrer or url_for("management"))

    flash(f"会社名を更新しました: {name if name else url}", "success")
    return redirect(request.referrer or url_for("management"))


@app.route("/api/update_site", methods=["POST"])
@login_required
def api_update_site():
    user_id = session["user_id"]
    data = request.get_json(silent=True) or {}
    old_url  = (data.get("old_url") or "").strip()
    new_url  = (data.get("new_url") or "").strip()
    name     = (data.get("name") or "").replace("\x00", "").strip()

    if not old_url or not new_url:
        return {"ok": False, "error": "URLが不正です"}, 400
    if not new_url.startswith("http"):
        return {"ok": False, "error": "URLはhttpまたはhttpsで始めてください"}, 400
    if not any(s["url"] == old_url for s in db.load_sites(user_id)):
        return {"ok": False, "error": "該当サイトが見つかりません"}, 404

    ok = db.update_site_url_and_name(user_id, old_url, new_url, name)
    if not ok:
        return {"ok": False, "error": "更新に失敗しました"}, 500
    return {"ok": True}


@app.route("/check_site", methods=["POST"])
@login_required
def check_site():
    url = request.form.get("url", "").strip()
    if url in db.get_all_running_tasks().get("site_check", set()):
        flash(f"チェック実行中です: {url}", "error")
        return redirect(request.referrer or url_for("management"))

    user_id = session["user_id"]
    sites = db.load_sites(user_id)
    site_name = next((s.get("name", "") for s in sites if s["url"] == url), "")

    import monitor as monitor_module

    db.add_running_task("site_check", url)

    def run():
        try:
            monitor_module.check_single_site(url, site_name)
        finally:
            db.remove_running_task("site_check", url)

    threading.Thread(target=run, daemon=True).start()
    flash(f"チェックを開始しました: {site_name if site_name else url}", "success")
    return redirect(request.referrer or url_for("management"))


@app.route("/api/check_site", methods=["POST"])
@login_required
def api_check_site():
    url = request.form.get("url", "").strip()
    if not url:
        return jsonify({"success": False, "message": "URLが不正です"})
    if url in db.get_all_running_tasks().get("site_check", set()):
        return jsonify({"success": False, "message": "チェック実行中です"})

    user_id = session["user_id"]
    sites = db.load_sites(user_id)
    site_name = next((s.get("name", "") for s in sites if s["url"] == url), "")

    import monitor as monitor_module

    db.add_running_task("site_check", url)

    def run():
        try:
            monitor_module.check_single_site(url, site_name)
        finally:
            db.remove_running_task("site_check", url)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"success": True, "message": f"収集を開始しました: {site_name if site_name else url}"})


@app.route("/collect_keyword", methods=["POST"])
@login_required
def collect_keyword():
    keyword = request.form.get("keyword", "").strip()
    if keyword in db.get_all_running_tasks().get("keyword_collect", set()):
        flash(f"収集中です: {keyword}", "error")
        return redirect(url_for("index"))

    user_id = session["user_id"]
    import monitor as monitor_module

    db.add_running_task("keyword_collect", keyword)

    def run():
        try:
            monitor_module.check_single_keyword(keyword, user_id)
        except Exception:
            logger.exception(
                "キーワード収集に失敗しました keyword=%r user_id=%s", keyword, user_id
            )
        finally:
            db.remove_running_task("keyword_collect", keyword)

    threading.Thread(target=run, daemon=True).start()
    flash(f"収集を開始しました: {keyword}", "success")
    return redirect(url_for("index"))


@app.route("/api/collect_keyword", methods=["POST"])
@login_required
def api_collect_keyword():
    keyword = request.form.get("keyword", "").strip()
    if not keyword:
        return jsonify({"success": False, "message": "キーワードが不正です"})
    if keyword in db.get_all_running_tasks().get("keyword_collect", set()):
        return jsonify({"success": False, "message": "収集中です"})

    user_id = session["user_id"]
    import monitor as monitor_module

    db.add_running_task("keyword_collect", keyword)

    def run():
        try:
            monitor_module.check_single_keyword(keyword, user_id)
        except Exception:
            logger.exception(
                "キーワード収集に失敗しました keyword=%r user_id=%s", keyword, user_id
            )
        finally:
            db.remove_running_task("keyword_collect", keyword)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"success": True, "message": f"収集を開始しました: {keyword}"})


@app.route("/add_keyword", methods=["POST"])
@login_required
def add_keyword():
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    if not keyword:
        flash("キーワードを入力してください", "error")
        return redirect(url_for("management", _anchor="keywords-section"))
    if db.add_keyword_if_not_exists(user_id, keyword):
        flash(f"キーワードを追加しました: {keyword}", "success")
    else:
        flash(f"すでに登録済みです: {keyword}", "error")
    return redirect(url_for("management", _anchor="keywords-section"))


@app.route("/api/add_keyword", methods=["POST"])
@login_required
def api_add_keyword():
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    if not keyword:
        return jsonify({"success": False, "message": "キーワードを入力してください"})
    if db.add_keyword_if_not_exists(user_id, keyword):
        return jsonify({"success": True, "keyword": keyword})
    return jsonify({"success": False, "message": f"すでに登録済みです: {keyword}"})


@app.route("/remove_keyword", methods=["POST"])
@login_required
def remove_keyword():
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    keywords = db.load_keywords(user_id)
    new_keywords = [k for k in keywords if k["keyword"] != keyword]
    if len(new_keywords) == len(keywords):
        flash("該当キーワードが見つかりません", "error")
        return redirect(url_for("management", _anchor="keywords-section"))
    db.save_keywords(new_keywords, user_id)
    db.delete_articles_by_keyword(user_id, keyword)
    flash(f"キーワードを削除しました: {keyword}", "success")
    return redirect(url_for("management", _anchor="keywords-section"))


@app.route("/toggle_keyword_notify", methods=["POST"])
@login_required
def toggle_keyword_notify():
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    notify_enabled = request.form.get("notify_enabled") == "1"
    if not keyword:
        flash("キーワードが不正です", "error")
        return redirect(url_for("index"))
    if db.update_keyword_notify(user_id, keyword, notify_enabled):
        flash(
            ("メール通知をONにしました" if notify_enabled else "メール通知をOFFにしました")
            + f": {keyword}",
            "success",
        )
    else:
        flash("該当キーワードが見つかりません", "error")
    return redirect(url_for("index", _anchor="keywords-section"))


@app.route("/mark_article_read", methods=["POST"])
@login_required
def mark_article_read():
    user_id = session["user_id"]
    try:
        article_id = int(request.form.get("article_id", "0"))
    except ValueError:
        flash("不正なリクエストです", "error")
        return redirect(url_for("index"))
    if article_id <= 0:
        flash("不正なリクエストです", "error")
        return redirect(url_for("index"))
    if db.mark_article_read(user_id, article_id):
        flash("チェック済みにしました", "success")
    else:
        flash("記事が見つかりません", "error")
    return redirect(request.referrer or url_for("index"))


@app.route("/mark_article_unread", methods=["POST"])
@login_required
def mark_article_unread():
    user_id = session["user_id"]
    try:
        article_id = int(request.form.get("article_id", "0"))
    except ValueError:
        flash("不正なリクエストです", "error")
        return redirect(url_for("index"))
    if article_id <= 0:
        flash("不正なリクエストです", "error")
        return redirect(url_for("index"))
    if db.mark_article_unread(user_id, article_id):
        flash("未読に戻しました", "success")
    else:
        flash("記事が見つかりません", "error")
    return redirect(request.referrer or url_for("index"))


@app.route("/add_alert_keyword", methods=["POST"])
@login_required
def add_alert_keyword():
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    if not keyword:
        flash("キーワードを入力してください", "error")
        return redirect(url_for("management", _anchor="keywords-section"))
    if len(keyword) > 50:
        flash("キーワードは50文字以内で入力してください", "error")
        return redirect(url_for("management", _anchor="keywords-section"))
    if db.add_alert_keyword(user_id, keyword):
        flash(f"アラートキーワード「{keyword}」を追加しました", "success")
    else:
        flash(f"「{keyword}」はすでに登録済みです", "error")
    return redirect(url_for("management", _anchor="keywords-section"))


@app.route("/api/add_alert_keyword", methods=["POST"])
@login_required
def api_add_alert_keyword():
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    if not keyword:
        return jsonify({"success": False, "message": "キーワードを入力してください"})
    if len(keyword) > 50:
        return jsonify({"success": False, "message": "キーワードは50文字以内で入力してください"})
    result = db.add_alert_keyword(user_id, keyword)
    if result is False:
        return jsonify({"success": False, "message": f"「{keyword}」はすでに登録済みです"})
    return jsonify({"success": True, "keyword": keyword, "keyword_id": result})


@app.route("/delete_alert_keyword", methods=["POST"])
@login_required
def delete_alert_keyword():
    user_id = session["user_id"]
    try:
        keyword_id = int(request.form.get("keyword_id", "0"))
    except ValueError:
        flash("不正なリクエストです", "error")
        return redirect(url_for("management", _anchor="keywords-section"))
    db.delete_alert_keyword(user_id, keyword_id)
    return redirect(url_for("management", _anchor="keywords-section"))


@app.route("/company/<int:company_id>/exclude/add", methods=["POST"])
@login_required
def add_company_exclude(company_id):
    user_id = session["user_id"]
    word = (request.form.get("exclude_word") or "").strip()
    if not word:
        flash("除外ワードを入力してください", "error")
        return redirect(request.referrer or url_for("company_list"))
    result = db.add_company_exclude_word(user_id, company_id, word)
    if result is None:
        flash("対象の企業が見つかりません", "error")
    elif result is False:
        flash("この除外ワードは既に登録されています", "error")
    else:
        flash("除外ワードを追加しました", "success")
    return redirect(request.referrer or url_for("company_list"))


@app.route("/company/<int:company_id>/exclude/<int:exclude_id>/delete", methods=["POST"])
@login_required
def delete_company_exclude(company_id, exclude_id):
    user_id = session["user_id"]
    ok = db.delete_company_exclude_word(user_id, company_id, exclude_id)
    if ok:
        flash("除外ワードを削除しました", "success")
    else:
        flash("除外ワードの削除に失敗しました", "error")
    return redirect(request.referrer or url_for("company_list"))


@app.route("/company/<int:company_id>/alert/add", methods=["POST"])
@login_required
def add_company_alert(company_id):
    user_id = session["user_id"]
    kw = (request.form.get("keyword") or "").strip()
    if not kw:
        flash("アラートキーワードを入力してください", "error")
        return redirect(request.referrer or url_for("company_list"))
    result = db.add_company_alert_keyword(user_id, company_id, kw)
    if result is None:
        flash("対象の企業が見つかりません", "error")
    elif result is False:
        flash("このアラートキーワードは既に登録されています", "error")
    else:
        flash("アラートキーワードを追加しました", "success")
    return redirect(request.referrer or url_for("company_list"))


@app.route("/company/<int:company_id>/alert/<int:alert_id>/delete", methods=["POST"])
@login_required
def delete_company_alert(company_id, alert_id):
    user_id = session["user_id"]
    ok = db.delete_company_alert_keyword(user_id, company_id, alert_id)
    if ok:
        flash("アラートキーワードを削除しました", "success")
    else:
        flash("アラートキーワードの削除に失敗しました", "error")
    return redirect(request.referrer or url_for("company_list"))


@app.route("/api/delete_keyword", methods=["POST"])
@login_required
def api_delete_keyword():
    user_id = session["user_id"]
    # 通常キーワード
    keyword = request.form.get("keyword", "").strip()
    if keyword:
        keywords = db.load_keywords(user_id)
        new_keywords = [k for k in keywords if k["keyword"] != keyword]
        if len(new_keywords) == len(keywords):
            return jsonify({"success": False, "message": "該当キーワードが見つかりません"})
        db.save_keywords(new_keywords, user_id)
        db.delete_articles_by_keyword(user_id, keyword)
        return jsonify({"success": True})
    # 重要アラートキーワード
    try:
        keyword_id = int(request.form.get("keyword_id", "0"))
    except ValueError:
        return jsonify({"success": False, "message": "不正なリクエストです"})
    if keyword_id <= 0:
        return jsonify({"success": False, "message": "不正なリクエストです"})
    db.delete_alert_keyword(user_id, keyword_id)
    return jsonify({"success": True})


@app.route("/mark_read/<int:article_id>", methods=["POST"])
@login_required
def mark_read_api(article_id):
    user_id = session["user_id"]
    if article_id <= 0:
        return jsonify({"ok": False, "error": "invalid id"}), 400
    ok = db.mark_article_read(user_id, article_id)
    return jsonify({"ok": ok})


@app.route("/mark_unread/<int:article_id>", methods=["POST"])
@login_required
def mark_unread_api(article_id):
    user_id = session["user_id"]
    if article_id <= 0:
        return jsonify({"ok": False, "error": "invalid id"}), 400
    ok = db.mark_article_unread(user_id, article_id)
    return jsonify({"ok": ok})


@app.route("/api/keyword_article_count")
@login_required
def api_keyword_article_count():
    user_id = session["user_id"]
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"count": 0})
    count = db.count_articles_by_keyword(user_id, keyword)
    return jsonify({"count": count})


@app.route("/api/articles")
@login_required
def api_articles():
    user_id = session["user_id"]
    unread_only = request.args.get("unread_only", "false").lower() == "true"
    data = db.load_articles_data(user_id)
    articles = data.get("articles", [])
    if unread_only:
        articles = [a for a in articles if not a.get("is_read")]
    return jsonify(articles)


@app.route("/api/tdnet/company", methods=["GET"])
@csrf.exempt
def api_tdnet_company():
    """証券コードから企業名候補を返す（securities_master と tdnet_disclosures の両方を検索）。
    Query: code (例: 7203)
    Response: {"companies": ["トヨタ自動車", ...]} （最大5件・ヒットなしは空リスト）"""
    code = (request.args.get("code") or "").strip()
    if not code:
        return jsonify({"companies": []})
    merged: list = []
    seen: set = set()

    def _add(names):
        for n in names:
            if n and n not in seen:
                seen.add(n)
                merged.append(n)

    try:
        # 1) JPX 上場銘柄マスタ
        _add(db.lookup_securities_master_by_code(code))
        # 2) TDnet 実開示データ
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT company_name FROM tdnet_disclosures "
                    "WHERE securities_code LIKE %s "
                    "ORDER BY company_name LIMIT 5",
                    (f"%{code}%",),
                )
                _add([r[0] for r in cur.fetchall()])
    except Exception as e:
        logger.error("[api_tdnet_company] error: %s", e)
        return jsonify({"companies": []})
    return jsonify({"companies": merged[:5]})


@app.route('/api/company_lookup')
@login_required
def api_company_lookup():
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({})
    result = db.search_listed_company(name)
    if result:
        return jsonify(result)
    return jsonify({})


@app.route("/api/suggest_url")
@login_required
def api_suggest_url():
    """入力URLを受け取り、より適切な登録候補URLを返す。
    優先順位:
      1. sitemap.xml を取得・解析 → ニュース系URLを抽出して提案
      2. /feed が 200 なら提案
      3. /rss  が 200 なら提案
      4. パターンマッチ（keizai.biz 等）にフォールバック
    """
    import requests as _requests
    from urllib.parse import urlparse

    # sitemap URL の優先度別キーワード
    _KW_HIGH = re.compile(
        r"news|press|release|newsroom|お知らせ|ニュース|プレスリリース", re.I
    )
    _KW_MID = re.compile(
        r"topics|information|info|announce", re.I
    )
    _KW_EXCLUDE = re.compile(
        r"customer|support|product|service|recruit|campaign|energy", re.I
    )

    def _fetch_get(url):
        """GET リクエストを試み、(Response or None) を返す"""
        try:
            return _requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; BizRadar/1.0)"},
                timeout=3,
                allow_redirects=True,
                verify=False,
            )
        except Exception:
            return None

    def _fetch_head(url):
        """HEAD リクエストを試み、ステータスコード (int or None) を返す"""
        try:
            r = _requests.head(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; BizRadar/1.0)"},
                timeout=3,
                allow_redirects=True,
                verify=False,
            )
            return r.status_code
        except Exception:
            return None

    # 個別記事ページ判定: パスに6桁以上の連続数字（日付・ID）を含む
    _DATE_DIGITS = re.compile(r"\d{6,}")

    def _kw_tier(url, kw_pattern):
        """キーワードが現れるパスセグメントの位置でティアを返す（小さいほど優先）。
        tier 0: 第1セグメント  例) /news/
        tier 1: 第2セグメント  例) /corporate/press/
        tier 2: 第3セグメント以降  例) /home/customer-support/info/
        """
        from urllib.parse import urlparse as _up
        segments = [s for s in _up(url).path.strip("/").split("/") if s]
        kw_idx = next(
            (i for i, s in enumerate(segments) if kw_pattern.search(s)),
            len(segments),
        )
        return min(kw_idx, 2)

    def _best_from(candidates, kw_pattern):
        """候補URLリストから一覧ページらしい最良の1件を選ぶ。
        スコア = (tier, has_date, depth) の昇順で最小を選択。
          tier:     キーワード位置ティア（0=最優先, 2=後回し）
          has_date: 6桁以上の連続数字を含む場合 1（記事URLを後回し）
          depth:    パスのスラッシュ数（浅いほど優先）
        """
        from urllib.parse import urlparse as _up

        def _score(url):
            path = _up(url).path
            tier     = _kw_tier(url, kw_pattern)
            has_date = 1 if _DATE_DIGITS.search(path) else 0
            depth    = path.rstrip("/").count("/")
            return (tier, has_date, depth)

        return min(candidates, key=_score) if candidates else None

    def _extract_news_url_from_sitemap(xml_text):
        """sitemap XML からニュース系URLを優先度付きで抽出する。
        優先度高 → 中 の順に候補を収集し、階層が浅く日付数字を含まない
        一覧ページらしいURLを選んで返す。
        除外キーワードのみを含むURLはスキップする。
        """
        from urllib.parse import urlparse as _up
        locs = re.findall(r"<loc>\s*(https?://[^\s<]+)\s*</loc>", xml_text)
        high_candidates = []
        mid_candidates  = []
        for loc in locs:
            path = _up(loc).path
            has_exclude = bool(_KW_EXCLUDE.search(path))
            has_high    = bool(_KW_HIGH.search(path))
            has_mid     = bool(_KW_MID.search(path))
            # 除外キーワードのみ → スキップ（高・中キーワードも含む場合は通す）
            if has_exclude and not has_high and not has_mid:
                continue
            if has_high:
                high_candidates.append(loc.rstrip("/") + "/")
            elif has_mid:
                mid_candidates.append(loc.rstrip("/") + "/")
        # 高優先候補から一覧ページを選び、なければ中優先候補で選ぶ
        return _best_from(high_candidates, _KW_HIGH) or _best_from(mid_candidates, _KW_MID)

    raw = request.args.get("url", "").strip()
    if not raw:
        return jsonify({"suggested": None})

    # ベースURL（スキーム＋ホスト）を取得
    try:
        parsed = urlparse(raw)
        if not parsed.scheme or not parsed.netloc:
            return jsonify({"suggested": None})
        base = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return jsonify({"suggested": None})

    # 0. ドメイン固有の特例ルール（サイトマップ検索より優先）— DBから読み込み
    _DOMAIN_OVERRIDES_DB = db.get_domain_overrides_dict()

    # ハードコード分（DB未登録時のフォールバック／初回マイグレーション用）
    _DOMAIN_OVERRIDES_HARDCODED = {
        "www.tohogas.co.jp":           "https://www.tohogas.co.jp/corporate-n/press/",
        "www.aisin.com":               "https://www.aisin.com/news/",
        "www.toyota-shokki.co.jp":     "https://www.toyota-shokki.co.jp/news/",
        "www.makita.co.jp":            "https://www.makita.co.jp/news/",
        "www.dmgmori.co.jp":           "https://www.dmgmori.co.jp/news/",
        "www.kagome.co.jp":            "https://www.kagome.co.jp/company/news/",
        "www.suzuken.co.jp":           "https://www.suzuken.co.jp/news/",
        "store.alpen-group.jp":        "https://store.alpen-group.jp/news/",
        "www.sugi-net.jp":             "https://www.sugi-net.jp/news/",
        "www.aeon.info":               "https://www.aeon.info/news/",
        "www.komeda-holdings.co.jp":   "https://www.komeda-holdings.co.jp/news/",
        "www.buffalo.jp":              "https://www.buffalo.jp/press/",
        "www.chuden.co.jp":            "https://www.chuden.co.jp/topics/",
        "powergrid.chuden.co.jp":      "https://powergrid.chuden.co.jp/news/",
        "www.e-meitetsu.com":          "https://www.e-meitetsu.com/news/",
        "www.meiko-trans.co.jp":       "https://www.meiko-trans.co.jp/news/",
        "www.hoshizaki.co.jp":         "https://www.hoshizaki.co.jp/news/",
        "www.ngkntk.co.jp":            "https://www.ngkntk.co.jp/news/",
        "www.fujimiinc.co.jp":         "https://www.fujimiinc.co.jp/topics/",
        "www.noritake.co.jp":          "https://www.noritake.co.jp/news/",
        "www.aica.co.jp":              "https://www.aica.co.jp/news/",
        "www.meikokensetsu.co.jp":     "https://www.meikokensetsu.co.jp/news/",
        "www.tokai-rika.co.jp":        "https://www.tokai-rika.co.jp/news/",
        "www.okb.co.jp":               "https://www.okb.co.jp/news/",
        "www.juroku.co.jp":            "https://www.juroku.co.jp/news/",
        "www.hyakugo.co.jp":           "https://www.hyakugo.co.jp/news/",
        "www.33fg.co.jp":              "https://www.33fg.co.jp/news/",
        "www.ibiden.co.jp":            "https://www.ibiden.co.jp/news/",
        "valorholdings.co.jp":         "https://valorholdings.co.jp/news/",
        "www.tokai-corp.com":          "https://www.tokai-corp.com/news/",
        "www.mirai.co.jp":             "https://www.mirai.co.jp/topics/",
        "www.tyk.co.jp":               "https://www.tyk.co.jp/news/",
        "www.apple-international.com": "https://www.apple-international.com/news/",
        "www.imuraya-group.com":       "https://www.imuraya-group.com/news/",
        "www.oyatsu.co.jp":            "https://www.oyatsu.co.jp/news/",
        "www.nagashima-onsen.co.jp":   "https://www.nagashima-onsen.co.jp/press/",
        "www.zent.co.jp":              "https://www.zent.co.jp/news/",
        "www.chunichi.co.jp":          "https://www.chunichi.co.jp/news/",
        "hicbc.com":                   "https://hicbc.com/news/",
        "www.tokai-tv.com":            "https://www.tokai-tv.com/press/",
        "www.nagoyatv.com":            "https://www.nagoyatv.com/news/",
        "zip-fm.co.jp":                "https://zip-fm.co.jp/news/",
        "www.beingcorp.co.jp":         "https://www.beingcorp.co.jp/news/",
        "www.33bank.co.jp":            "https://www.33bank.co.jp/news/",
        # ── 民間企業（追加分）──────────────────────────────────────
        "www.musashi.co.jp":           "https://www.musashi.co.jp/ir/news/",
        "www.aichitokei.co.jp":        "https://www.aichitokei.co.jp/news/",
        "www.chuoseiki.co.jp":         "https://www.chuoseiki.co.jp/news/",
        "www.n-sharyo.co.jp":          "https://www.n-sharyo.co.jp/finance/ir_news.htm/",
        "www.howa.co.jp":              "https://www.howa.co.jp/news/",
        "www.otics.co.jp":             "https://www.otics.co.jp/feed",
        "www.maruyasu.co.jp":          "https://www.maruyasu.co.jp/feed",
        "www.tokaikogyo.co.jp":        "https://www.tokaikogyo.co.jp/feed",
        "www.fujipan.co.jp":           "https://www.fujipan.co.jp/news/",
        "www.pasconet.co.jp":          "https://www.pasconet.co.jp/release/",
        "www.super-yamanaka.co.jp":    "https://www.super-yamanaka.co.jp/feed",
        "feel-corp.jp":                "https://feel-corp.jp/news/",
        "www.aokisuper.co.jp":         "https://www.aokisuper.co.jp/news/",
        "www.genky.co.jp":             "https://www.genky.co.jp/topics/",
        "valor.jp":                    "https://valor.jp/feed",
        "www.gifushin.co.jp":          "https://www.gifushin.co.jp/release/",
        "www.tokairadio.co.jp":        "https://www.tokairadio.co.jp/news/",
        "www.komehyo.co.jp":           "https://www.komehyo.co.jp/news/",
        "www.medius.co.jp":            "https://www.medius.co.jp/feed",
        "www.mitachi.co.jp":           "https://www.mitachi.co.jp/news/",
        "www.resorttrust.co.jp":       "https://www.resorttrust.co.jp/company/news/",
        "www.mtg.gr.jp":               "https://www.mtg.gr.jp/news/",
        "www.trinityind.co.jp":        "https://www.trinityind.co.jp/feed",
        "www.taihonet.co.jp":          "https://www.taihonet.co.jp/news/",
        "holdings.sanco.co.jp":        "https://holdings.sanco.co.jp/news/",
        "www.mikimoto.com":            "https://www.mikimoto.com/jp_jp/news/",
        "www.sangetsu.co.jp":          "https://www.sangetsu.co.jp/company/news/",
        "www.ckd.co.jp":               "https://www.ckd.co.jp/news/",
        "www.yahagi.co.jp":            "https://www.yahagi.co.jp/news/",
        # ── 官公庁・自治体（追加分）───────────────────────────────
        "www.pref.aichi.jp":           "https://www.pref.aichi.jp/rss",
        "www.pref.gifu.lg.jp":         "https://www.pref.gifu.lg.jp/rss",
        "www.pref.mie.lg.jp":          "https://www.pref.mie.lg.jp/koho/",
        "www.city.nagoya.jp":          "https://www.city.nagoya.jp/rss",
        "www.city.toyota.aichi.jp":    "https://www.city.toyota.aichi.jp/topics/",
        "www.city.kasugai.lg.jp":      "https://www.city.kasugai.lg.jp/rss",
        "www.city.gifu.lg.jp":         "https://www.city.gifu.lg.jp/info/",
        "www.city.kani.lg.jp":         "https://www.city.kani.lg.jp/info/",
        "www.city.nakatsugawa.lg.jp":  "https://www.city.nakatsugawa.lg.jp/topics/",
        "www.city.kuwana.lg.jp":       "https://www.city.kuwana.lg.jp/news/",
        "www.city.matsusaka.mie.jp":   "https://www.city.matsusaka.mie.jp/rss",
        "www.city.nabari.lg.jp":       "https://www.city.nabari.lg.jp/news.html/",
        "www.chubu.meti.go.jp":        "https://www.chubu.meti.go.jp/rss",
        "www.nta.go.jp":               "https://www.nta.go.jp/information/news/index.htm/",
        "global.toyota":               "https://global.toyota/jp/newsroom/",
        # ── 民間企業（追加分7）────────────────────────────────────────
        "www.fujikikai-inc.co.jp":     "https://www.fujikikai-inc.co.jp/news/",
        "www.amada.co.jp":             "https://www.amada.co.jp/ja/info/",
        "www.jr-takashimaya.co.jp":    "https://www.jr-takashimaya.co.jp/info/",
        "www.yabaton.com":             "https://www.yabaton.com/modules/news/",
        "www.maruha-net.co.jp":        "https://www.maruha-net.co.jp/category/news/",
        "www.sugakico.co.jp":          "https://www.sugakico.co.jp/news/",
        "www.akafuku.co.jp":           "https://www.akafuku.co.jp/topics/",
        "www.kakiyasuhonten.co.jp":    "https://www.kakiyasuhonten.co.jp/news/",
        "www.yunoyama-onsen.com":      "https://www.yunoyama-onsen.com/news/",
        "www.meitetsu-kankobus.co.jp": "https://www.meitetsu-kankobus.co.jp/topics/",
        "www.m-cd.co.jp":              "https://www.m-cd.co.jp/information/",
        "www.sunace.co.jp":            "https://www.sunace.co.jp/news/",
        "www.kanesue.co.jp":           "https://kanesue.co.jp/news/",
        "www.brass.ne.jp":             "https://www.brass.ne.jp/corporate/news/",
        "www.zetton.co.jp":            "https://www.zetton.co.jp/news",
        "www.plandosee.co.jp":         "https://www.plandosee.co.jp/information/",
        "www.yagami.co.jp":            "https://www.yagami.co.jp/news/",
        "www.kawai-juku.ac.jp":        "https://www.kawai-juku.ac.jp/information/",
        "www.meishinken.co.jp":        "https://www.meishinken.co.jp/news",
        "www.hamagakuen.co.jp":        "https://www.hamagakuen.co.jp/press/",
        "www.meitetsu-hospital.jp":    "https://www.meitetsu-hospital.jp/news/",
        "www.nagoya-ekisaikaihosp.jp": "https://www.nagoya-ekisaikaihosp.jp/news",
        "www.nagoya.tokushukai.or.jp": "https://www.nagoya.tokushukai.or.jp/wp/news/",
        "www.gifu-np.co.jp":           "https://www.gifu-np.co.jp/list/news",
        "www.zf-web.com":              "https://www.zf-web.com/news/",
        "fmmie.jp":                    "https://fmmie.jp/topics/",
        # ── 民間企業（追加分6）────────────────────────────────────────
        "www.toyoda-gosei.co.jp":      "https://www.toyoda-gosei.co.jp/news/",
        "www.shiroki.co.jp":           "https://www.shiroki.co.jp/news/",
        "www.takagi-mfg.co.jp":        "https://takagi-mfg.co.jp/news",
        "www.nagoya-denki.co.jp":      "https://www.nagoya-denki.co.jp/news/",
        "www.aichidensen.co.jp":       "https://www.aichidensen.co.jp/news/",
        "www.toyotahome-aichi.co.jp":  "https://aichi.toyotahome.co.jp/info/",
        "www.sekisuihouse.co.jp":      "https://www.sekisuihouse.co.jp/company/newsroom/",
        "www.cti.co.jp":               "https://www.cti.co.jp/news/",
        "www.ctc.co.jp":               "https://www.ctc.co.jp/news/",
        "www.tokaitokyo-fh.jp":        "https://www.tokaitokyo-fh.jp/news/",
        "www.tokaitokyo.co.jp":        "https://www.tokaitokyo.co.jp/company/news/",
        "www.setoshin.co.jp":          "https://www.setoshin.co.jp/topics/",
        "www.hekishin.jp":             "https://www.hekishin.jp/news/",
        "www.ogakiseino-shinkin.co.jp":"https://www.ogakiseino-shinkin.co.jp/news/",
        "www.ctv.co.jp":               "https://www.ctv.co.jp/announce/",
        "www.starcat.co.jp":           "https://www.starcat.co.jp/announcement/",
        "www.meitetsu-bus.co.jp":      "https://www.meitetsu-bus.co.jp/info/",
        "www.toyotetsu.com":           "https://www.toyotetsu.com/news/",
        "www.aonamiline.co.jp":        "https://www.aonamiline.co.jp/news/",
        "www.port-of-nagoya.jp":       "https://www.port-of-nagoya.jp/news.html",
        "www.toyo.co.jp":              "https://www.toyo.co.jp/news/",
        "www.tsuchiya.co.jp":          "https://www.tsuchiya.co.jp/news/",
        "www.rinnai.co.jp":            "https://www.rinnai.co.jp/corp/news/",
        "www.paloma.co.jp":            "https://www.paloma.co.jp/corporate/news/index.html",
        "melco-hd.jp":                 "https://melco-hd.jp/topics/",
        "www.sun-denshi.co.jp":        "https://www.sun-denshi.co.jp/news/",
        "www.elmo.co.jp":              "https://www.elmo.co.jp/news/",
        "www.kawamura.co.jp":          "https://www.kawamura.co.jp/news/",
        "www.maspro.co.jp":            "https://www.maspro.co.jp/info/",
        "www.brother.co.jp":           "https://www.brother.co.jp/news/index.aspx",
        "www.menicon.co.jp":           "https://www.menicon.co.jp/company/news/",
        "www.komeda.co.jp":            "https://www.komeda.co.jp/news/",
        "www.ohsho.co.jp":             "https://www.ohsho.co.jp/info/",
        "www.korona.co.jp":            "https://korona.co.jp/news/",
        "www.risupack.co.jp":          "https://www.risupack.co.jp/risupack_contents/topics/",
        # ── 民間企業（追加分5）────────────────────────────────────────
        "www.cns.co.jp":               "https://www.cns.co.jp/news/",
        "avex.com":                    "https://avex.com/jp/ja/news/",
        "www.maruhon.co.jp":           "https://www.maruhon.co.jp/news/",
        "www.honda.co.jp":             "https://global.honda/jp/pressroom/",
        # ── 民間企業（追加分4）────────────────────────────────────────
        "www.advics.co.jp":            "https://www.advics.co.jp/news/",
        "www.hosei.co.jp":             "https://www.hosei.co.jp/news/",
        "www.tytlabs.co.jp":           "https://www.tytlabs.co.jp/ja/news.html",
        "www.toyotahome.co.jp":        "https://www.toyotahome.co.jp/info/archive/",
        "www.toyota-finance.co.jp":    "https://www.toyota-finance.co.jp/newsrelease/",
        "www.toyotaconnected.co.jp":   "https://www.toyotaconnected.co.jp/news/",
        "www.toyotasystems.com":       "https://www.toyotasystems.com/news/",
        "www.toyota-ep.co.jp":         "https://www.toyota-ep.co.jp/news/",
        "www.toyota-lf.com":           "https://www.toyota-lf.com/news/",
        "www.kojima-tns.co.jp":        "https://www.kojima-tns.co.jp/news/",
        "www.futabasangyo.com":        "https://www.futabasangyo.com/news/",
        "www.sumitomoriko.co.jp":      "https://www.sumitomoriko.co.jp/news/",
        "www.hayashi-telempu.com":     "https://www.hayashi-telempu.com/news/",
        "www.aichikikai.co.jp":        "https://www.aichikikai.co.jp/news/",
        "www.kanemi-foods.co.jp":      "https://www.kanemi-foods.co.jp/news/",
        "www.morita119.com":           "https://www.morita119.com/news/",
        "www.takihyo.co.jp":           "https://www.takihyo.co.jp/category/news/",
        "www.sakai-holdings.co.jp":    "https://www.sakai-holdings.co.jp/news/",
        "www.vt-holdings.co.jp":       "https://www.vt-holdings.co.jp/news/pr/index.html",
        "www.nds-g.co.jp":             "https://www.nds-g.co.jp/news/",
        "www.siix.co.jp":              "https://www.siix.co.jp/wordpress/news/",
        "www.daiseki.co.jp":           "https://www.daiseki.co.jp/news/",
        "www.daiseki-eco.co.jp":       "https://www.daiseki-eco.co.jp/info/",
        "www.ftgroup.co.jp":           "https://www.ftgroup.co.jp/newsrelease/",
        "tobila.com":                  "https://tobila.com/news/",
        "www.jbr.co.jp":               "https://www.jbr.co.jp/news/",
        "www.tear.co.jp":              "https://www.tear.co.jp/news/",
        "www.kisoji.co.jp":            "https://www.kisoji.co.jp/news/",
        "www.monogatari.co.jp":        "https://www.monogatari.co.jp/news/",
        "www.sagami-holdings.co.jp":   "https://www.sagami-holdings.co.jp/newsrelease/",
        "www.hamayuu.co.jp":           "https://www.hamayuu.co.jp/news/",
        "www.bronco.co.jp":            "https://www.bronco.co.jp/news/",
        "www.amiyakitei.co.jp":        "https://www.amiyakitei.co.jp/ir/press/",
        "www.jgroup.jp":               "https://www.jgroup.jp/news/",
        "www.colowide.co.jp":          "https://www.colowide.co.jp/information/",
        "www.kurasushi.co.jp":         "https://www.kurasushi.co.jp/news/index.html",
        "www.fujitrans.co.jp":         "https://www.fujitrans.co.jp/news/",
        "www.konoike.net":             "https://www.konoike.net/news/",
        "www.chubukohan.co.jp":        "https://www.chubukohan.co.jp/news",
        "www.sinto.co.jp":             "https://www.sinto.co.jp/news/",
        "www.unipres.co.jp":           "https://www.unipres.co.jp/news/",
        "www.osg.co.jp":               "https://www.osg.co.jp/about_us/ir/news/",
        "www.asahi-intecc.co.jp":      "https://www.asahi-intecc.co.jp/news/",
        "www.aichidenki.jp":           "https://www.aichidenki.jp/news/index.html",
        "www.kawai.jp":                "https://www.kawai.jp/news/",
        "corp.renet.jp":               "https://corp.renet.jp/ir/news/index.html",
        "www.asukanet.co.jp":          "https://www.asukanet.co.jp/contents/news/index.html",
        "www.gifubody.co.jp":          "https://www.gifubody.co.jp/news/index.html",
        "www.chuco.co.jp":             "https://www.chuco.co.jp/news",
        "www.iwakipumps.jp":           "https://www.iwakipumps.jp/news/",
        "www.maruichi.com":            "https://www.maruichi.com/news/",
        "www.mieden.co.jp":            "https://www.mieden.co.jp/news/",
        "www.jmuc.co.jp":              "https://www.jmuc.co.jp/news/",
        "www.mietv.com":               "https://www.mietv.com/news",
        "www.d-kintetsu.co.jp":        "https://www.d-kintetsu.co.jp/info/",
        "www.gozaisho.co.jp":          "https://www.gozaisho.co.jp/news/",
        # ── 民間企業（追加分3）────────────────────────────────────────
        "www.toenec.co.jp":            "https://www.toenec.co.jp/news/",
        "www.tokura.co.jp":            "https://www.tokura.co.jp/news/",
        "www.ngk.co.jp":               "https://www.ngk.co.jp/ir/news/",
        "www.nichiha.co.jp":           "https://www.nichiha.co.jp/news/",
        "www.maruwa-g.com":            "https://www.maruwa-g.com/company/news/",
        "www.chkk.co.jp":              "https://www.chkk.co.jp/news/index.html",
        "www.aisan-ind.co.jp":         "https://www.aisan-ind.co.jp/news/index.html",
        "www.pacific-ind.co.jp":       "https://www.pacific-ind.co.jp/news/",
        "www.mitsuboshi.com":          "https://www.mitsuboshi.com/news/",
        "www.fine-sinter.com":         "https://www.fine-sinter.com/news/",
        "www.yutakagiken.co.jp":       "https://www.yutakagiken.co.jp/news/",
        "www.nok.co.jp":               "https://www.nok.co.jp/news/",
        "www.edion.co.jp":             "https://www.edion.co.jp/news",
        "www.hc-kohnan.com":           "https://www.hc-kohnan.com/news/",
        "www.dcm-hc.co.jp":           "https://www.dcm-hc.co.jp/news/",
        "www.heiwado.jp":              "https://www.heiwado.jp/news",
        "www.yamada-holdings.jp":      "https://www.yamada-holdings.jp/ir/news.html",
        "www.arclands.co.jp":          "https://www.arclands.co.jp/ja/ir/news",
        "www.chubushiryo.co.jp":       "https://www.chubushiryo.co.jp/news/",
        "www.nipponham.co.jp":         "https://www.nipponham.co.jp/news/",
        "www.marusanai.co.jp":         "https://www.marusanai.co.jp/news/",
        "www.komi.co.jp":              "https://www.komi.co.jp/news",
        "www.hamaotome.co.jp":         "https://www.hamaotome.co.jp/info/",
        "www.pokkasapporo-fb.jp":      "https://www.pokkasapporo-fb.jp/company/news/",
        "www.pasco.co.jp":             "https://www.pasco.co.jp/topics/",
        "www.hikkoshi-sakai.co.jp":    "https://www.hikkoshi-sakai.co.jp/news/",
        "www.nittsu.co.jp":            "https://www.nittsu.co.jp/info/",
        "corp.fukutsu.co.jp":          "https://corp.fukutsu.co.jp/corp/news/",
        "www.kwe.com":                 "https://www.kwe.com/news/",
        "www.trancom.co.jp":           "https://www.trancom.co.jp/news/",
        "www.toyo-logistics.co.jp":    "https://www.toyo-logistics.co.jp/news/",
        "www.nikku.co.jp":             "https://www.nikku.co.jp/ja/news.html",
        "www.ctechcorp.co.jp":         "https://www.ctechcorp.co.jp/news/",
        "www.ut-g.co.jp":              "https://www.ut-g.co.jp/news/",
        "www.alpsgiken.co.jp":         "https://www.alpsgiken.co.jp/ir/news/index.shtml",
        "www.cmc.co.jp":               "https://www.cmc.co.jp/news/",
        "www.tis.co.jp":               "https://www.tis.co.jp/news/",
        "www.totec.jp":                "https://www.totec.jp/news/",
        "www.sr-net.co.jp":            "https://www.sr-net.co.jp/news/",
        "life-design.a-tm.co.jp":      "https://life-design.a-tm.co.jp/news/",
        "oh.openhouse-group.com":      "https://oh.openhouse-group.com/company/news/",
        "www.stepon.co.jp":            "https://www.stepon.co.jp/news/",
        "www.nomura-solutions.co.jp":  "https://www.nomura-solutions.co.jp/news/",
        "athome-inc.jp":               "https://athome-inc.jp/news/",
        "www.linical.com":             "https://www.linical.com/ja/news-and-events",
        "www.sugi-hd.co.jp":           "https://www.sugi-hd.co.jp/news/",
        "www.m-ikkou.co.jp":           "https://www.m-ikkou.co.jp/news/",
        "www.chukyoiyakuhin.co.jp":    "https://chukyoiyakuhin.co.jp/news/",
        "medpeer.co.jp":               "https://medpeer.co.jp/news",
        "www.ikont.co.jp":             "https://www.ikont.co.jp/news.html",
        "www.sunmesse.co.jp":          "https://www.sunmesse.co.jp/news/",
        "www.cgco.co.jp":              "https://www.cgco.co.jp/news/",
        "www.sanko-kk.co.jp":          "https://www.sanko-kk.co.jp/news/",
        "www.rikentechnos.co.jp":      "https://www.rikentechnos.co.jp/information/",
        "www.kobelco.co.jp":           "https://www.kobelco.co.jp/releases/",
        "www.sanco.co.jp":             "https://www.sanco.co.jp/newsrelease/",
        "www.unicharm.co.jp":          "https://www.unicharm.co.jp/ja/company/news.html",
        # ── 民間企業・業界団体（追加分2）──────────────────────────────
        "www.sala.jp":                 "https://www.sala.jp/ja/news.html",
        "miraini-gr.com":              "https://miraini-gr.com/news/",
        "www.hagiwara.co.jp":          "https://www.hagiwara.co.jp/news/info/",
        "www.cjqca.com":               "https://www.cjqca.com/cqca_news/",
        "www.chukei-news.co.jp":       "https://www.chukei-news.co.jp/news/",
        "www.nagoya-cci.or.jp":        "https://www.nagoya-cci.or.jp/koho/news-release/index.html",
        "www.chukeiren.or.jp":         "https://www.chukeiren.or.jp/news/",
        "www.jimin.jp":                "https://www.jimin.jp/news/",
        "meieki.keizai.biz":           "https://meieki.keizai.biz/headline/archives/1/",
        "kk-matsuo-ss.co.jp":          "https://kk-matsuo-ss.co.jp/news/",
        "www.yamaichi-hagane.jp":      "https://www.yamaichi-hagane.jp/news/",
    }
    # ハードコード辞書をマイグレーション用に関数属性として保存
    api_suggest_url._hardcoded = _DOMAIN_OVERRIDES_HARDCODED
    # DB優先、なければハードコードにフォールバック
    _DOMAIN_OVERRIDES = {**_DOMAIN_OVERRIDES_HARDCODED, **_DOMAIN_OVERRIDES_DB}
    if parsed.netloc in _DOMAIN_OVERRIDES:
        return jsonify({"suggested": _DOMAIN_OVERRIDES[parsed.netloc]})

    # 1. sitemap.xml を取得して解析
    sitemap_resp = _fetch_get(base + "/sitemap.xml")
    if sitemap_resp and sitemap_resp.status_code == 200:
        news_url = _extract_news_url_from_sitemap(sitemap_resp.text)
        if news_url:
            return jsonify({"suggested": news_url})
        # sitemap は存在したがニュース系URLが見つからなかった → 次へ進む

    # 2. /feed → 3. /rss の順に HEAD で確認
    for path in ("/feed", "/rss"):
        if _fetch_head(base + path) == 200:
            return jsonify({"suggested": base + path})

    # 4. パターンマッチ（keizai.biz など）にフォールバック
    # ─ 将来のパターンは SITE_URL_RULES リストに追加する ─
    SITE_URL_RULES = [
        {
            # keizai.biz: サブドメインのトップページ → 記事一覧ページ
            "pattern": re.compile(r"^https?://([a-z0-9-]+)\.keizai\.biz/?$", re.I),
            "suggest": lambda m: f"https://{m.group(1)}.keizai.biz/headline/archives/1/",
        },
    ]
    for rule in SITE_URL_RULES:
        m = rule["pattern"].match(raw)
        if m:
            return jsonify({"suggested": rule["suggest"](m)})

    return jsonify({"suggested": None})


@app.route("/set_notify_timing", methods=["POST"])
@login_required
def set_notify_timing():
    user_id = session["user_id"]
    selected = request.form.getlist("notify_timing")
    if not selected:
        selected = ["immediate"]
    if "immediate" in selected:
        timing = "immediate"
    else:
        timing = ",".join(selected)
    if db.set_user_notify_timing(user_id, timing):
        flash("通知タイミングを変更しました", "success")
    else:
        flash("通知タイミングの更新に失敗しました", "error")
    return redirect(url_for("settings"))


@app.route("/change_password", methods=["POST"])
@login_required
def change_password():
    user_id  = session["user_id"]
    current  = request.form.get("current_password", "")
    new_pass = request.form.get("new_password", "")
    confirm  = request.form.get("confirm_password", "")

    if new_pass != confirm:
        flash("新しいパスワードが一致しません", "error")
        return redirect(url_for("settings"))
    if len(new_pass) < 6:
        flash("パスワードは6文字以上で入力してください", "error")
        return redirect(url_for("settings"))

    user = db.get_user_by_id(user_id)
    if not user or not db.verify_user_password(user, current):
        flash("現在のパスワードが正しくありません", "error")
        return redirect(url_for("settings"))

    db.update_user_password(user_id, new_pass)
    flash("パスワードを変更しました", "success")
    return redirect(url_for("settings"))


@app.route("/settings/change_email", methods=["GET", "POST"])
@login_required
def change_email():
    user_id = session["user_id"]
    user = db.get_user_by_id(user_id)
    current_email = user.get("email", "") if user else ""

    if request.method == "POST":
        new_email = (request.form.get("new_email") or "").strip().lower()
        password  = request.form.get("password", "")

        if not new_email or "@" not in new_email:
            flash("有効なメールアドレスを入力してください", "error")
            return render_template("change_email.html", current_email=current_email,
                                   user_email=session.get("email", ""),
                                   is_admin=session.get("is_admin", False))

        if new_email == current_email.lower():
            flash("現在のメールアドレスと同じです", "error")
            return render_template("change_email.html", current_email=current_email,
                                   user_email=session.get("email", ""),
                                   is_admin=session.get("is_admin", False))

        existing = db.get_user_by_email(new_email)
        if existing and existing.get("id") != user_id:
            flash("このメールアドレスは既に使用されています", "error")
            return render_template("change_email.html", current_email=current_email,
                                   user_email=session.get("email", ""),
                                   is_admin=session.get("is_admin", False))

        if not user or not db.verify_user_password(user, password):
            flash("パスワードが正しくありません", "error")
            return render_template("change_email.html", current_email=current_email,
                                   user_email=session.get("email", ""),
                                   is_admin=session.get("is_admin", False))

        try:
            db.update_user_email(user_id, new_email)
        except Exception as e:
            logger.error("[change_email] update failed: %s", e)
            flash("メールアドレスの変更に失敗しました", "error")
            return render_template("change_email.html", current_email=current_email,
                                   user_email=session.get("email", ""),
                                   is_admin=session.get("is_admin", False))

        session["email"] = new_email
        flash("メールアドレスを変更しました", "success")
        return redirect(url_for("settings"))

    return render_template("change_email.html", current_email=current_email,
                           user_email=session.get("email", ""),
                           is_admin=session.get("is_admin", False))


@app.route("/set_interval", methods=["POST"])
@admin_required
def set_interval():
    try:
        seconds = int(request.form.get("seconds", 3600))
    except ValueError:
        flash("無効な値です", "error")
        return redirect(url_for("index"))

    config = db.load_config()
    config["check_interval_seconds"] = seconds
    db.save_config(config)
    flash(f"チェック間隔を {seconds // 60} 分に変更しました", "success")
    return redirect(url_for("settings"))


@app.route("/news")
@login_required
def news():
    user_id = session["user_id"]
    kw_entries = db.load_keywords(user_id)
    articles_data = db.load_articles_data(user_id)
    raw_articles = articles_data.get("articles", [])
    # 各記事にアラートフラグ付与（per-company 判定） → 重複排除 → 未読アラート件数算出
    _flag_articles_alert(user_id, raw_articles)
    for a in raw_articles:
        a["published"] = a.get("published", "")
    deduped_articles = _deduplicate_articles(raw_articles)
    alert_count = sum(1 for a in deduped_articles if a.get("is_alert") and not a.get("is_read"))
    all_articles = deduped_articles

    return render_template(
        "news.html",
        articles=all_articles,
        keyword_entries=kw_entries,
        user_email=session.get("email", ""),
        is_admin=session.get("is_admin", False),
        alert_count=alert_count,
    )


@app.route("/tdnet")
@login_required
def tdnet_page():
    user_id = session["user_id"]
    user = db.get_user_by_id(user_id) or {}
    is_pro = (user.get("plan") == "pro")
    if not is_pro:
        flash("TDnet開示情報はProプラン限定です", "error")
        return redirect(url_for("news"))
    tdnet_items = db.get_tdnet_for_user(user_id)
    return render_template(
        "tdnet.html",
        tdnet_items=tdnet_items,
        user_email=session.get("email", ""),
        is_admin=session.get("is_admin", False),
    )


@app.route("/settings")
@login_required
def settings():
    user_id = session["user_id"]
    config = db.load_config()
    raw_timing = db.get_user_notify_timing(user_id)
    return render_template("settings.html",
                           check_interval=config.get("check_interval_seconds", 3600),
                           notify_timing=raw_timing,
                           notify_timing_list=raw_timing.split(","),
                           user_email=session.get("email", ""),
                           is_admin=session.get("is_admin", False),
                           dashboard_settings=db.get_dashboard_settings(user_id))


@app.route("/settings/dashboard", methods=["POST"])
@login_required
def save_dashboard_settings():
    user_id = session["user_id"]
    try:
        card_count = int(request.form.get("card_count", 4))
    except (TypeError, ValueError):
        card_count = 4
    card_order   = request.form.getlist("card_order")
    card_visible = request.form.getlist("card_visible")
    if card_count not in (2, 4, 6):
        card_count = 4
    db.save_dashboard_settings(user_id, {
        "card_count":   card_count,
        "card_order":   card_order,
        "card_visible": card_visible,
    })
    flash("ダッシュボード設定を保存しました", "success")
    return redirect(url_for("settings"))


@app.route("/admin")
@admin_required
def admin():
    users = db.get_all_users()
    return render_template("admin.html", users=users,
                           user_email=session.get("email", ""))


@app.route("/admin/domain-overrides")
@admin_required
def admin_domain_overrides():
    overrides = db.get_all_domain_overrides()
    return render_template("admin_domain_overrides.html",
                           overrides=overrides,
                           user_email=session.get("email", ""),
                           is_admin=True)


@app.route("/admin/domain-overrides/add", methods=["POST"])
@admin_required
def admin_add_domain_override():
    domain = request.form.get("domain", "").strip().lower()
    suggested_url = request.form.get("suggested_url", "").strip()
    company_name = request.form.get("company_name", "").strip()
    company_name_kana = request.form.get("company_name_kana", "").strip()
    if domain and suggested_url:
        db.add_domain_override(domain, suggested_url, company_name, company_name_kana)
        label = company_name or domain
        flash(f"{label}を追加しました", "success")
    else:
        flash("ドメインと推奨URLを入力してください", "error")
    return redirect(url_for("admin_domain_overrides"))


@app.route("/admin/domain-overrides/edit/<int:override_id>", methods=["POST"])
@admin_required
def admin_edit_domain_override(override_id):
    domain = request.form.get("domain", "").strip().lower()
    suggested_url = request.form.get("suggested_url", "").strip()
    company_name = request.form.get("company_name", "").strip()
    company_name_kana = request.form.get("company_name_kana", "").strip()
    if domain and suggested_url:
        db.update_domain_override(override_id, domain, suggested_url, company_name, company_name_kana)
        flash("更新しました", "success")
    else:
        flash("ドメインと推奨URLは必須です", "error")
    return redirect(url_for("admin_domain_overrides"))


@app.route("/admin/domain-overrides/csv-upload", methods=["POST"])
@admin_required
def admin_csv_upload_domain_overrides():
    """CSVファイルからドメインオーバーライドを一括登録する"""
    import csv
    import io

    file = request.files.get("csv_file")
    if not file or not file.filename:
        flash("ファイルを選択してください", "error")
        return redirect(url_for("admin_domain_overrides"))

    # ファイル読み込み（UTF-8 → Shift-JIS フォールバック）
    raw = file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = raw.decode("shift_jis")
        except UnicodeDecodeError:
            flash("ファイルの文字コードを読み取れません（UTF-8またはShift-JISに対応）", "error")
            return redirect(url_for("admin_domain_overrides"))

    # 既存ドメインを取得（重複チェック用）
    existing = db.get_domain_overrides_dict()

    reader = csv.reader(io.StringIO(text))
    registered = 0
    skip_empty = 0
    skip_domain_fmt = 0
    skip_url_fmt = 0
    skip_dup = 0

    for i, row in enumerate(reader):
        # ヘッダー行をスキップ（先頭行がドメインっぽくない場合）
        if i == 0 and len(row) >= 3:
            header_check = row[2].strip().lower()
            if header_check in ("ドメイン", "domain", ""):
                continue

        if len(row) < 4:
            # 3列の場合は企業名なしとして扱う
            if len(row) == 3:
                row = ["", ""] + row[0:1] + row[1:2]
            elif len(row) == 2:
                row = ["", ""] + row
            else:
                skip_empty += 1
                continue

        company_name = row[0].strip()
        company_name_kana = row[1].strip()
        domain = row[2].strip().lower()
        suggested_url = row[3].strip()

        # 企業名・ドメインが空
        if not domain:
            skip_empty += 1
            continue

        # ドメイン形式チェック
        if "://" in domain or "/" in domain:
            skip_domain_fmt += 1
            continue

        # 推奨URLチェック
        if not suggested_url.startswith("https://"):
            skip_url_fmt += 1
            continue

        # 重複チェック
        if domain in existing:
            skip_dup += 1
            continue

        db.add_domain_override(domain, suggested_url, company_name, company_name_kana)
        existing[domain] = suggested_url
        registered += 1

    # 結果メッセージ
    skipped = skip_empty + skip_domain_fmt + skip_url_fmt + skip_dup
    details = []
    if skip_empty:
        details.append(f"空行: {skip_empty}")
    if skip_domain_fmt:
        details.append(f"ドメイン形式エラー: {skip_domain_fmt}")
    if skip_url_fmt:
        details.append(f"URL形式エラー: {skip_url_fmt}")
    if skip_dup:
        details.append(f"重複: {skip_dup}")
    msg = f"{registered} 件登録"
    if skipped:
        msg += f"、{skipped} 件スキップ（{', '.join(details)}）"
    flash(msg, "success" if registered > 0 else "warning")
    return redirect(url_for("admin_domain_overrides"))


@app.route("/admin/domain-overrides/delete/<int:override_id>", methods=["POST"])
@admin_required
def admin_delete_domain_override(override_id):
    db.delete_domain_override(override_id)
    flash("削除しました", "success")
    return redirect(url_for("admin_domain_overrides"))


@app.route("/admin/fetch_securities_master", methods=["POST"])
@admin_required
def admin_fetch_securities_master():
    try:
        n = db.fetch_and_save_securities_master()
        return jsonify({"saved": n})
    except Exception as e:
        logger.error("[admin_fetch_securities_master] %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/terms")
def terms():
    back_url = url_for("index") if session.get("user_id") else url_for("login")
    return render_template("terms.html", back_url=back_url)


@app.route("/privacy")
def privacy():
    back_url = url_for("index") if session.get("user_id") else url_for("login")
    return render_template("privacy.html", back_url=back_url)


# ============================================================
# PWA
# ============================================================

@app.route("/manifest.json")
def pwa_manifest():
    return send_from_directory("static", "manifest.json",
                               mimetype="application/manifest+json")


@app.route("/sw.js")
def pwa_sw():
    response = send_from_directory("static", "sw.js",
                                   mimetype="application/javascript")
    response.headers["Service-Worker-Allowed"] = "/"
    return response


@app.route("/offline")
def pwa_offline():
    return render_template("offline.html")


# ============================================================
# パスワードリセット
# ============================================================

def _send_reset_email(to_email: str, reset_url: str):
    """パスワードリセットURLをメールで送信する"""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import html as _html

    smtp_server   = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port     = int(os.environ.get("SMTP_PORT", "587"))
    sender_email  = os.environ.get("SENDER_EMAIL", "")
    sender_pass   = os.environ.get("SENDER_PASSWORD", "")
    if not sender_email or not sender_pass:
        logger.error("メール送信設定が不足しています (SENDER_EMAIL / SENDER_PASSWORD)")
        return

    url_esc = _html.escape(reset_url)
    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:560px;margin:0 auto;padding:16px">
<h2 style="font-size:1.1em">BizRadar パスワードリセット</h2>
<p>以下のリンクから新しいパスワードを設定してください。<br>
このリンクは<strong>1時間</strong>で無効になります。</p>
<p style="margin:20px 0">
  <a href="{url_esc}" style="background:#1a1a2e;color:#fff;padding:10px 20px;
     border-radius:8px;text-decoration:none;font-weight:600">
    パスワードをリセットする
  </a>
</p>
<p style="font-size:0.85em;color:#6b7280">
  このメールに心当たりがない場合は無視してください。<br>
  リンク: {url_esc}
</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin-top:24px">
<p style="color:#9ca3af;font-size:0.78em">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""

    msg = MIMEMultipart()
    from email.utils import formataddr as _formataddr
    msg["From"]    = _formataddr(("BizRadar", sender_email))
    msg["To"]      = to_email
    msg["Subject"] = "【BizRadar】パスワードリセットのご案内"
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_pass)
            server.send_message(msg)
        logger.info("パスワードリセットメールを送信しました → %s", to_email)
    except smtplib.SMTPException as e:
        logger.error("パスワードリセットメール送信に失敗しました: %s", e)


def _send_magic_login_email(to_email: str, login_url: str, token: str = ""):
    """マジックリンクログインURLをメールで送信する"""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import html as _html

    smtp_server   = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port     = int(os.environ.get("SMTP_PORT", "587"))
    sender_email  = os.environ.get("SENDER_EMAIL", "")
    sender_pass   = os.environ.get("SENDER_PASSWORD", "")
    if not sender_email or not sender_pass:
        logger.error("メール送信設定が不足しています (SENDER_EMAIL / SENDER_PASSWORD)")
        return

    url_esc = _html.escape(login_url)
    html_body = f"""<!DOCTYPE html>
<html lang="ja"><body style="font-family:sans-serif;color:#111;max-width:560px;margin:0 auto;padding:16px">
<h2 style="font-size:1.1em">BizRadar ログイン用リンク</h2>
<p>ログイン用URLをお送りします。<br>
以下のリンクをクリックしてログインしてください。</p>
<p style="margin:20px 0">
  <a href="{url_esc}" style="background:#1a1a2e;color:#fff;padding:10px 20px;
     border-radius:8px;text-decoration:none;font-weight:600">
    ログインする
  </a>
</p>
<p style="font-size:0.85em;color:#6b7280">
  このリンクは<strong>15分間</strong>有効です。<br>
  身に覚えのない場合はこのメールを無視してください。<br>
  リンク: {url_esc}
</p>
<hr style="border:none;border-top:1px solid #e5e7eb;margin-top:24px">
<p style="color:#9ca3af;font-size:0.78em">このメールはBizRadarにより自動送信されました。</p>
</body></html>"""

    msg = MIMEMultipart()
    from email.utils import formataddr as _formataddr
    msg["From"]    = _formataddr(("BizRadar", sender_email))
    msg["To"]      = to_email
    msg["Subject"] = "BizRadar ログイン用リンク"
    msg["X-Mailer"] = "BizRadar"
    if token:
        msg["Message-ID"] = f"<{token[:8]}@bizradar>"
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_pass)
            server.send_message(msg)
        logger.info("マジックログインメールを送信しました → %s", to_email)
    except smtplib.SMTPException as e:
        logger.error("マジックログインメール送信に失敗しました: %s", e)


@app.route("/magic-login", methods=["GET", "POST"])
@limiter.limit("3 per hour", methods=["POST"])
def magic_login_request():
    """マジックリンク送信フォーム + 送信処理"""
    if request.method == "GET":
        return render_template("magic_login.html")

    email = request.form.get("email", "").strip().lower()
    # ユーザー列挙攻撃対策: 存在有無に関わらず同じメッセージを返す
    user = db.get_user_by_email(email) if email else None
    if user:
        try:
            token = db.create_magic_token(user["id"], ttl_minutes=15)
            login_url = url_for("magic_login_verify", token=token, _external=True)
            _send_magic_login_email(email, login_url, token)
        except Exception as e:
            logger.error("マジックリンク生成・送信に失敗しました: %s", e)

    flash("ログイン用URLをメールで送信しました。15分以内にご確認ください。", "info")
    return render_template("magic_login.html")


@app.route("/magic-login/<token>", methods=["GET"])
def magic_login_verify(token: str):
    """マジックリンクの検証とログイン処理"""
    user_id = db.consume_magic_token(token)
    if not user_id:
        flash("このリンクは無効または期限切れです。", "danger")
        return redirect(url_for("login"))
    user = db.get_user_by_id(user_id)
    if not user:
        flash("このリンクは無効または期限切れです。", "danger")
        return redirect(url_for("login"))

    # ログイン成功時のセッション状態を通常ログインと同じにする
    session.pop("login_fail_count", None)
    session.pop("login_fail_first_at", None)
    session.pop("login_locked_until", None)
    session.permanent = True
    session["user_id"] = user["id"]
    session["email"] = user["email"]
    session["is_admin"] = user["is_admin"]
    db.update_last_login(user["id"])
    return redirect(url_for("index"))


@app.route("/forgot-password", methods=["GET", "POST"])
@limiter.limit("3 per hour", methods=["POST"])
def forgot_password():
    if request.method == "GET":
        return render_template("forgot_password.html")

    email = request.form.get("email", "").strip().lower()
    # ユーザーが存在しなくても同じメッセージを返してメールアドレスの存在を漏らさない
    user = db.get_user_by_email(email)
    if user:
        token = db.create_reset_token(user["id"])
        reset_url = url_for("reset_password", token=token, _external=True)
        _send_reset_email(email, reset_url)
    return render_template("forgot_password.html", sent=True)


@app.route("/reset-password/<token>", methods=["GET", "POST"])
@limiter.limit("10 per hour", methods=["POST"])
def reset_password(token: str):
    user_id = db.get_reset_token_user_id(token)
    if user_id is None:
        return render_template("reset_password.html", invalid=True)

    if request.method == "GET":
        return render_template("reset_password.html", token=token)

    password  = request.form.get("password", "")
    password2 = request.form.get("password2", "")
    if len(password) < 8:
        return render_template("reset_password.html", token=token,
                               error="パスワードは8文字以上で入力してください")
    if password != password2:
        return render_template("reset_password.html", token=token,
                               error="パスワードが一致しません")

    db.update_user_password(user_id, password)
    db.invalidate_reset_token(token)
    return redirect(url_for("login") + "?reset=1")


@app.route("/api/checking_status")
@login_required
def api_checking_status():
    """チェック中・完了（猶予期間内）のURL・キーワードをステータス付きで返す軽量エンドポイント。
    site_statuses: {url: "running" | "completed"}
    keyword_statuses: {keyword: "running" | "completed"}
    """
    statuses = db.get_running_task_statuses()
    return jsonify({
        "site_statuses":    statuses.get("site_check", {}),
        "keyword_statuses": statuses.get("keyword_collect", {}),
    })


@app.route("/api/status")
@login_required
def api_status():
    user_id = session["user_id"]
    log   = db.load_monitor_log(user_id)
    sites = db.load_sites(user_id)

    site_data = []
    for s in sites:
        url = s["url"]
        check_info = log["last_checks"].get(url, {})
        site_data.append({
            "url":        url,
            "name":       s.get("name", ""),
            "last_check": check_info.get("timestamp", "未チェック"),
            "status":     check_info.get("status", "unknown"),
        })

    return jsonify({
        "sites":          site_data,
        "change_history": log.get("change_history", [])[:50],
        "generated_at":   _now_jst_str(),
    })


# ============================================================
# Companies
# ============================================================

@app.route("/company")
@login_required
def company_list():
    user_id = session["user_id"]
    # per-company アラート判定: user-wide + 各企業の company_alert_keywords を結合
    user_alert_kws = db.get_alert_keywords_set(user_id)
    per_cid_alert: dict = {}
    for e in db.get_all_company_alert_keywords_for_user(user_id):
        per_cid_alert.setdefault(e["company_id"], set()).add(e["keyword"].lower())
    companies = db.load_companies(user_id)
    for c in companies:
        effective = user_alert_kws | per_cid_alert.get(c["id"], set())
        summary = db.get_company_summary(user_id, c["id"], effective)
        c.update(summary)
    companies = sorted(companies, key=lambda c: (
        0 if c.get('alert_count', 0) > 0 else (1 if c.get('unread_count', 0) > 0 else 2),
        c.get('sort_order', 0)
    ))
    user = db.get_user_by_id(user_id) or {}
    is_pro = (user.get("plan") == "pro")
    return render_template("company_list.html",
                           companies=companies,
                           user_email=session.get("email", ""),
                           is_admin=session.get("is_admin", False),
                           is_pro=is_pro)


@app.route("/management")
@login_required
def management():
    return redirect(url_for("company"))


@app.route("/companies/reorder", methods=["POST"])
@login_required
def reorder_companies():
    user_id = session["user_id"]
    data = request.get_json(silent=True)
    if not data or "ids" not in data:
        return {"ok": False, "error": "invalid"}, 400
    ids = data["ids"]
    if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
        return {"ok": False, "error": "invalid ids"}, 400
    db.update_companies_order(user_id, ids)
    return {"ok": True}


@app.route("/companies/add", methods=["POST"])
@login_required
def add_company():
    user_id = session["user_id"]
    name = request.form.get("name", "").strip()
    if not name:
        flash("企業名を入力してください", "error")
        return redirect(url_for("management", _anchor="keywords-section"))
    name_kana       = request.form.get("name_kana", "").strip()
    website_url     = request.form.get("website_url", "").strip()
    memo            = request.form.get("memo", "").strip()
    securities_code = request.form.get("securities_code", "").strip()

    # 同一ドメインの既存企業があれば登録をブロック
    if website_url:
        new_domain = _extract_domain(website_url)
        if new_domain:
            existing = db.load_companies(user_id)
            for c in existing:
                c_url = c.get("website_url", "") or ""
                if c_url and _extract_domain(c_url) == new_domain:
                    flash(f"このドメインはすでに登録されています：{c['name']}（{c_url}）", "error")
                    return redirect(url_for("company_list"))

    company_id = db.create_company(user_id, name, name_kana, website_url, memo,
                                   securities_code=securities_code)
    if request.form.get("add_as_keyword"):
        created = db.add_keyword_if_not_exists(user_id, name)
        if created:
            db.set_keyword_company(user_id, name, company_id)
    flash(f"「{name}」を登録しました", "success")
    return redirect(url_for("company_list"))


@app.route("/companies/<int:company_id>")
@login_required
def company_detail(company_id):
    user_id = session["user_id"]
    try:
        company = db.get_company(user_id, company_id)
        if not company:
            flash("企業が見つかりません", "error")
            return redirect(url_for("management", _anchor="keywords-section"))

        sites_linked    = db.load_company_sites(user_id, company_id)
        keywords_linked = db.load_company_keywords(user_id, company_id)
        company_exclude_words = db.get_company_exclude_words(company_id)
        company_alert_words   = db.get_company_alert_keywords(company_id)
        articles        = db.load_company_articles(user_id, company_id, limit=30)
        history         = db.load_company_change_history(user_id, company_id, limit=10)

        # 記事に重要フラグ付与（この企業に紐づく記事なので user-wide + この企業のアラート）
        user_alert_entries = db.load_alert_keywords(user_id)
        alert_kw_map = {e["keyword"].lower(): e["keyword"] for e in user_alert_entries}
        for e in company_alert_words:
            alert_kw_map[e["keyword"].lower()] = e["keyword"]
        alert_kws = set(alert_kw_map.keys())
        for a in articles:
            title_lower = a.get("title", "").lower()
            matched = [alert_kw_map[kw] for kw in alert_kws if kw in title_lower]
            a["is_alert"] = bool(matched)
            a["alert_matches"] = matched
            a["published"] = a.get("published", "")

        # 重複記事除去
        articles = _deduplicate_articles(articles)

        # 重要記事と通常記事に分離（両グループとも公開日時の新しい順）
        alert_articles  = sorted(
            [a for a in articles if a.get("is_alert") and not a.get("is_read")],
            key=lambda x: x.get("published", ""), reverse=True,
        )
        normal_articles = sorted(
            [a for a in articles if not a.get("is_alert") and not a.get("is_read")],
            key=lambda x: x.get("published", ""), reverse=True,
        )

        summary = db.get_company_summary(user_id, company_id, alert_kws)

        # TDnet 開示情報（Pro プランかつ証券コード登録済みの企業のみ、最新10件）
        tdnet_disclosures = []
        _code = (company.get("securities_code") or "").strip()
        if _code:
            _user = db.get_user_by_id(user_id) or {}
            if _user.get("plan") == "pro":
                tdnet_disclosures = db.get_tdnet_by_securities_code(_code, limit=10)

        return render_template("company_detail.html",
                               company=company,
                               sites_linked=sites_linked,
                               keywords_linked=keywords_linked,
                               company_exclude_words=company_exclude_words,
                               company_alert_words=company_alert_words,
                               articles=articles,
                               alert_articles=alert_articles,
                               normal_articles=normal_articles,
                               history=history,
                               summary=summary,
                               tdnet_disclosures=tdnet_disclosures,
                               user_email=session.get("email", ""),
                               is_admin=session.get("is_admin", False))
    except Exception:
        logger.exception(
            "company_detail: DB エラー発生 company_id=%s user_id=%s",
            company_id, user_id
        )
        flash("ページの読み込み中にエラーが発生しました。しばらくしてから再度お試しください。", "error")
        return redirect(url_for("company_list"))


@app.route("/companies/<int:company_id>/edit", methods=["POST"])
@login_required
def edit_company(company_id):
    user_id = session["user_id"]
    name = request.form.get("name", "").strip()
    if not name:
        flash("企業名を入力してください", "error")
        return redirect(url_for("company_detail", company_id=company_id))
    name_kana       = request.form.get("name_kana", "").strip()
    website_url     = request.form.get("website_url", "").strip()
    memo            = request.form.get("memo", "").strip()
    securities_code = request.form.get("securities_code", "").strip()
    db.update_company(user_id, company_id, name, name_kana, website_url, memo,
                      securities_code=securities_code)
    flash("企業情報を更新しました", "success")
    return redirect(url_for("company_detail", company_id=company_id))


@app.route("/companies/<int:company_id>/delete", methods=["POST"])
@login_required
def delete_company(company_id):
    user_id = session["user_id"]
    company = db.get_company(user_id, company_id)
    if company and db.delete_company(user_id, company_id):
        flash(f"「{company['name']}」を削除しました", "success")
    return redirect(url_for("company_list"))


@app.route("/companies/<int:company_id>/link_site", methods=["POST"])
@login_required
def link_site_to_company(company_id):
    user_id  = session["user_id"]
    site_url = request.form.get("site_url", "")
    action   = request.form.get("action", "link")  # "link" or "unlink"
    target_id = company_id if action == "link" else None
    db.set_site_company(user_id, site_url, target_id)
    return redirect(url_for("company_detail", company_id=company_id))


@app.route("/companies/<int:company_id>/link_keyword", methods=["POST"])
@login_required
def link_keyword_to_company(company_id):
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "")
    action  = request.form.get("action", "link")
    if action == "unlink":
        # 「削除」ボタン: キーワードと関連記事を物理削除する（紐づけ解除ではない）
        db.delete_articles_by_keyword(user_id, keyword)
        db.delete_keyword_by_text(user_id, keyword)
    else:
        db.set_keyword_company(user_id, keyword, company_id)
    return redirect(url_for("company_detail", company_id=company_id))


@app.route("/companies/<int:company_id>/new_site", methods=["POST"])
@login_required
def new_site_for_company(company_id):
    user_id = session["user_id"]
    url  = request.form.get("url", "").strip()
    name = request.form.get("name", "").strip()
    if not url:
        flash("URLを入力してください", "error")
        return redirect(url_for("company_detail", company_id=company_id))
    db.create_site_and_link(user_id, url, name, company_id)
    flash(f"サイトを登録・紐づけしました", "success")
    return redirect(url_for("company_detail", company_id=company_id))


@app.route("/companies/<int:company_id>/new_keyword", methods=["POST"])
@login_required
def new_keyword_for_company(company_id):
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    if not keyword:
        flash("キーワードを入力してください", "error")
        return redirect(url_for("company_detail", company_id=company_id))
    db.create_keyword_and_link(user_id, keyword, company_id)
    flash(f"「{keyword}」を登録・紐づけしました", "success")
    return redirect(url_for("company_detail", company_id=company_id))


# Render 上ではモニターをバックグラウンドスレッドで起動
if os.environ.get("RENDER"):
    import monitor as _monitor
    _t = threading.Thread(target=_monitor.main, daemon=True)
    _t.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    # デバッグモードは環境変数 DEBUG=true/1/yes のときのみ有効。デフォルトは本番想定で無効。
    debug_mode = os.environ.get("DEBUG", "").strip().lower() in ("true", "1", "yes")
    print(f"ダッシュボード起動: http://localhost:{port} (debug={debug_mode})")
    app.run(debug=debug_mode, host="0.0.0.0", port=port, threaded=True)
