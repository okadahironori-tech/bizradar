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


def _extract_domain(url: str) -> str:
    """URL からドメイン（netloc）を返す。"""
    try:
        return _urlparse(url).netloc.lower()
    except Exception:
        return ""


def _deduplicate_articles(articles, threshold=0.80):
    """同一キーワード内でタイトル類似度が threshold 以上の記事を重複排除する。
    最も古い記事を残し、同日付の場合はソース名に Yahoo を含むものを優先する。"""
    from collections import defaultdict

    def _sim(a, b):
        return difflib.SequenceMatcher(None, a, b).ratio()

    def _sort_key(art):
        published = art.get("published", "") or ""
        # Yahoo含むソースを優先（0）、それ以外（1）
        is_yahoo = 0 if "yahoo" in (art.get("source", "") or "").lower() else 1
        return (published, is_yahoo)

    by_kw = defaultdict(list)
    for idx, art in enumerate(articles):
        by_kw[art.get("keyword", "")].append(idx)

    keep = set()
    for kw, indices in by_kw.items():
        group = sorted([(idx, articles[idx]) for idx in indices], key=lambda x: _sort_key(x[1]))
        removed = set()
        for i, (idx_a, art_a) in enumerate(group):
            if idx_a in removed:
                continue
            keep.add(idx_a)
            title_a = art_a.get("title", "") or ""
            for idx_b, art_b in group[i + 1:]:
                if idx_b in removed or idx_b in keep:
                    continue
                title_b = art_b.get("title", "") or ""
                if _sim(title_a, title_b) >= threshold:
                    removed.add(idx_b)

    return [art for idx, art in enumerate(articles) if idx in keep]

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")

# DB 初期化（テーブル作成 + マイグレーション）
# Render 新インスタンス起動直後の一時的な接続失敗に備えてリトライする。
# 全リトライ失敗時はプロセスを終了し、Render に再起動させる。
_INIT_MAX_RETRIES = 5
_INIT_RETRY_DELAY = 3  # 秒

for _attempt in range(1, _INIT_MAX_RETRIES + 1):
    try:
        db.init_db()
        print(f"[INFO] データベース初期化完了 (試行 {_attempt}/{_INIT_MAX_RETRIES})", file=sys.stderr)
        break
    except Exception as _e:
        print(f"[エラー] DB初期化失敗 (試行 {_attempt}/{_INIT_MAX_RETRIES}): {_e}", file=sys.stderr)
        if _attempt < _INIT_MAX_RETRIES:
            time.sleep(_INIT_RETRY_DELAY)
        else:
            print("[FATAL] データベース初期化に失敗しました。プロセスを終了します。", file=sys.stderr)
            sys.exit(1)


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
def login():
    error = None
    if request.method == "POST":
        email = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        user = db.get_user_by_email(email)
        if user and db.verify_user_password(user, password):
            session["user_id"] = user["id"]
            session["email"] = user["email"]
            session["is_admin"] = user["is_admin"]
            db.update_last_login(user["id"])
            next_url = request.form.get("next", "")
            return redirect(next_url if next_url.startswith("/") else url_for("index"))
        error = "メールアドレスまたはパスワードが正しくありません"
    return render_template("login.html", error=error, next=request.args.get("next", ""))


@app.route("/register", methods=["GET", "POST"])
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
    articles      = all_articles[:300]
    keyword_counts = {}
    for a in all_articles:
        kw = a.get("keyword", "")
        keyword_counts[kw] = keyword_counts.get(kw, 0) + 1

    alert_kw_entries = db.load_alert_keywords(user_id)
    alert_kws_set = {e["keyword"].lower() for e in alert_kw_entries}

    # 記事に重要フラグ付与・published をJSTに変換
    for a in articles:
        title_lower = a.get("title", "").lower()
        a["is_alert"] = any(kw in title_lower for kw in alert_kws_set)
        a["published"] = a.get("published", "")

    # ---- サマリー集計 ----
    unread_count       = sum(1 for a in articles if not a.get("is_read"))
    alert_count        = sum(1 for a in articles if a.get("is_alert") and not a.get("is_read"))
    error_site_count   = sum(1 for s in sites if s["status"] == "error")
    today_company_list = db.load_active_companies_today(user_id)
    today_companies    = len(today_company_list)

    # ---- 前回ログイン以降の更新企業（今日0:00より前が対象） ----
    from datetime import datetime, timezone, timedelta
    jst = timezone(timedelta(hours=9))
    today_jst_midnight = datetime.now(jst).replace(hour=0, minute=0, second=0, microsecond=0)
    last_login_at = db.get_user_last_login(user_id)
    if last_login_at and last_login_at < today_jst_midnight:
        prev_login_company_list = db.load_active_companies_since(user_id, last_login_at)
    else:
        prev_login_company_list = []
    prev_login_at = last_login_at

    return render_template(
        "index.html",
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
        alert_kw_entries=alert_kw_entries,
        summary_unread=unread_count,
        summary_alert=alert_count,
        summary_error_sites=error_site_count,
        summary_today_companies=today_companies,
        today_company_list=today_company_list,
        prev_login_company_list=prev_login_company_list,
        prev_login_at=prev_login_at,
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

    # 0. ドメイン固有の特例ルール（サイトマップ検索より優先）
    _DOMAIN_OVERRIDES = {
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
    }
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
    keywords = [k["keyword"] for k in kw_entries]
    articles_data = db.load_articles_data(user_id)
    all_articles = articles_data.get("articles", [])[:300]
    alert_kw_entries = db.load_alert_keywords(user_id)
    alert_kws_set = {e["keyword"].lower() for e in alert_kw_entries}
    for a in all_articles:
        a["is_alert"] = any(kw in a.get("title", "").lower() for kw in alert_kws_set)
        a["published"] = a.get("published", "")
    all_articles = _deduplicate_articles(all_articles)
    return render_template(
        "news.html",
        articles=all_articles,
        keywords=keywords,
        keyword_entries=kw_entries,
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
                           is_admin=session.get("is_admin", False))


@app.route("/admin")
@admin_required
def admin():
    users = db.get_all_users()
    return render_template("admin.html", users=users,
                           user_email=session.get("email", ""))


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


@app.route("/forgot-password", methods=["GET", "POST"])
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
    alert_kws = db.get_alert_keywords_set(user_id)
    companies = db.load_companies(user_id)
    for c in companies:
        summary = db.get_company_summary(user_id, c["id"], alert_kws)
        c.update(summary)
    return render_template("company_list.html",
                           companies=companies,
                           user_email=session.get("email", ""),
                           is_admin=session.get("is_admin", False))


@app.route("/management")
@login_required
def management():
    user_id = session["user_id"]
    alert_kws = db.get_alert_keywords_set(user_id)
    company_list = db.load_companies(user_id)
    for c in company_list:
        summary = db.get_company_summary(user_id, c["id"], alert_kws)
        c.update(summary)

    # 設定ページから統合: サイト一覧・キーワード・アカウント設定データ
    config = db.load_config()
    kw_entries = db.load_keywords(user_id)
    keywords = [k["keyword"] for k in kw_entries]
    running = db.get_running_task_statuses()
    collecting_kws = set(running.get("keyword_collect", {}).keys())
    articles_data = db.load_articles_data(user_id)
    keyword_counts = {}
    for a in articles_data.get("articles", []):
        kw = a.get("keyword", "")
        keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
    alert_kw_entries = db.load_alert_keywords(user_id)
    log = db.load_monitor_log(user_id)
    site_statuses = running.get("site_check", {})
    sites = []
    for s in db.load_sites(user_id):
        url = s["url"]
        check_info = log["last_checks"].get(url, {})
        error_text = check_info.get("error", "")
        status = check_info.get("status", "unknown")
        error_label, error_cls = _classify_site_error(error_text) if status == "error" else ("", "")
        sites.append({
            "id":          s.get("id"),
            "url":         url,
            "name":        s.get("name", ""),
            "enabled":     s.get("enabled", True),
            "last_check":  check_info.get("timestamp", "未チェック"),
            "status":      status,
            "error_label": error_label,
            "error_cls":   error_cls,
            "checking":    site_statuses.get(url) == "running",
        })

    return render_template("companies.html",
                           companies=company_list,
                           sites=sites,
                           check_interval=config.get("check_interval_seconds", 3600),
                           keywords=keywords,
                           keyword_entries=kw_entries,
                           keyword_counts=keyword_counts,
                           keyword_collecting=collecting_kws,
                           notify_timing=db.get_user_notify_timing(user_id),
                           alert_kw_entries=alert_kw_entries,
                           user_email=session.get("email", ""),
                           is_admin=session.get("is_admin", False))


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
    name_kana   = request.form.get("name_kana", "").strip()
    website_url = request.form.get("website_url", "").strip()
    memo        = request.form.get("memo", "").strip()

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

    company_id = db.create_company(user_id, name, name_kana, website_url, memo)
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
    company = db.get_company(user_id, company_id)
    if not company:
        flash("企業が見つかりません", "error")
        return redirect(url_for("management", _anchor="keywords-section"))

    alert_kws = db.get_alert_keywords_set(user_id)

    sites_linked    = db.load_company_sites(user_id, company_id)
    keywords_linked = db.load_company_keywords(user_id, company_id)
    articles        = db.load_company_articles(user_id, company_id, limit=30)
    history         = db.load_company_change_history(user_id, company_id, limit=10)

    # 記事に重要フラグ付与
    for a in articles:
        a["is_alert"] = any(kw in a.get("title", "").lower() for kw in alert_kws)
        a["published"] = a.get("published", "")

    # 全サイト・全キーワード（紐づけドロップダウン用）
    all_sites    = db.load_sites_with_company(user_id)
    all_keywords = db.load_keywords_with_company(user_id)

    summary = db.get_company_summary(user_id, company_id, alert_kws)

    return render_template("company_detail.html",
                           company=company,
                           sites_linked=sites_linked,
                           keywords_linked=keywords_linked,
                           articles=articles,
                           history=history,
                           all_sites=all_sites,
                           all_keywords=all_keywords,
                           summary=summary,
                           user_email=session.get("email", ""),
                           is_admin=session.get("is_admin", False))


@app.route("/companies/<int:company_id>/edit", methods=["POST"])
@login_required
def edit_company(company_id):
    user_id = session["user_id"]
    name = request.form.get("name", "").strip()
    if not name:
        flash("企業名を入力してください", "error")
        return redirect(url_for("company_detail", company_id=company_id))
    name_kana   = request.form.get("name_kana", "").strip()
    website_url = request.form.get("website_url", "").strip()
    memo        = request.form.get("memo", "").strip()
    db.update_company(user_id, company_id, name, name_kana, website_url, memo)
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
    target_id = company_id if action == "link" else None
    db.set_keyword_company(user_id, keyword, target_id)
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
    print(f"ダッシュボード起動: http://localhost:{port}")
    app.run(debug=True, host="0.0.0.0", port=port, threaded=True)
