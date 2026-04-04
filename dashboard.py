"""
監視ダッシュボード
monitor.py の監視データをブラウザで確認できるWebアプリ（マルチユーザー対応）
"""

import logging
import os
import sys
import threading
import time
from datetime import datetime
from functools import wraps
from flask import Flask, flash, render_template, jsonify, request, redirect, url_for, session
from dotenv import load_dotenv

import db

load_dotenv()

logger = logging.getLogger(__name__)

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
    site_list = db.load_sites(user_id)
    running    = db.get_running_task_statuses()
    site_statuses  = running.get("site_check", {})
    collecting_kws = set(running.get("keyword_collect", {}).keys())

    sites = []
    for s in site_list:
        url = s["url"]
        check_info = log["last_checks"].get(url, {})
        sites.append({
            "url":        url,
            "name":       s.get("name", ""),
            "last_check": check_info.get("timestamp", "未チェック"),
            "status":     check_info.get("status", "unknown"),
            "error":      check_info.get("error", ""),
            "hash":       hashes.get(url, "-"),
            "checking":   site_statuses.get(url) == "running",
        })

    change_history = log.get("change_history", [])[:50]
    interval = config.get("check_interval_seconds", 3600)

    kw_entries = db.load_keywords(user_id)
    keywords   = [k["keyword"] for k in kw_entries]
    collecting = collecting_kws

    articles_data = db.load_articles_data(user_id)
    all_articles  = articles_data.get("articles", [])
    articles      = all_articles[:300]
    keyword_counts = {}
    for a in all_articles:
        kw = a.get("keyword", "")
        keyword_counts[kw] = keyword_counts.get(kw, 0) + 1

    return render_template(
        "index.html",
        sites=sites,
        change_history=change_history,
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        check_interval=interval,
        keywords=keywords,
        keyword_entries=kw_entries,
        articles=articles,
        keyword_counts=keyword_counts,
        keyword_collecting=collecting,
        user_email=session.get("email", ""),
        is_admin=session.get("is_admin", False),
    )


@app.route("/add_site", methods=["POST"])
@login_required
def add_site():
    user_id = session["user_id"]
    url  = request.form.get("url", "").strip()
    name = request.form.get("name", "").strip()

    if not url:
        flash("URLを入力してください", "error")
        return redirect(url_for("index"))

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    sites = db.load_sites(user_id)
    if any(s["url"] == url for s in sites):
        flash(f"すでに登録済みです: {url}", "error")
        return redirect(url_for("index"))

    sites.append({"url": url, "name": name})
    db.save_sites(sites, user_id)
    flash(f"追加しました: {name if name else url}", "success")
    return redirect(url_for("index"))


@app.route("/remove_site", methods=["POST"])
@login_required
def remove_site():
    user_id = session["user_id"]
    url = request.form.get("url", "").strip()
    sites = db.load_sites(user_id)
    new_sites = [s for s in sites if s["url"] != url]
    if len(new_sites) == len(sites):
        flash("該当URLが見つかりません", "error")
        return redirect(url_for("index"))
    db.save_sites(new_sites, user_id)
    flash(f"削除しました: {url}", "success")
    return redirect(url_for("index"))


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
        return redirect(url_for("index"))

    # そのユーザーが登録しているURLか確認（URL固定の保証）
    if not any(s["url"] == url for s in db.load_sites(user_id)):
        flash("該当URLが見つかりません", "error")
        return redirect(url_for("index"))

    ok = db.update_site_name(user_id=user_id, url=url, name=name)
    if not ok:
        flash("会社名の更新に失敗しました", "error")
        return redirect(url_for("index"))

    flash(f"会社名を更新しました: {name if name else url}", "success")
    return redirect(url_for("index"))


@app.route("/check_site", methods=["POST"])
@login_required
def check_site():
    url = request.form.get("url", "").strip()
    if url in db.get_all_running_tasks().get("site_check", set()):
        flash(f"チェック実行中です: {url}", "error")
        return redirect(url_for("index"))

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
    return redirect(url_for("index"))


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
        return redirect(url_for("index"))
    keywords = db.load_keywords(user_id)
    existing = [k["keyword"] for k in keywords]
    if keyword in existing:
        flash(f"すでに登録済みです: {keyword}", "error")
        return redirect(url_for("index"))
    keywords.append({"keyword": keyword, "notify_enabled": True})
    db.save_keywords(keywords, user_id)
    flash(f"キーワードを追加しました: {keyword}", "success")
    return redirect(url_for("index"))


@app.route("/remove_keyword", methods=["POST"])
@login_required
def remove_keyword():
    user_id = session["user_id"]
    keyword = request.form.get("keyword", "").strip()
    keywords = db.load_keywords(user_id)
    new_keywords = [k for k in keywords if k["keyword"] != keyword]
    if len(new_keywords) == len(keywords):
        flash("該当キーワードが見つかりません", "error")
        return redirect(url_for("index", _anchor="keywords-section"))
    db.save_keywords(new_keywords, user_id)
    flash(f"キーワードを削除しました: {keyword}", "success")
    return redirect(url_for("index", _anchor="keywords-section"))


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
    return redirect(url_for("index", _anchor="articles-section"))


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
    return redirect(url_for("index", _anchor="articles-section"))


@app.route("/change_password", methods=["POST"])
@login_required
def change_password():
    user_id  = session["user_id"]
    current  = request.form.get("current_password", "")
    new_pass = request.form.get("new_password", "")
    confirm  = request.form.get("confirm_password", "")

    if new_pass != confirm:
        flash("新しいパスワードが一致しません", "error")
        return redirect(url_for("index", _anchor="settings-section"))
    if len(new_pass) < 6:
        flash("パスワードは6文字以上で入力してください", "error")
        return redirect(url_for("index", _anchor="settings-section"))

    user = db.get_user_by_id(user_id)
    if not user or not db.verify_user_password(user, current):
        flash("現在のパスワードが正しくありません", "error")
        return redirect(url_for("index", _anchor="settings-section"))

    db.update_user_password(user_id, new_pass)
    flash("パスワードを変更しました", "success")
    return redirect(url_for("index", _anchor="settings-section"))


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
    return redirect(url_for("index", _anchor="settings-section"))


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
        "generated_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


# Render 上ではモニターをバックグラウンドスレッドで起動
if os.environ.get("RENDER"):
    import monitor as _monitor
    _t = threading.Thread(target=_monitor.main, daemon=True)
    _t.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    print(f"ダッシュボード起動: http://localhost:{port}")
    app.run(debug=True, host="0.0.0.0", port=port, threaded=True)
