"""
監視ダッシュボード
monitor.py の監視データをブラウザで確認できるWebアプリ
"""

import hashlib
import hmac
import os
import secrets
import sys
import threading
from datetime import datetime
from functools import wraps
from flask import Flask, flash, render_template, jsonify, request, redirect, url_for, session
from dotenv import load_dotenv

import db

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")

# DB 初期化（テーブル作成 + JSON ファイルからの移行）
try:
    db.init_db()
except Exception as _e:
    print(f"[エラー] データベース初期化失敗: {_e}", file=sys.stderr)

_check_running = set()      # 現在チェック中のURL
_keyword_collecting = set() # 現在収集中のキーワード


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def get_credentials() -> dict:
    """認証情報を返す。DB の config が env var より優先される。"""
    config = db.load_config()
    auth = config.get("auth", {})
    if isinstance(auth, dict) and auth.get("password_hash") and auth.get("salt"):
        return {
            "username":      auth.get("username", os.environ.get("DASHBOARD_USER", "admin")),
            "password_hash": auth["password_hash"],
            "salt":          auth["salt"],
            "use_hash":      True,
        }
    return {
        "username": os.environ.get("DASHBOARD_USER", "admin"),
        "password": os.environ.get("DASHBOARD_PASSWORD", ""),
        "use_hash": False,
    }


def verify_password(input_pass: str) -> bool:
    creds = get_credentials()
    if creds["use_hash"]:
        input_hash = _hash_password(input_pass, creds["salt"])
        return hmac.compare_digest(input_hash, creds["password_hash"])
    return hmac.compare_digest(input_pass, creds.get("password", ""))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        creds = get_credentials()
        input_user = request.form.get("username", "")
        input_pass = request.form.get("password", "")
        user_ok = hmac.compare_digest(input_user, creds["username"])
        pass_ok = verify_password(input_pass)
        if user_ok and pass_ok:
            session["logged_in"] = True
            next_url = request.form.get("next", "")
            return redirect(next_url if next_url.startswith("/") else url_for("index"))
        error = "IDまたはパスワードが正しくありません"
    return render_template("login.html", error=error, next=request.args.get("next", ""))


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    log       = db.load_monitor_log()
    hashes    = db.load_hashes()
    config    = db.load_config()
    site_list = db.load_sites()

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
            "checking":   url in _check_running,
        })

    change_history = log.get("change_history", [])[:50]
    interval = config.get("check_interval_seconds", 3600)

    kw_entries = db.load_keywords()
    keywords   = [k["keyword"] for k in kw_entries]
    collecting = set(_keyword_collecting)

    articles_data = db.load_articles_data()
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
        articles=articles,
        keyword_counts=keyword_counts,
        keyword_collecting=collecting,
    )


@app.route("/add_site", methods=["POST"])
@login_required
def add_site():
    url  = request.form.get("url", "").strip()
    name = request.form.get("name", "").strip()

    if not url:
        flash("URLを入力してください", "error")
        return redirect(url_for("index"))

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    sites = db.load_sites()
    if any(s["url"] == url for s in sites):
        flash(f"すでに登録済みです: {url}", "error")
        return redirect(url_for("index"))

    sites.append({"url": url, "name": name})
    db.save_sites(sites)
    flash(f"追加しました: {name if name else url}", "success")
    return redirect(url_for("index"))


@app.route("/remove_site", methods=["POST"])
@login_required
def remove_site():
    url = request.form.get("url", "").strip()
    sites = db.load_sites()
    new_sites = [s for s in sites if s["url"] != url]
    if len(new_sites) == len(sites):
        flash("該当URLが見つかりません", "error")
        return redirect(url_for("index"))
    db.save_sites(new_sites)
    flash(f"削除しました: {url}", "success")
    return redirect(url_for("index"))


@app.route("/check_site", methods=["POST"])
@login_required
def check_site():
    url = request.form.get("url", "").strip()
    if url in _check_running:
        flash(f"チェック実行中です: {url}", "error")
        return redirect(url_for("index"))

    sites = db.load_sites()
    site_name = next((s.get("name", "") for s in sites if s["url"] == url), "")

    import monitor as monitor_module

    _check_running.add(url)

    def run():
        try:
            monitor_module.check_single_site(url, site_name)
        finally:
            _check_running.discard(url)

    threading.Thread(target=run, daemon=True).start()
    flash(f"チェックを開始しました: {site_name if site_name else url}", "success")
    return redirect(url_for("index"))


@app.route("/collect_keyword", methods=["POST"])
@login_required
def collect_keyword():
    keyword = request.form.get("keyword", "").strip()
    if keyword in _keyword_collecting:
        flash(f"収集中です: {keyword}", "error")
        return redirect(url_for("index"))

    import monitor as monitor_module

    _keyword_collecting.add(keyword)

    def run():
        try:
            monitor_module.check_single_keyword(keyword)
        finally:
            _keyword_collecting.discard(keyword)

    threading.Thread(target=run, daemon=True).start()
    flash(f"収集を開始しました: {keyword}", "success")
    return redirect(url_for("index"))


@app.route("/add_keyword", methods=["POST"])
@login_required
def add_keyword():
    keyword = request.form.get("keyword", "").strip()
    if not keyword:
        flash("キーワードを入力してください", "error")
        return redirect(url_for("index"))
    keywords = db.load_keywords()
    existing = [k["keyword"] for k in keywords]
    if keyword in existing:
        flash(f"すでに登録済みです: {keyword}", "error")
        return redirect(url_for("index"))
    keywords.append({"keyword": keyword})
    db.save_keywords(keywords)
    flash(f"キーワードを追加しました: {keyword}", "success")
    return redirect(url_for("index"))


@app.route("/remove_keyword", methods=["POST"])
@login_required
def remove_keyword():
    keyword = request.form.get("keyword", "").strip()
    keywords = db.load_keywords()
    new_keywords = [k for k in keywords if k["keyword"] != keyword]
    if len(new_keywords) == len(keywords):
        flash("該当キーワードが見つかりません", "error")
        return redirect(url_for("index"))
    db.save_keywords(new_keywords)
    flash(f"キーワードを削除しました: {keyword}", "success")
    return redirect(url_for("index"))


@app.route("/change_password", methods=["POST"])
@login_required
def change_password():
    current  = request.form.get("current_password", "")
    new_pass = request.form.get("new_password", "")
    confirm  = request.form.get("confirm_password", "")

    if new_pass != confirm:
        flash("新しいパスワードが一致しません", "error")
        return redirect(url_for("index"))
    if len(new_pass) < 6:
        flash("パスワードは6文字以上で入力してください", "error")
        return redirect(url_for("index"))
    if not verify_password(current):
        flash("現在のパスワードが正しくありません", "error")
        return redirect(url_for("index"))

    salt     = secrets.token_hex(16)
    new_hash = _hash_password(new_pass, salt)
    config   = db.load_config()
    config["auth"] = {
        "username":      get_credentials()["username"],
        "password_hash": new_hash,
        "salt":          salt,
    }
    db.save_config(config)
    flash("パスワードを変更しました", "success")
    return redirect(url_for("index"))


@app.route("/set_interval", methods=["POST"])
@login_required
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
    return redirect(url_for("index"))


@app.route("/terms")
def terms():
    back_url = url_for("index") if session.get("logged_in") else url_for("login")
    return render_template("terms.html", back_url=back_url)


@app.route("/privacy")
def privacy():
    back_url = url_for("index") if session.get("logged_in") else url_for("login")
    return render_template("privacy.html", back_url=back_url)


@app.route("/api/status")
@login_required
def api_status():
    log   = db.load_monitor_log()
    sites = db.load_sites()

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
