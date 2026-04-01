"""
監視ダッシュボード
monitor.py の監視データをブラウザで確認できるWebアプリ
"""

import hmac
import json
import os
import threading
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, jsonify, request, redirect, url_for, session
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated

MONITOR_LOG_FILE = "monitor_log.json"
HASH_FILE = "previous_hashes.json"
SITES_FILE = "sites.json"
CONFIG_FILE = "config.json"

_check_running = set()  # 現在チェック中のURL


def load_sites() -> list:
    if os.path.exists(SITES_FILE):
        with open(SITES_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("sites", [])
    return []


def save_sites(sites: list):
    with open(SITES_FILE, "w", encoding="utf-8") as f:
        json.dump({"sites": sites}, f, ensure_ascii=False, indent=2)


def load_monitor_log() -> dict:
    if os.path.exists(MONITOR_LOG_FILE):
        with open(MONITOR_LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_checks": {}, "change_history": []}


def load_hashes() -> dict:
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"check_interval_seconds": 3600}


def save_config(config: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        expected_user = os.environ.get("DASHBOARD_USER", "admin")
        expected_pass = os.environ.get("DASHBOARD_PASSWORD", "")
        input_user = request.form.get("username", "")
        input_pass = request.form.get("password", "")
        user_ok = hmac.compare_digest(input_user, expected_user)
        pass_ok = hmac.compare_digest(input_pass, expected_pass)
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
    log = load_monitor_log()
    hashes = load_hashes()
    config = load_config()
    site_list = load_sites()

    sites = []
    for s in site_list:
        url = s["url"]
        check_info = log["last_checks"].get(url, {})
        sites.append({
            "url": url,
            "name": s.get("name", ""),
            "last_check": check_info.get("timestamp", "未チェック"),
            "status": check_info.get("status", "unknown"),
            "hash": hashes.get(url, "-"),
            "checking": url in _check_running,
        })

    change_history = log.get("change_history", [])[:50]
    flash_msg = request.args.get("msg")
    flash_type = request.args.get("msg_type", "info")
    interval = config.get("check_interval_seconds", 3600)

    return render_template(
        "index.html",
        sites=sites,
        change_history=change_history,
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        flash_msg=flash_msg,
        flash_type=flash_type,
        check_interval=interval,
    )


@app.route("/add_site", methods=["POST"])
@login_required
def add_site():
    url = request.form.get("url", "").strip()
    name = request.form.get("name", "").strip()

    if not url:
        return redirect(url_for("index", msg="URLを入力してください", msg_type="error"))

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    sites = load_sites()
    if any(s["url"] == url for s in sites):
        return redirect(url_for("index", msg=f"すでに登録済みです: {url}", msg_type="error"))

    sites.append({"url": url, "name": name})
    save_sites(sites)
    label = name if name else url
    return redirect(url_for("index", msg=f"追加しました: {label}", msg_type="success"))


@app.route("/remove_site", methods=["POST"])
@login_required
def remove_site():
    url = request.form.get("url", "").strip()
    sites = load_sites()
    new_sites = [s for s in sites if s["url"] != url]
    if len(new_sites) == len(sites):
        return redirect(url_for("index", msg="該当URLが見つかりません", msg_type="error"))
    save_sites(new_sites)
    return redirect(url_for("index", msg=f"削除しました: {url}", msg_type="success"))


@app.route("/check_site", methods=["POST"])
@login_required
def check_site():
    url = request.form.get("url", "").strip()
    if url in _check_running:
        return redirect(url_for("index", msg=f"チェック実行中です: {url}", msg_type="error"))

    sites = load_sites()
    site_name = next((s.get("name", "") for s in sites if s["url"] == url), "")

    import monitor as monitor_module

    def run():
        _check_running.add(url)
        try:
            monitor_module.check_single_site(url, site_name)
        finally:
            _check_running.discard(url)

    threading.Thread(target=run, daemon=True).start()
    label = site_name if site_name else url
    return redirect(url_for("index", msg=f"チェックを開始しました: {label}", msg_type="success"))


@app.route("/set_interval", methods=["POST"])
@login_required
def set_interval():
    try:
        seconds = int(request.form.get("seconds", 3600))
    except ValueError:
        return redirect(url_for("index", msg="無効な値です", msg_type="error"))

    config = load_config()
    config["check_interval_seconds"] = seconds
    save_config(config)
    minutes = seconds // 60
    return redirect(url_for("index", msg=f"チェック間隔を {minutes} 分に変更しました", msg_type="success"))


@app.route("/api/status")
@login_required
def api_status():
    log = load_monitor_log()
    sites = load_sites()

    site_data = []
    for s in sites:
        url = s["url"]
        check_info = log["last_checks"].get(url, {})
        site_data.append({
            "url": url,
            "name": s.get("name", ""),
            "last_check": check_info.get("timestamp", "未チェック"),
            "status": check_info.get("status", "unknown"),
        })

    return jsonify({
        "sites": site_data,
        "change_history": log.get("change_history", [])[:50],
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
