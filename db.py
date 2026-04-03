"""
データベース操作モジュール (PostgreSQL)
全ての load_* / save_* 関数はここで定義し、dashboard.py と monitor.py から import して使う
"""
import json
import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager


def _get_dsn() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL 環境変数が設定されていません")
    # Render は postgres:// を使うが psycopg2 は postgresql:// を期待する
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    # Render 環境では SSL 必須
    if os.environ.get("RENDER") and "sslmode" not in url:
        sep = "&" if "?" in url else "?"
        url += sep + "sslmode=require"
    return url


@contextmanager
def _conn():
    """コンテキストマネージャ: 接続 → コミット/ロールバック → クローズ"""
    conn = psycopg2.connect(_get_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """テーブル作成 + 既存 JSON ファイルからの初期データ移行"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sites (
                    id      SERIAL,
                    url     TEXT PRIMARY KEY,
                    name    TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS config (
                    key     TEXT PRIMARY KEY,
                    value   TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS hashes (
                    url         TEXT PRIMARY KEY,
                    hash_value  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS last_checks (
                    url         TEXT PRIMARY KEY,
                    timestamp   TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    error       TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS change_history (
                    id          SERIAL PRIMARY KEY,
                    timestamp   TEXT NOT NULL,
                    url         TEXT NOT NULL,
                    name        TEXT NOT NULL DEFAULT '',
                    diff        JSONB NOT NULL DEFAULT '[]',
                    UNIQUE (timestamp, url)
                );
                CREATE TABLE IF NOT EXISTS content_store (
                    url     TEXT PRIMARY KEY,
                    content TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS keywords (
                    id      SERIAL,
                    keyword TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS articles (
                    id          SERIAL PRIMARY KEY,
                    keyword     TEXT NOT NULL,
                    title       TEXT NOT NULL,
                    url         TEXT UNIQUE NOT NULL,
                    source      TEXT NOT NULL DEFAULT '',
                    published   TEXT NOT NULL DEFAULT '',
                    found_at    TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS running_tasks (
                    task_type    TEXT NOT NULL,
                    key          TEXT NOT NULL,
                    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    PRIMARY KEY (task_type, key)
                );
            """)
        # 既存テーブルに completed_at カラムがない場合は追加
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE running_tasks
                ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
            """)
    _migrate_from_json()


def _migrate_from_json():
    """既存 JSON ファイルからの一回限りの移行（テーブルが空のときのみ実行）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            # sites
            cur.execute("SELECT COUNT(*) FROM sites")
            if cur.fetchone()[0] == 0 and os.path.exists("sites.json"):
                with open("sites.json", encoding="utf-8") as f:
                    data = json.load(f)
                for site in data.get("sites", []):
                    cur.execute(
                        "INSERT INTO sites (url, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (site["url"], site.get("name", ""))
                    )
                print(f"[DB移行] sites: {len(data.get('sites', []))} 件")

            # keywords
            cur.execute("SELECT COUNT(*) FROM keywords")
            if cur.fetchone()[0] == 0 and os.path.exists("keywords.json"):
                with open("keywords.json", encoding="utf-8") as f:
                    data = json.load(f)
                count = 0
                for kw in data.get("keywords", []):
                    keyword = kw.get("keyword", "") if isinstance(kw, dict) else kw
                    if keyword:
                        cur.execute(
                            "INSERT INTO keywords (keyword) VALUES (%s) ON CONFLICT DO NOTHING",
                            (keyword,)
                        )
                        count += 1
                if count:
                    print(f"[DB移行] keywords: {count} 件")

            # config
            cur.execute("SELECT COUNT(*) FROM config")
            if cur.fetchone()[0] == 0 and os.path.exists("config.json"):
                with open("config.json", encoding="utf-8") as f:
                    data = json.load(f)
                for key, value in data.items():
                    cur.execute(
                        "INSERT INTO config (key, value) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (key, json.dumps(value))
                    )
                print(f"[DB移行] config: {len(data)} 件")


# ============================================================
# Sites
# ============================================================

def load_sites() -> list:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT url, name FROM sites ORDER BY id")
            return [dict(row) for row in cur.fetchall()]


def save_sites(sites: list):
    with _conn() as conn:
        with conn.cursor() as cur:
            urls = [s["url"] for s in sites]
            if urls:
                cur.execute("DELETE FROM sites WHERE url != ALL(%s)", (urls,))
            else:
                cur.execute("DELETE FROM sites")
            for site in sites:
                cur.execute(
                    "INSERT INTO sites (url, name) VALUES (%s, %s) "
                    "ON CONFLICT (url) DO UPDATE SET name = EXCLUDED.name",
                    (site["url"], site.get("name", ""))
                )


# ============================================================
# Config
# ============================================================

def load_config() -> dict:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT key, value FROM config")
                result = {}
                for key, value in cur.fetchall():
                    try:
                        result[key] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        result[key] = value
                if "check_interval_seconds" not in result:
                    result["check_interval_seconds"] = 3600
                return result
    except Exception:
        return {"check_interval_seconds": 3600}


def save_config(config: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            for key, value in config.items():
                cur.execute(
                    "INSERT INTO config (key, value) VALUES (%s, %s) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    (key, json.dumps(value))
                )


# ============================================================
# Hashes
# ============================================================

def load_hashes() -> dict:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT url, hash_value FROM hashes")
            return {url: h for url, h in cur.fetchall()}


def save_hashes(hashes: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            for url, h in hashes.items():
                cur.execute(
                    "INSERT INTO hashes (url, hash_value) VALUES (%s, %s) "
                    "ON CONFLICT (url) DO UPDATE SET hash_value = EXCLUDED.hash_value",
                    (url, h)
                )


# ============================================================
# Monitor Log (last_checks + change_history)
# ============================================================

def load_monitor_log() -> dict:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT url, timestamp, status, error FROM last_checks")
            last_checks = {
                row["url"]: {
                    "timestamp": row["timestamp"],
                    "status":    row["status"],
                    "error":     row["error"],
                }
                for row in cur.fetchall()
            }
            cur.execute(
                "SELECT timestamp, url, name, diff "
                "FROM change_history ORDER BY id DESC LIMIT 100"
            )
            change_history = [
                {
                    "timestamp": row["timestamp"],
                    "url":       row["url"],
                    "name":      row["name"],
                    "diff":      row["diff"] if row["diff"] else [],
                }
                for row in cur.fetchall()
            ]
    return {"last_checks": last_checks, "change_history": change_history}


def save_monitor_log(log: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            for url, info in log.get("last_checks", {}).items():
                cur.execute(
                    "INSERT INTO last_checks (url, timestamp, status, error) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (url) DO UPDATE SET "
                    "timestamp = EXCLUDED.timestamp, "
                    "status    = EXCLUDED.status, "
                    "error     = EXCLUDED.error",
                    (url, info.get("timestamp", ""), info.get("status", "unknown"), info.get("error", ""))
                )
            for entry in log.get("change_history", []):
                cur.execute(
                    "INSERT INTO change_history (timestamp, url, name, diff) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (timestamp, url) DO NOTHING",
                    (
                        entry.get("timestamp", ""),
                        entry.get("url", ""),
                        entry.get("name", ""),
                        json.dumps(entry.get("diff", [])),
                    )
                )


# ============================================================
# Content Store
# ============================================================

def load_content_store() -> dict:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT url, content FROM content_store")
            return {url: content for url, content in cur.fetchall()}


def save_content_store(store: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            for url, content in store.items():
                cur.execute(
                    "INSERT INTO content_store (url, content) VALUES (%s, %s) "
                    "ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content",
                    (url, content[:30000])
                )


# ============================================================
# Keywords
# ============================================================

def load_keywords() -> list:
    """[{"keyword": "..."}, ...] 形式で返す"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT keyword FROM keywords ORDER BY id")
            return [{"keyword": row[0]} for row in cur.fetchall()]


def save_keywords(keywords: list):
    """[{"keyword": "..."}, ...] 形式を受け取る"""
    kw_list = [
        (k["keyword"] if isinstance(k, dict) else k)
        for k in keywords
        if (k["keyword"] if isinstance(k, dict) else k)
    ]
    with _conn() as conn:
        with conn.cursor() as cur:
            if kw_list:
                cur.execute("DELETE FROM keywords WHERE keyword != ALL(%s)", (kw_list,))
            else:
                cur.execute("DELETE FROM keywords")
            for kw in kw_list:
                cur.execute(
                    "INSERT INTO keywords (keyword) VALUES (%s) ON CONFLICT DO NOTHING",
                    (kw,)
                )


# ============================================================
# Articles
# ============================================================

def load_articles_data() -> dict:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT keyword, title, url, source, published, found_at "
                "FROM articles ORDER BY id DESC LIMIT 1000"
            )
            articles = [dict(row) for row in cur.fetchall()]
    seen_urls = {a["url"]: True for a in articles}
    return {"articles": articles, "seen_urls": seen_urls}


def save_articles_data(data: dict):
    articles = data.get("articles", [])
    with _conn() as conn:
        with conn.cursor() as cur:
            for article in articles:
                cur.execute(
                    "INSERT INTO articles (keyword, title, url, source, published, found_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (url) DO NOTHING",
                    (
                        article.get("keyword", ""),
                        article.get("title", ""),
                        article.get("url", ""),
                        article.get("source", ""),
                        article.get("published", ""),
                        article.get("found_at", ""),
                    )
                )


# ============================================================
# Running Tasks（チェック実行中フラグ）
# ============================================================

# この時間（分）を超えたレコードはサーバー再起動による取り残しとみなし無視する
_TASK_TIMEOUT_MINUTES = 15


def add_running_task(task_type: str, key: str):
    """タスク開始を記録する（古いレコードも同時にクリーンアップ）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            # 1時間以上前の古いレコードを削除
            cur.execute(
                "DELETE FROM running_tasks WHERE started_at < NOW() - INTERVAL '1 hour'"
            )
            cur.execute(
                "INSERT INTO running_tasks (task_type, key, started_at, completed_at) "
                "VALUES (%s, %s, NOW(), NULL) "
                "ON CONFLICT (task_type, key) DO UPDATE SET started_at = NOW(), completed_at = NULL",
                (task_type, key)
            )


def remove_running_task(task_type: str, key: str):
    """タスク完了を記録する（削除ではなく completed_at を設定）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE running_tasks SET completed_at = NOW() "
                "WHERE task_type = %s AND key = %s",
                (task_type, key)
            )


# 完了後もこの秒数はボタンを「実行中」表示する（高速完了時の競合状態対策）
_COMPLETED_GRACE_SECONDS = 30


def get_all_running_tasks() -> dict:
    """実行中タスクを {task_type: {key, ...}} 形式で返す。
    ・completed_at が NULL → まだ実行中
    ・completed_at が設定済みでも _COMPLETED_GRACE_SECONDS 以内 → 表示維持
    ・started_at が _TASK_TIMEOUT_MINUTES 以上前 → 取り残しとみなし除外
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT task_type, key FROM running_tasks "
                "WHERE started_at > NOW() - (INTERVAL '1 minute' * %s) "
                "AND (completed_at IS NULL "
                "     OR completed_at > NOW() - (INTERVAL '1 second' * %s))",
                (_TASK_TIMEOUT_MINUTES, _COMPLETED_GRACE_SECONDS)
            )
            result: dict = {}
            for task_type, key in cur.fetchall():
                result.setdefault(task_type, set()).add(key)
            return result
