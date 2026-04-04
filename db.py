"""
データベース操作モジュール (PostgreSQL)
"""
import hashlib
import hmac as _hmac
import json
import os
import secrets
import psycopg2
import psycopg2.extras
from contextlib import contextmanager


def _get_dsn() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL 環境変数が設定されていません")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if os.environ.get("RENDER") and "sslmode" not in url:
        sep = "&" if "?" in url else "?"
        url += sep + "sslmode=require"
    return url


@contextmanager
def _conn():
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
    """テーブル作成・マイグレーション"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id            SERIAL PRIMARY KEY,
                    email         TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    salt          TEXT NOT NULL,
                    is_admin      BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS sites (
                    id      SERIAL PRIMARY KEY,
                    url     TEXT NOT NULL,
                    name    TEXT NOT NULL DEFAULT '',
                    user_id INTEGER REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS config (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS hashes (
                    url        TEXT PRIMARY KEY,
                    hash_value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS last_checks (
                    url       TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    status    TEXT NOT NULL,
                    error     TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS change_history (
                    id        SERIAL PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    url       TEXT NOT NULL,
                    name      TEXT NOT NULL DEFAULT '',
                    diff      JSONB NOT NULL DEFAULT '[]',
                    UNIQUE (timestamp, url)
                );
                CREATE TABLE IF NOT EXISTS content_store (
                    url     TEXT PRIMARY KEY,
                    content TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS keywords (
                    id      SERIAL PRIMARY KEY,
                    keyword TEXT NOT NULL,
                    user_id INTEGER REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS articles (
                    id        SERIAL PRIMARY KEY,
                    keyword   TEXT NOT NULL,
                    title     TEXT NOT NULL,
                    url       TEXT NOT NULL,
                    source    TEXT NOT NULL DEFAULT '',
                    published TEXT NOT NULL DEFAULT '',
                    found_at  TEXT NOT NULL DEFAULT '',
                    user_id   INTEGER REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS running_tasks (
                    task_type    TEXT NOT NULL,
                    key          TEXT NOT NULL,
                    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    PRIMARY KEY (task_type, key)
                );
                CREATE TABLE IF NOT EXISTS password_reset_tokens (
                    token      TEXT PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    expires_at TIMESTAMPTZ NOT NULL,
                    used       BOOLEAN NOT NULL DEFAULT FALSE
                );
            """)
    _run_migrations()


def _run_migrations():
    """既存テーブルへのカラム追加・制約変更"""
    with _conn() as conn:
        with conn.cursor() as cur:
            # running_tasks: completed_at 追加
            cur.execute("ALTER TABLE running_tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;")

            # sites: user_id 追加 + url PK → id PK + UNIQUE(user_id, url)
            cur.execute("ALTER TABLE sites ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);")
            cur.execute("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.key_column_usage
                        WHERE constraint_name = 'sites_pkey'
                          AND table_name = 'sites'
                          AND column_name = 'url'
                    ) THEN
                        ALTER TABLE sites DROP CONSTRAINT sites_pkey;
                        ALTER TABLE sites ADD PRIMARY KEY (id);
                    END IF;
                END $$;
            """)
            cur.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'sites'
                          AND constraint_name = 'sites_user_url_unique'
                    ) THEN
                        ALTER TABLE sites ADD CONSTRAINT sites_user_url_unique UNIQUE (user_id, url);
                    END IF;
                END $$;
            """)

            # keywords: user_id 追加 + keyword PK → id PK + UNIQUE(user_id, keyword)
            cur.execute("ALTER TABLE keywords ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);")
            cur.execute("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.key_column_usage
                        WHERE constraint_name = 'keywords_pkey'
                          AND table_name = 'keywords'
                          AND column_name = 'keyword'
                    ) THEN
                        ALTER TABLE keywords DROP CONSTRAINT keywords_pkey;
                        ALTER TABLE keywords ADD PRIMARY KEY (id);
                    END IF;
                END $$;
            """)
            cur.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'keywords'
                          AND constraint_name = 'keywords_user_kw_unique'
                    ) THEN
                        ALTER TABLE keywords ADD CONSTRAINT keywords_user_kw_unique UNIQUE (user_id, keyword);
                    END IF;
                END $$;
            """)

            # articles: user_id 追加 + url UNIQUE → (user_id, url) UNIQUE
            cur.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);")
            cur.execute("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'articles'
                          AND constraint_name = 'articles_url_key'
                    ) THEN
                        ALTER TABLE articles DROP CONSTRAINT articles_url_key;
                    END IF;
                END $$;
            """)
            cur.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'articles'
                          AND constraint_name = 'articles_user_url_unique'
                    ) THEN
                        ALTER TABLE articles ADD CONSTRAINT articles_user_url_unique UNIQUE (user_id, url);
                    END IF;
                END $$;
            """)

            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS is_read BOOLEAN NOT NULL DEFAULT FALSE;"
            )
            cur.execute(
                "ALTER TABLE keywords ADD COLUMN IF NOT EXISTS notify_enabled BOOLEAN NOT NULL DEFAULT TRUE;"
            )

            # articles: 同一(user_id, keyword, title)の重複行を削除（id最大=最新を残す）
            cur.execute("""
                DELETE FROM articles
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM articles
                    GROUP BY user_id, keyword, title
                );
            """)

            # articles: (user_id, keyword, title) UNIQUE制約追加
            cur.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'articles'
                          AND constraint_name = 'articles_user_kw_title_unique'
                    ) THEN
                        ALTER TABLE articles
                            ADD CONSTRAINT articles_user_kw_title_unique
                            UNIQUE (user_id, keyword, title);
                    END IF;
                END $$;
            """)

            # user_id=NULL の孤立キーワード・記事を整理
            # まず user_id=1 に重複しないキーワードを移行し、残りを削除
            cur.execute("""
                UPDATE keywords SET user_id = 1
                WHERE user_id IS NULL
                  AND EXISTS (SELECT 1 FROM users WHERE id = 1)
                  AND keyword NOT IN (
                      SELECT keyword FROM keywords WHERE user_id = 1
                  );
            """)
            cur.execute("DELETE FROM keywords WHERE user_id IS NULL;")
            # user_id=NULL の記事も削除（孤立データ）
            cur.execute("DELETE FROM articles WHERE user_id IS NULL;")

            # キーワード一覧に存在しないキーワードの記事を削除
            # （キーワード削除時に記事も消えるが、過去データの残骸をクリーンアップ）
            cur.execute("""
                DELETE FROM articles a
                WHERE a.user_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM keywords k
                      WHERE k.user_id = a.user_id
                        AND k.keyword = a.keyword
                  );
            """)

            # ADMIN_EMAIL で指定されたユーザーを管理者に設定
            admin_email = os.environ.get("ADMIN_EMAIL", "").lower().strip()
            if admin_email:
                cur.execute(
                    "UPDATE users SET is_admin = TRUE WHERE email = %s",
                    (admin_email,)
                )


# ============================================================
# Users
# ============================================================

def _hash_pw(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def create_user(email: str, password: str) -> int:
    """新規ユーザーを作成して user_id を返す"""
    salt = secrets.token_hex(16)
    pw_hash = _hash_pw(password, salt)
    admin_email = os.environ.get("ADMIN_EMAIL", "").lower().strip()
    is_admin = bool(admin_email and email.lower() == admin_email)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (email, password_hash, salt, is_admin) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (email.lower(), pw_hash, salt, is_admin)
            )
            return cur.fetchone()[0]


def get_user_by_email(email: str):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, password_hash, salt, is_admin FROM users WHERE email = %s",
                (email.lower(),)
            )
            row = cur.fetchone()
            return dict(row) if row else None


def get_user_by_id(user_id: int):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, password_hash, salt, is_admin FROM users WHERE id = %s",
                (user_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None


def verify_user_password(user: dict, password: str) -> bool:
    h = _hash_pw(password, user["salt"])
    return _hmac.compare_digest(h, user["password_hash"])


def update_user_password(user_id: int, new_password: str):
    salt = secrets.token_hex(16)
    pw_hash = _hash_pw(new_password, salt)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash = %s, salt = %s WHERE id = %s",
                (pw_hash, salt, user_id)
            )


def get_all_users() -> list:
    """管理者用: 全ユーザー一覧（各種件数付き）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT u.id, u.email, u.is_admin, u.created_at,
                    (SELECT COUNT(*) FROM sites    s WHERE s.user_id = u.id) AS site_count,
                    (SELECT COUNT(*) FROM keywords k WHERE k.user_id = u.id) AS keyword_count,
                    (SELECT COUNT(*) FROM articles a WHERE a.user_id = u.id) AS article_count
                FROM users u
                ORDER BY u.created_at
            """)
            return [dict(row) for row in cur.fetchall()]


# ============================================================
# Sites
# ============================================================

def load_sites(user_id=None) -> list:
    """user_id=None → 全件（管理者用）、user_id=X → そのユーザーのみ"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if user_id is not None:
                cur.execute("SELECT url, name FROM sites WHERE user_id = %s ORDER BY id", (user_id,))
            else:
                cur.execute("SELECT url, name FROM sites ORDER BY id")
            return [dict(row) for row in cur.fetchall()]


def load_sites_for_monitor() -> list:
    """
    monitor.py 用: user_id が設定済みのサイトだけ監視する。

    以前の JSON 移行（sites.json）由来で `user_id` が NULL になっている行がある場合、
    それらを監視対象から除外してダッシュボード登録分だけを監視できるようにする。
    """
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT url, name FROM sites WHERE user_id IS NOT NULL ORDER BY id")
            return [dict(row) for row in cur.fetchall()]


def save_sites(sites: list, user_id: int):
    with _conn() as conn:
        with conn.cursor() as cur:
            urls = [s["url"] for s in sites]
            if urls:
                cur.execute("DELETE FROM sites WHERE user_id = %s AND url != ALL(%s)", (user_id, urls))
            else:
                cur.execute("DELETE FROM sites WHERE user_id = %s", (user_id,))
            for site in sites:
                cur.execute(
                    "INSERT INTO sites (url, name, user_id) VALUES (%s, %s, %s) "
                    "ON CONFLICT (user_id, url) DO UPDATE SET name = EXCLUDED.name",
                    (site["url"], site.get("name", ""), user_id)
                )


def update_site_name(user_id: int, url: str, name: str) -> bool:
    """指定ユーザーの指定URLの会社名だけを更新する（URLは変更しない）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sites SET name = %s WHERE user_id = %s AND url = %s",
                (name, user_id, url)
            )
            return cur.rowcount > 0


# ============================================================
# Config （グローバル設定）
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
# Hashes （グローバル: URL→ハッシュ値）
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
# Monitor Log （グローバル: 表示時にユーザーのURLでフィルタ）
# ============================================================

def load_monitor_log(user_id=None) -> dict:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # ユーザーの登録URL一覧を取得
            if user_id is not None:
                cur.execute("SELECT url FROM sites WHERE user_id = %s", (user_id,))
                user_urls = [row["url"] for row in cur.fetchall()]
                if not user_urls:
                    return {"last_checks": {}, "change_history": []}

            if user_id is not None:
                cur.execute(
                    "SELECT url, timestamp, status, error FROM last_checks WHERE url = ANY(%s)",
                    (user_urls,)
                )
            else:
                cur.execute("SELECT url, timestamp, status, error FROM last_checks")
            last_checks = {
                row["url"]: {"timestamp": row["timestamp"], "status": row["status"], "error": row["error"]}
                for row in cur.fetchall()
            }

            if user_id is not None:
                cur.execute(
                    "SELECT timestamp, url, name, diff FROM change_history "
                    "WHERE url = ANY(%s) ORDER BY id DESC LIMIT 100",
                    (user_urls,)
                )
            else:
                cur.execute(
                    "SELECT timestamp, url, name, diff FROM change_history ORDER BY id DESC LIMIT 100"
                )
            change_history = [
                {"timestamp": r["timestamp"], "url": r["url"], "name": r["name"],
                 "diff": r["diff"] if r["diff"] else []}
                for r in cur.fetchall()
            ]
    return {"last_checks": last_checks, "change_history": change_history}


def save_monitor_log(log: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            for url, info in log.get("last_checks", {}).items():
                cur.execute(
                    "INSERT INTO last_checks (url, timestamp, status, error) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (url) DO UPDATE SET "
                    "timestamp = EXCLUDED.timestamp, status = EXCLUDED.status, error = EXCLUDED.error",
                    (url, info.get("timestamp", ""), info.get("status", "unknown"), info.get("error", ""))
                )
            for entry in log.get("change_history", []):
                cur.execute(
                    "INSERT INTO change_history (timestamp, url, name, diff) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (timestamp, url) DO NOTHING",
                    (entry.get("timestamp", ""), entry.get("url", ""),
                     entry.get("name", ""), json.dumps(entry.get("diff", [])))
                )


# ============================================================
# Content Store （グローバル）
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

def load_keywords(user_id=None) -> list:
    """[{"keyword": "...", "notify_enabled": bool}, ...] 形式で返す"""
    with _conn() as conn:
        with conn.cursor() as cur:
            if user_id is not None:
                cur.execute(
                    "SELECT keyword, COALESCE(notify_enabled, TRUE) FROM keywords "
                    "WHERE user_id = %s ORDER BY id",
                    (user_id,),
                )
            else:
                cur.execute(
                    "SELECT keyword, COALESCE(notify_enabled, TRUE) FROM keywords ORDER BY id"
                )
            return [{"keyword": row[0], "notify_enabled": bool(row[1])} for row in cur.fetchall()]


def add_keyword_if_not_exists(user_id: int, keyword: str) -> bool:
    """キーワードを1件追加する。既に存在する場合は何もせず False を返す。
    ON CONFLICT DO NOTHING でアトミックに重複チェックと挿入を行う。
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO keywords (keyword, user_id, notify_enabled) VALUES (%s, %s, TRUE) "
                "ON CONFLICT (user_id, keyword) DO NOTHING",
                (keyword, user_id),
            )
            return cur.rowcount > 0


def save_keywords(keywords: list, user_id: int):
    normalized = []
    for k in keywords:
        if isinstance(k, dict):
            kw = k.get("keyword", "").strip()
            if kw:
                normalized.append((kw, k.get("notify_enabled", True)))
        else:
            s = str(k).strip()
            if s:
                normalized.append((s, True))
    kw_list = [kw for kw, _ in normalized]
    with _conn() as conn:
        with conn.cursor() as cur:
            if kw_list:
                cur.execute("DELETE FROM keywords WHERE user_id = %s AND keyword != ALL(%s)", (user_id, kw_list))
            else:
                cur.execute("DELETE FROM keywords WHERE user_id = %s", (user_id,))
            for kw, notify in normalized:
                cur.execute(
                    "INSERT INTO keywords (keyword, user_id, notify_enabled) VALUES (%s, %s, %s) "
                    "ON CONFLICT (user_id, keyword) DO NOTHING",
                    (kw, user_id, notify)
                )


def update_keyword_notify(user_id: int, keyword: str, notify_enabled: bool) -> bool:
    """キーワードのメール通知ON/OFFを更新する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE keywords SET notify_enabled = %s WHERE user_id = %s AND keyword = %s",
                (notify_enabled, user_id, keyword),
            )
            updated = cur.rowcount > 0
            print(
                f"[DB] update_keyword_notify keyword={keyword!r} user_id={user_id} "
                f"notify_enabled={notify_enabled} rowcount={cur.rowcount}",
                flush=True,
            )
            return updated


def is_keyword_notify_enabled(user_id: int, keyword: str) -> bool:
    """キーワードの通知が有効か（行が無い場合は True）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(notify_enabled, TRUE) FROM keywords WHERE user_id = %s AND keyword = %s",
                (user_id, keyword),
            )
            row = cur.fetchone()
            result = bool(row[0]) if row else True
            print(
                f"[DB] is_keyword_notify_enabled keyword={keyword!r} user_id={user_id} "
                f"row={row} → {result}",
                flush=True,
            )
            return result


def load_all_keywords_with_users() -> list:
    """バックグラウンド用: [(user_id, keyword, notify_enabled), ...]
    user_id が NULL の孤立キーワードは除外する。
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, keyword, COALESCE(notify_enabled, TRUE) FROM keywords "
                "WHERE user_id IS NOT NULL ORDER BY id"
            )
            return [(row[0], row[1], bool(row[2])) for row in cur.fetchall()]


# ============================================================
# Articles
# ============================================================

def load_article_seen_urls(user_id: int) -> set:
    """ユーザーが既に登録済みの記事URL集合（重複検知用。件数制限なし）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT url FROM articles WHERE user_id = %s", (user_id,))
            return {row[0] for row in cur.fetchall()}


def load_article_seen_titles(user_id: int) -> set:
    """ユーザーが既に登録済みの (keyword, title) 集合（タイトル重複検知用）。
    返り値: {"keyword::title", ...} 形式の set
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT keyword, title FROM articles WHERE user_id = %s", (user_id,))
            return {f"{row[0]}::{row[1]}" for row in cur.fetchall()}


def load_articles_data(user_id=None) -> dict:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if user_id is not None:
                cur.execute(
                    "SELECT id, keyword, title, url, source, published, found_at, is_read "
                    "FROM articles WHERE user_id = %s ORDER BY id DESC LIMIT 1000",
                    (user_id,)
                )
                articles = [dict(row) for row in cur.fetchall()]
                cur.execute("SELECT url FROM articles WHERE user_id = %s", (user_id,))
                seen_urls = {row["url"]: True for row in cur.fetchall()}
            else:
                cur.execute(
                    "SELECT id, keyword, title, url, source, published, found_at, is_read "
                    "FROM articles ORDER BY id DESC LIMIT 1000"
                )
                articles = [dict(row) for row in cur.fetchall()]
                cur.execute("SELECT url FROM articles")
                seen_urls = {row["url"]: True for row in cur.fetchall()}
    return {"articles": articles, "seen_urls": seen_urls}


def insert_articles(articles: list, user_id: int):
    """新着記事のみDBに登録する（URL・タイトル重複は ON CONFLICT で無視）"""
    if not articles:
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            for article in articles:
                cur.execute(
                    "INSERT INTO articles (keyword, title, url, source, published, found_at, user_id, is_read) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE) ON CONFLICT DO NOTHING",
                    (article.get("keyword", ""), article.get("title", ""), article.get("url", ""),
                     article.get("source", ""), article.get("published", ""),
                     article.get("found_at", ""), user_id)
                )


def delete_articles_by_keyword(user_id: int, keyword: str):
    """指定キーワードに紐づく記事を全件削除する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM articles WHERE user_id = %s AND keyword = %s",
                (user_id, keyword),
            )


def mark_article_read(user_id: int, article_id: int) -> bool:
    """記事を既読にする"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET is_read = TRUE WHERE id = %s AND user_id = %s",
                (article_id, user_id),
            )
            return cur.rowcount > 0


def mark_article_unread(user_id: int, article_id: int) -> bool:
    """記事を未読に戻す"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET is_read = FALSE WHERE id = %s AND user_id = %s",
                (article_id, user_id),
            )
            return cur.rowcount > 0


# ============================================================
# Running Tasks
# ============================================================

_TASK_TIMEOUT_MINUTES  = 10
_COMPLETED_GRACE_SECONDS = 30


def add_running_task(task_type: str, key: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM running_tasks WHERE started_at < NOW() - INTERVAL '1 hour'")
            cur.execute(
                "INSERT INTO running_tasks (task_type, key, started_at, completed_at) "
                "VALUES (%s, %s, NOW(), NULL) "
                "ON CONFLICT (task_type, key) DO UPDATE SET started_at = NOW(), completed_at = NULL",
                (task_type, key)
            )


def remove_running_task(task_type: str, key: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE running_tasks SET completed_at = NOW() WHERE task_type = %s AND key = %s",
                (task_type, key)
            )


def get_running_task_statuses() -> dict:
    """実行中・完了猶予期間内のタスクをステータス付きで返す。
    Returns: {task_type: {key: "running" | "completed"}}
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT task_type, key, completed_at FROM running_tasks "
                "WHERE started_at > NOW() - (INTERVAL '1 minute' * %s) "
                "AND (completed_at IS NULL OR completed_at > NOW() - (INTERVAL '1 second' * %s))",
                (_TASK_TIMEOUT_MINUTES, _COMPLETED_GRACE_SECONDS)
            )
            result: dict = {}
            for task_type, key, completed_at in cur.fetchall():
                status = "completed" if completed_at is not None else "running"
                result.setdefault(task_type, {})[key] = status
            return result


def get_all_running_tasks() -> dict:
    """後方互換用: 実行中・完了猶予期間内タスクのキーセットを返す。"""
    statuses = get_running_task_statuses()
    return {task_type: set(keys.keys()) for task_type, keys in statuses.items()}


# ============================================================
# Password Reset Tokens
# ============================================================

def create_reset_token(user_id: int) -> str:
    """パスワードリセット用トークンを生成してDBに保存し、トークン文字列を返す。
    有効期限は1時間。既存の未使用トークンは削除してから新規作成する。
    """
    token = secrets.token_urlsafe(32)
    with _conn() as conn:
        with conn.cursor() as cur:
            # 古いトークンを削除
            cur.execute("DELETE FROM password_reset_tokens WHERE user_id = %s", (user_id,))
            cur.execute(
                "INSERT INTO password_reset_tokens (token, user_id, expires_at) "
                "VALUES (%s, %s, NOW() + INTERVAL '1 hour')",
                (token, user_id),
            )
    return token


def get_reset_token_user_id(token: str):
    """有効なトークンに紐づく user_id を返す。無効・期限切れ・使用済みは None。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id FROM password_reset_tokens "
                "WHERE token = %s AND used = FALSE AND expires_at > NOW()",
                (token,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def invalidate_reset_token(token: str):
    """トークンを使用済みにする。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE password_reset_tokens SET used = TRUE WHERE token = %s",
                (token,),
            )
