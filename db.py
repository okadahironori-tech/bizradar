"""
データベース操作モジュール (PostgreSQL)
"""
from __future__ import annotations
import hashlib
import hmac as _hmac
import logging
import bcrypt as _bcrypt

logger = logging.getLogger(__name__)


def normalize_domain(value: str) -> str:
    """ドメインを正規化して registered domain (例: toyota.co.jp) を返す。"""
    if not value:
        return ""
    try:
        import tldextract
        from urllib.parse import urlparse
        v = value.strip().lower()
        if "://" in v or "/" in v:
            netloc = urlparse(v).netloc
            if not netloc:
                return ""
            v = netloc
        ext = tldextract.extract(v)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
        return ""
    except Exception:
        return ""


def clean_hostname(value: str) -> str:
    """ホスト名を小文字化・空白除去・ポート除去・末尾ドット除去して返す。"""
    if not value:
        return ""
    try:
        from urllib.parse import urlparse
        v = value.strip().lower()
        if "://" in v or "/" in v:
            netloc = urlparse(v).netloc
            if not netloc:
                return ""
            v = netloc
        if ":" in v:
            v = v.rsplit(":", 1)[0]
        v = v.rstrip(".")
        return v
    except Exception:
        return ""


_gunicorn_error = logging.getLogger("gunicorn.error")
if _gunicorn_error.handlers:
    logger.handlers = _gunicorn_error.handlers
    logger.setLevel(_gunicorn_error.level)
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
                    id                 SERIAL PRIMARY KEY,
                    email              TEXT UNIQUE NOT NULL,
                    password_hash      TEXT NOT NULL,
                    salt               TEXT NOT NULL,
                    is_admin           BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    -- 課金プラン: 'free' / 'pro' / 'corporate'
                    plan               TEXT NOT NULL DEFAULT 'free',
                    stripe_customer_id TEXT
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
                CREATE TABLE IF NOT EXISTS magic_tokens (
                    id         SERIAL PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token      VARCHAR(64) NOT NULL UNIQUE,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMP NOT NULL,
                    used_at    TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS tdnet_disclosures (
                    id            SERIAL PRIMARY KEY,
                    document_id   VARCHAR(20) NOT NULL UNIQUE,
                    company_name  TEXT NOT NULL,
                    title         TEXT NOT NULL,
                    disclosed_at  TIMESTAMP NOT NULL,
                    document_url  TEXT NOT NULL,
                    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS system_status (
                    key        VARCHAR(50) PRIMARY KEY,
                    value      TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS securities_master (
                    code       VARCHAR(10) PRIMARY KEY,
                    name       VARCHAR(255) NOT NULL,
                    name_kana  VARCHAR(255),
                    market     VARCHAR(50),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS alert_keywords (
                    id         SERIAL PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    keyword    TEXT NOT NULL,
                    UNIQUE (user_id, keyword)
                );
                CREATE TABLE IF NOT EXISTS companies (
                    id          SERIAL PRIMARY KEY,
                    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name        TEXT NOT NULL,
                    name_kana   TEXT NOT NULL DEFAULT '',
                    website_url TEXT NOT NULL DEFAULT '',
                    memo        TEXT NOT NULL DEFAULT '',
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS source_health (
                    source               TEXT PRIMARY KEY,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    last_error           TEXT,
                    last_checked_at      TIMESTAMPTZ,
                    error_notified_at    TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS exclude_keywords (
                    id      SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    keyword VARCHAR(100) NOT NULL,
                    UNIQUE(user_id, keyword)
                );
                CREATE TABLE IF NOT EXISTS company_exclude_keywords (
                    id           SERIAL PRIMARY KEY,
                    company_id   INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    user_id      INTEGER NOT NULL REFERENCES users(id),
                    exclude_word VARCHAR(100) NOT NULL,
                    UNIQUE(company_id, exclude_word)
                );
                CREATE TABLE IF NOT EXISTS company_alert_keywords (
                    id         SERIAL PRIMARY KEY,
                    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    user_id    INTEGER NOT NULL REFERENCES users(id),
                    keyword    VARCHAR(100) NOT NULL,
                    UNIQUE(company_id, keyword)
                );
                CREATE TABLE IF NOT EXISTS domain_overrides (
                    id            SERIAL PRIMARY KEY,
                    domain        TEXT NOT NULL UNIQUE,
                    suggested_url TEXT NOT NULL,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
    _run_migrations()


def _run_migrations():
    """既存テーブルへのカラム追加・制約変更"""
    with _conn() as conn:
        with conn.cursor() as cur:
            # running_tasks: completed_at 追加
            cur.execute("ALTER TABLE running_tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;")
            # running_tasks: error_message 追加
            cur.execute("ALTER TABLE running_tasks ADD COLUMN IF NOT EXISTS error_message TEXT;")

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

            # users: 通知タイミング設定
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "notify_timing TEXT NOT NULL DEFAULT 'immediate';"
            )
            # articles: ダイジェスト送信済みフラグ
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ;"
            )
            # articles: 取得日付が信頼できるか（未来/30日超前は fetch で差し替え or False）
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "date_verified BOOLEAN DEFAULT FALSE;"
            )
            # articles: 類似タイトルによる重複グルーピング用
            cur.execute("ALTER TABLE articles ADD COLUMN IF NOT EXISTS group_id INTEGER;")
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "is_representative BOOLEAN NOT NULL DEFAULT TRUE;"
            )
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "duplicate_count INTEGER NOT NULL DEFAULT 0;"
            )
            # articles: Claude 重要度スコア (high/medium/low)
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "importance TEXT NOT NULL DEFAULT 'low';"
            )
            # articles: Claude による本文要約（business/pro のみ）
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "summary TEXT NOT NULL DEFAULT '';"
            )

            # sites / keywords: company_id カラム追加
            cur.execute(
                "ALTER TABLE sites ADD COLUMN IF NOT EXISTS "
                "company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL;"
            )
            # sites: enabled カラム追加
            cur.execute(
                "ALTER TABLE sites ADD COLUMN IF NOT EXISTS "
                "enabled BOOLEAN NOT NULL DEFAULT TRUE;"
            )
            cur.execute(
                "ALTER TABLE keywords ADD COLUMN IF NOT EXISTS "
                "company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL;"
            )

            # keywords: 表示順カラム（ドラッグ&ドロップ並び替え用）
            cur.execute(
                "ALTER TABLE keywords ADD COLUMN IF NOT EXISTS "
                "sort_order INTEGER NOT NULL DEFAULT 0;"
            )
            cur.execute("UPDATE keywords SET sort_order = id WHERE sort_order = 0;")

            # keywords: 重複行を削除してUNIQUE制約を確実に追加
            cur.execute("""
                DELETE FROM keywords k1
                USING keywords k2
                WHERE k1.id < k2.id
                  AND k1.user_id IS NOT DISTINCT FROM k2.user_id
                  AND k1.keyword = k2.keyword;
            """)
            cur.execute("""
                ALTER TABLE keywords
                    ADD COLUMN IF NOT EXISTS notify_enabled BOOLEAN DEFAULT TRUE;
            """)
            cur.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'keywords'
                          AND constraint_name = 'keywords_user_keyword_unique'
                    ) THEN
                        ALTER TABLE keywords
                            ADD CONSTRAINT keywords_user_keyword_unique
                            UNIQUE (user_id, keyword);
                    END IF;
                END $$;
            """)

            # users: 課金プラン ('free' / 'pro' / 'corporate') と Stripe 顧客 ID
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "plan TEXT NOT NULL DEFAULT 'basic';"
            )
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "stripe_customer_id TEXT;"
            )
            # users: Slack Incoming Webhook URL
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "slack_webhook_url TEXT NOT NULL DEFAULT '';"
            )
            # users: LINE Messaging API の userId（Webhook follow イベントで取得）
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "line_user_id TEXT NOT NULL DEFAULT '';"
            )
            # line_pending_links: follow 直後〜設定画面で連携コードを入力するまでの一時テーブル
            cur.execute("""
                CREATE TABLE IF NOT EXISTS line_pending_links (
                    line_user_id TEXT PRIMARY KEY,
                    code         VARCHAR(8) NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            # users: plan='free' を 'basic' に一括移行 + デフォルト値も更新
            cur.execute("UPDATE users SET plan = 'basic' WHERE plan = 'free';")
            cur.execute("ALTER TABLE users ALTER COLUMN plan SET DEFAULT 'basic';")

            # sites: 複数ページ取得数
            cur.execute(
                "ALTER TABLE sites ADD COLUMN IF NOT EXISTS "
                "max_pages INTEGER DEFAULT 1;"
            )

            # companies: YouTubeチャンネルID（旧・単一。新テーブルに移行済み）
            cur.execute(
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
                "youtube_channel_id TEXT;"
            )
            # company_youtube_channels: 企業ごとに複数チャンネル登録
            cur.execute("""
                CREATE TABLE IF NOT EXISTS company_youtube_channels (
                    id          SERIAL PRIMARY KEY,
                    company_id  INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    channel_id  TEXT NOT NULL,
                    label       TEXT,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (company_id, channel_id)
                );
            """)
            # articles: AI判定の主役企業
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "primary_company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL;"
            )
            # badge_feedback: バッジNGフィードバック
            cur.execute("""
                CREATE TABLE IF NOT EXISTS badge_feedback (
                    id                SERIAL PRIMARY KEY,
                    article_id        INTEGER REFERENCES articles(id) ON DELETE CASCADE,
                    user_id           INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    correct_company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL,
                    reason_type       TEXT,
                    reason_text       TEXT,
                    created_at        TIMESTAMP DEFAULT NOW()
                );
            """)

            cur.execute(
                "ALTER TABLE badge_feedback ADD COLUMN IF NOT EXISTS "
                "importance_feedback TEXT;"
            )

            # excluded_sources: 除外配信元
            cur.execute("""
                CREATE TABLE IF NOT EXISTS excluded_sources (
                    id          SERIAL PRIMARY KEY,
                    user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    source_name TEXT NOT NULL,
                    created_at  TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, source_name)
                );
            """)

            # companies: 企業単位の通知オン/オフ
            cur.execute(
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
                "notify_enabled BOOLEAN NOT NULL DEFAULT TRUE;"
            )
            # companies: 即時通知フラグ
            cur.execute(
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
                "notify_instant BOOLEAN NOT NULL DEFAULT FALSE;"
            )

            # users: 通知曜日（カンマ区切り 0=日 1=月 ... 6=土）
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "notify_days TEXT NOT NULL DEFAULT '0,1,2,3,4,5,6';"
            )

            # users: ふりがな・電話番号
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name_kana TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name_kana TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT;")

            # users: 利用停止フラグ
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "is_active BOOLEAN DEFAULT TRUE;"
            )

            # users: プロフィール項目
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS company_name TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS industry TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS job_type TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS job_title TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS company_size TEXT;")

            # users: 氏名（メール本文の宛名表示に使用、任意入力）
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "last_name TEXT NOT NULL DEFAULT '';"
            )
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "first_name TEXT NOT NULL DEFAULT '';"
            )
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "last_login_at TIMESTAMP WITH TIME ZONE;"
            )
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "last_active_at TIMESTAMP WITH TIME ZONE;"
            )
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "prev_active_at TIMESTAMP WITH TIME ZONE;"
            )
            # users: ダッシュボードカード表示設定（JSONB）
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "dashboard_settings JSONB DEFAULT NULL;"
            )

            # users: スポーツ記事フィルター (BOOLEAN→VARCHAR(10) 移行対応)
            cur.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name='users' AND column_name='sports_filter'"
            )
            sf_row = cur.fetchone()
            if sf_row is None:
                cur.execute(
                    "ALTER TABLE users ADD COLUMN sports_filter "
                    "VARCHAR(10) NOT NULL DEFAULT 'low';"
                )
            elif sf_row[0] == 'boolean':
                cur.execute(
                    "ALTER TABLE users ADD COLUMN sports_filter_new "
                    "VARCHAR(10) NOT NULL DEFAULT 'low';"
                )
                cur.execute(
                    "UPDATE users SET sports_filter_new = CASE "
                    "WHEN sports_filter = TRUE THEN 'low' "
                    "WHEN sports_filter = FALSE THEN 'off' "
                    "ELSE 'low' END;"
                )
                cur.execute("ALTER TABLE users DROP COLUMN sports_filter;")
                cur.execute(
                    "ALTER TABLE users RENAME COLUMN sports_filter_new "
                    "TO sports_filter;"
                )
            cur.execute(
                "UPDATE users SET sports_filter = 'low' "
                "WHERE sports_filter IS NULL;"
            )
            # articles: スポーツ記事フラグ
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "is_sports BOOLEAN DEFAULT FALSE;"
            )
            # users: エンタメ・芸能記事フィルター
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "entertainment_filter VARCHAR(10) NOT NULL DEFAULT 'low';"
            )
            cur.execute(
                "UPDATE users SET entertainment_filter = 'low' "
                "WHERE entertainment_filter IS NULL;"
            )
            # articles: エンタメ・芸能記事フラグ
            cur.execute(
                "ALTER TABLE articles ADD COLUMN IF NOT EXISTS "
                "is_entertainment BOOLEAN DEFAULT FALSE;"
            )

            # companies: 並び順カラム追加
            cur.execute(
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
                "sort_order INTEGER NOT NULL DEFAULT 0;"
            )
            # 既存レコードの sort_order を id 値で初期化（0 のままのものだけ）
            cur.execute(
                "UPDATE companies SET sort_order = id WHERE sort_order = 0;"
            )
            # companies: 証券コード（上場企業用、任意）
            cur.execute(
                "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
                "securities_code VARCHAR(10);"
            )
            # tdnet_disclosures: 証券コード（TDnet API の company_code を保存）
            cur.execute(
                "ALTER TABLE tdnet_disclosures ADD COLUMN IF NOT EXISTS "
                "securities_code VARCHAR(10);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_tdnet_securities_code "
                "ON tdnet_disclosures (securities_code);"
            )
            # 既存レコードの5桁コードを4桁に正規化する一度きりのマイグレーション。
            # TDnet の5桁形式（末尾0付き 例: 72030）→ JPX 4桁（7203）。
            # LENGTH=5 かつ末尾 '0' のものだけを先頭4桁に切り詰める（冪等）。
            cur.execute(
                "UPDATE tdnet_disclosures SET securities_code = LEFT(securities_code, 4) "
                "WHERE securities_code IS NOT NULL "
                "AND LENGTH(securities_code) = 5 "
                "AND securities_code LIKE '%0';"
            )

            # domain_overrides: 企業名カラム追加
            cur.execute(
                "ALTER TABLE domain_overrides ADD COLUMN IF NOT EXISTS "
                "company_name TEXT NOT NULL DEFAULT '';"
            )
            cur.execute(
                "ALTER TABLE domain_overrides ADD COLUMN IF NOT EXISTS "
                "company_name_kana TEXT NOT NULL DEFAULT '';"
            )

            # domain_overrides: 正規化例外フラグ
            cur.execute(
                "ALTER TABLE domain_overrides ADD COLUMN IF NOT EXISTS "
                "is_exception BOOLEAN DEFAULT FALSE;"
            )

            # JPX 上場企業一覧
            cur.execute("""
                CREATE TABLE IF NOT EXISTS listed_companies (
                    securities_code VARCHAR(10) PRIMARY KEY,
                    company_name VARCHAR(255) NOT NULL,
                    company_name_kana VARCHAR(255),
                    market VARCHAR(50),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)

            # fix_url_log: エラーURL修正履歴
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fix_url_log (
                    id SERIAL PRIMARY KEY,
                    securities_code VARCHAR(10),
                    company_name VARCHAR(255),
                    old_url TEXT,
                    new_url TEXT,
                    fixed_at TIMESTAMP DEFAULT NOW(),
                    fixed_by VARCHAR(50) DEFAULT 'admin'
                );
            """)

            # merge_log: ドメインオーバーライドマージ履歴
            cur.execute("""
                CREATE TABLE IF NOT EXISTS merge_log (
                    id SERIAL PRIMARY KEY,
                    executed_at TIMESTAMP DEFAULT NOW(),
                    action VARCHAR(20) NOT NULL,
                    normalized_domain VARCHAR(255),
                    kept_entry_id INTEGER,
                    kept_domain VARCHAR(255),
                    kept_company_name VARCHAR(255),
                    kept_suggested_url TEXT,
                    deleted_entries JSONB,
                    skip_session_id VARCHAR(100),
                    executed_by VARCHAR(255) DEFAULT 'admin'
                );
            """)

            # listed_companies: 公式サイトURL カラム追加
            cur.execute(
                "ALTER TABLE listed_companies ADD COLUMN IF NOT EXISTS "
                "website_url TEXT NOT NULL DEFAULT '';"
            )
            # listed_companies: URL 死活監視用カラム
            cur.execute(
                "ALTER TABLE listed_companies ADD COLUMN IF NOT EXISTS "
                "url_status TEXT NOT NULL DEFAULT 'unchecked';"
            )
            cur.execute(
                "ALTER TABLE listed_companies ADD COLUMN IF NOT EXISTS "
                "url_checked_at TIMESTAMPTZ;"
            )
            # 既存 domain_overrides の URL を listed_companies に反映する
            # （一回限りのマイグレーション: website_url が空のレコードだけ更新）
            cur.execute(
                "UPDATE listed_companies "
                "SET website_url = d.suggested_url "
                "FROM domain_overrides d "
                "WHERE listed_companies.company_name = d.company_name "
                "AND listed_companies.website_url = '' "
                "AND d.suggested_url <> '';"
            )

            # ADMIN_EMAIL で指定されたユーザーを管理者に設定
            admin_email = os.environ.get("ADMIN_EMAIL", "").lower().strip()
            if admin_email:
                cur.execute(
                    "UPDATE users SET is_admin = TRUE WHERE email = %s",
                    (admin_email,)
                )

            # データ修正: 愛三工業のかな読み
            cur.execute(
                "UPDATE listed_companies SET company_name_kana = 'あいさんこうぎょう' "
                "WHERE company_name = '愛三工業';"
            )
            cur.execute(
                "UPDATE companies SET name_kana = 'あいさんこうぎょう' "
                "WHERE name = '愛三工業';"
            )


# ============================================================
# Users
# ============================================================

def _hash_pw(password: str, salt: str) -> str:
    """レガシー（SHA256+salt）ハッシュ関数。既存ユーザー検証用に残す。"""
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def _hash_pw_bcrypt(password: str) -> str:
    """bcrypt でパスワードハッシュを生成する。新規登録・パスワード変更用。"""
    return _bcrypt.hashpw(password.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")


def _is_bcrypt_hash(h: str) -> bool:
    """bcrypt標準プレフィックス ($2a$, $2b$, $2y$) を検出"""
    return bool(h) and h.startswith("$2")


def _upgrade_password_hash_to_bcrypt(user_id: int, password: str) -> None:
    """レガシー検証成功時に bcrypt ハッシュへ自動アップグレード"""
    new_hash = _hash_pw_bcrypt(password)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash = %s, salt = %s WHERE id = %s",
                (new_hash, "", user_id),
            )


def create_user(email: str, password: str, plan: str = "basic",
                last_name: str = "", first_name: str = "") -> int:
    """新規ユーザーを作成して user_id を返す（パスワードは bcrypt で保存）"""
    pw_hash = _hash_pw_bcrypt(password)
    admin_email = os.environ.get("ADMIN_EMAIL", "").lower().strip()
    is_admin = bool(admin_email and email.lower() == admin_email)
    if plan not in ("basic", "business", "pro"):
        plan = "basic"
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (email, password_hash, salt, is_admin, plan, last_name, first_name) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (email.lower(), pw_hash, "", is_admin, plan,
                 (last_name or "").strip(), (first_name or "").strip())
            )
            return cur.fetchone()[0]


def get_salutation_for_email(email: str) -> str:
    """メール本文冒頭の宛名文字列を返す。
    users.last_name と users.first_name が両方非空なら「{last_name} {first_name} 様」、
    どちらか空 / ユーザー未登録なら「{email} 様」を返す。
    email が空なら空文字を返す。
    """
    if not email:
        return ""
    email_norm = email.lower().strip()
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_name, first_name FROM users WHERE email = %s",
                    (email_norm,)
                )
                row = cur.fetchone()
    except Exception:
        row = None
    if row:
        last = (row[0] or "").strip()
        first = (row[1] or "").strip()
        if last and first:
            return f"{last} {first} 様"
    return f"{email} 様"


def upsert_line_pending_link(line_user_id: str, code: str):
    """LINE follow 時に発行した連携コードを upsert する（既存コードがあれば上書き）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO line_pending_links (line_user_id, code) "
                "VALUES (%s, %s) "
                "ON CONFLICT (line_user_id) DO UPDATE "
                "SET code = EXCLUDED.code, created_at = NOW()",
                (line_user_id, code),
            )


def consume_line_pending_link(code: str, expiry_minutes: int = 30) -> str:
    """code に一致する未期限切れの line_user_id を返し、該当レコードを削除する。
    見つからなければ空文字。複数一致時は最新を採用。
    """
    from datetime import datetime, timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=expiry_minutes)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM line_pending_links "
                "WHERE line_user_id = ("
                "  SELECT line_user_id FROM line_pending_links "
                "  WHERE code = %s AND created_at > %s "
                "  ORDER BY created_at DESC LIMIT 1"
                ") "
                "RETURNING line_user_id",
                (code, cutoff),
            )
            row = cur.fetchone()
            return row[0] if row else ""


def update_user_line_id(user_id: int, line_user_id: str):
    """users.line_user_id を更新する。空文字は連携解除扱い。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET line_user_id = %s WHERE id = %s",
                (line_user_id or "", user_id),
            )


def update_user_profile(user_id: int, company_name: str, industry: str,
                        company_size: str, job_type: str = "", job_title: str = ""):
    """プロフィール項目を更新する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET company_name=%s, industry=%s, company_size=%s, "
                "job_type=%s, job_title=%s WHERE id=%s",
                (company_name or None, industry or None, company_size or None,
                 job_type or None, job_title or None, user_id),
            )


def update_slack_webhook_url(user_id: int, webhook_url: str):
    """users.slack_webhook_url を更新する。空文字は解除扱い。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET slack_webhook_url = %s WHERE id = %s",
                (webhook_url or "", user_id),
            )


def update_user_plan(user_id: int, plan: str) -> str:
    """users.plan を更新し、変更前の値を返す。無効な plan は 'basic' に正規化する。"""
    if plan not in ("basic", "business", "pro"):
        plan = "basic"
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT plan FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
            old_plan = row[0] if row else ""
            cur.execute("UPDATE users SET plan = %s WHERE id = %s", (plan, user_id))
    return old_plan


def get_user_by_email(email: str):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, password_hash, salt, is_admin, plan, slack_webhook_url, line_user_id, company_name, industry, job_type, job_title, company_size, is_active, last_name_kana, first_name_kana, phone FROM users WHERE email = %s",
                (email.lower(),)
            )
            row = cur.fetchone()
            return dict(row) if row else None


def get_user_by_id(user_id: int):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, password_hash, salt, is_admin, plan, slack_webhook_url, line_user_id, company_name, industry, job_type, job_title, company_size, is_active, last_name, first_name, last_name_kana, first_name_kana, phone, sports_filter, entertainment_filter FROM users WHERE id = %s",
                (user_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None


def verify_user_password(user: dict, password: str) -> bool:
    """パスワード検証。bcrypt と レガシー(SHA256+salt) 両方をサポート。
    レガシー形式で検証成功した場合は bcrypt に自動アップグレードする。"""
    stored = user.get("password_hash", "") or ""
    # 1) bcrypt 形式
    if _is_bcrypt_hash(stored):
        try:
            return _bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
        except Exception:
            return False
    # 2) レガシー SHA256 + salt
    try:
        h = _hash_pw(password, user.get("salt", "") or "")
        ok = _hmac.compare_digest(h, stored)
    except Exception:
        ok = False
    # 検証成功したら bcrypt に自動アップグレード
    if ok and user.get("id"):
        try:
            _upgrade_password_hash_to_bcrypt(user["id"], password)
        except Exception:
            pass
    return ok


def get_user_last_login(user_id: int):
    """last_login_at を返す（datetime or None）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_login_at FROM users WHERE id = %s",
                (user_id,)
            )
            row = cur.fetchone()
            return row[0] if row else None


def update_last_login(user_id: int):
    """ログイン成功時に last_login_at を現在時刻で更新する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET last_login_at = NOW() WHERE id = %s",
                (user_id,)
            )


def update_last_active(user_id: int):
    """prev_active_at に現在の last_active_at をコピーし、last_active_at を現在時刻で更新する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET prev_active_at = last_active_at, last_active_at = NOW() WHERE id = %s",
                (user_id,)
            )


def get_user_last_active(user_id: int):
    """last_active_at を返す（datetime or None）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_active_at FROM users WHERE id = %s",
                (user_id,)
            )
            row = cur.fetchone()
            return row[0] if row else None


def get_user_prev_active(user_id: int):
    """prev_active_at を返す（datetime or None）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT prev_active_at FROM users WHERE id = %s",
                (user_id,)
            )
            row = cur.fetchone()
            return row[0] if row else None


def get_all_users_detail() -> list:
    """管理者用: プロフィール情報付きユーザー一覧（作成日降順）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, last_name, first_name, last_name_kana, first_name_kana, phone, "
                "plan, is_active, company_name, industry, company_size, job_type, job_title, created_at "
                "FROM users ORDER BY created_at DESC"
            )
            return [dict(r) for r in cur.fetchall()]


def toggle_user_active(user_id: int) -> bool | None:
    """is_active を反転し、新しい値を返す。該当ユーザー無しなら None。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET is_active = NOT COALESCE(is_active, TRUE) "
                "WHERE id = %s RETURNING is_active",
                (user_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def update_user_password(user_id: int, new_password: str):
    """パスワードを更新する（bcrypt で保存）"""
    pw_hash = _hash_pw_bcrypt(new_password)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash = %s, salt = %s WHERE id = %s",
                (pw_hash, "", user_id)
            )


def update_user_email(user_id: int, new_email: str):
    """メールアドレスを更新する（小文字正規化）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET email = %s WHERE id = %s",
                (new_email.lower(), user_id)
            )


def get_all_users() -> list:
    """管理者用: 全ユーザー一覧（各種件数付き）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT u.id, u.email, u.is_admin, u.plan, u.created_at,
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
                cur.execute(
                    "SELECT id, url, name, COALESCE(enabled, TRUE) AS enabled "
                    "FROM sites WHERE user_id = %s ORDER BY id",
                    (user_id,),
                )
            else:
                cur.execute(
                    "SELECT id, url, name, COALESCE(enabled, TRUE) AS enabled "
                    "FROM sites ORDER BY id"
                )
            return [dict(row) for row in cur.fetchall()]


def delete_site_by_url(user_id: int, url: str) -> bool:
    """URLで指定したサイトを削除する。削除できた場合 True を返す。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sites WHERE user_id = %s AND url = %s",
                (user_id, url),
            )
            return cur.rowcount > 0


def toggle_site_enabled(user_id: int, url: str):
    """サイトの enabled を反転して、更新後の値を返す。該当なし時は None。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sites SET enabled = NOT COALESCE(enabled, TRUE) "
                "WHERE user_id = %s AND url = %s RETURNING enabled",
                (user_id, url),
            )
            row = cur.fetchone()
            return bool(row[0]) if row else None


def load_sites_for_monitor() -> list:
    """
    monitor.py 用: user_id が設定済みのサイトだけモニターする。

    以前の JSON 移行（sites.json）由来で `user_id` が NULL になっている行がある場合、
    それらをモニター対象から除外してダッシュボード登録分だけをモニターできるようにする。
    """
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT s.url, s.name, s.user_id, s.company_id, "
                "COALESCE(s.enabled, TRUE) AS enabled, "
                "COALESCE(c.notify_enabled, TRUE) AS company_notify_enabled, "
                "COALESCE(s.max_pages, 1) AS max_pages "
                "FROM sites s "
                "LEFT JOIN companies c ON c.id = s.company_id "
                "WHERE s.user_id IS NOT NULL ORDER BY s.id"
            )
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


def update_site_url_and_name(user_id: int, old_url: str, new_url: str, name: str,
                             max_pages: int = 1) -> bool:
    """サイトのURLと名前を更新する。URL変更時は更新履歴・保存コンテンツをリセットする。"""
    if max_pages not in (1, 2, 3, 5, 10):
        max_pages = 1
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sites SET url = %s, name = %s, max_pages = %s WHERE user_id = %s AND url = %s",
                (new_url, name, max_pages, user_id, old_url)
            )
            if cur.rowcount == 0:
                return False
            if old_url != new_url:
                cur.execute("DELETE FROM content_store WHERE url = %s", (old_url,))
                cur.execute("DELETE FROM change_history WHERE url = %s", (old_url,))
            return True


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
    _nul = lambda s: s.replace("\x00", "") if isinstance(s, str) else s
    with _conn() as conn:
        with conn.cursor() as cur:
            for url, info in log.get("last_checks", {}).items():
                cur.execute(
                    "INSERT INTO last_checks (url, timestamp, status, error) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (url) DO UPDATE SET "
                    "timestamp = EXCLUDED.timestamp, status = EXCLUDED.status, error = EXCLUDED.error",
                    (_nul(url), _nul(info.get("timestamp", "")),
                     _nul(info.get("status", "unknown")), _nul(info.get("error", "")))
                )
            for entry in log.get("change_history", []):
                _diff_json = json.dumps(entry.get("diff", []), ensure_ascii=False).replace("\x00", "")
                cur.execute(
                    "INSERT INTO change_history (timestamp, url, name, diff) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (timestamp, url) DO NOTHING",
                    (_nul(entry.get("timestamp", "")), _nul(entry.get("url", "")),
                     _nul(entry.get("name", "")), _diff_json)
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
                try:
                    safe_content = (content or "").replace("\x00", "")[:30000]
                    cur.execute(
                        "INSERT INTO content_store (url, content) VALUES (%s, %s) "
                        "ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content",
                        (url, safe_content)
                    )
                except Exception as e:
                    print(f"[save_content_store] failed for url={url}: {e}")
                    continue


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
                    "WHERE user_id = %s ORDER BY sort_order, id",
                    (user_id,),
                )
            else:
                cur.execute(
                    "SELECT keyword, COALESCE(notify_enabled, TRUE) FROM keywords ORDER BY sort_order, id"
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
    """バックグラウンド用: [(user_id, keyword, notify_enabled, keyword_id, company_id), ...]
    user_id が NULL の孤立キーワードは除外する。
    追加フィールドは末尾に並べて、旧 3-tuple / 4-tuple の位置互換を維持する。
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, keyword, COALESCE(notify_enabled, TRUE), id, company_id "
                "FROM keywords WHERE user_id IS NOT NULL ORDER BY sort_order, id"
            )
            return [
                (row[0], row[1], bool(row[2]), row[3], row[4])
                for row in cur.fetchall()
            ]


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


def count_unread_articles(user_id: int) -> int:
    """ユーザーの未読記事数をDBから直接カウントする（上限なし）。
    注意: 重複排除（_deduplicate_articles）は反映されない。UI向けには
    重複排除済みリストからカウントすること。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM articles WHERE user_id = %s AND is_read = FALSE",
                (user_id,),
            )
            return cur.fetchone()[0]



def load_articles_data(user_id=None, hide_sports: bool = False, hide_entertainment: bool = False) -> dict:
    from datetime import datetime, timedelta, timezone
    jst = timezone(timedelta(hours=9))
    cutoff = (datetime.now(jst) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    pub_cutoff = (datetime.now(jst) - timedelta(days=7)).strftime("%Y-%m-%d")
    sports_clause = "AND COALESCE(a.is_sports, FALSE) = FALSE " if hide_sports else ""
    ent_clause = "AND COALESCE(a.is_entertainment, FALSE) = FALSE " if hide_entertainment else ""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if user_id is not None:
                cur.execute(
                    "SELECT a.id, a.keyword, a.title, a.url, a.source, a.published, a.found_at, "
                    "a.is_read, a.date_verified, a.duplicate_count, a.importance, a.summary, "
                    "a.primary_company_id, pc.name AS primary_company_name "
                    "FROM articles a "
                    "INNER JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword "
                    "LEFT JOIN companies pc ON pc.id = a.primary_company_id "
                    "WHERE a.user_id = %s AND a.is_representative = TRUE "
                    "AND a.found_at >= %s "
                    "AND (a.published = '' OR REPLACE(a.published, '~', '') >= %s) "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM excluded_sources es "
                    "  WHERE es.user_id = a.user_id AND es.source_name = a.source"
                    ") "
                    + sports_clause + ent_clause +
                    "ORDER BY "
                    "CASE WHEN a.importance='high' THEN 0 "
                    "     WHEN a.importance='medium' THEN 1 ELSE 2 END, "
                    "a.found_at DESC LIMIT 3000",
                    (user_id, cutoff, pub_cutoff)
                )
                articles = [dict(row) for row in cur.fetchall()]
            else:
                cur.execute(
                    "SELECT a.id, a.keyword, a.title, a.url, a.source, a.published, a.found_at, "
                    "a.is_read, a.date_verified, a.duplicate_count, a.importance, a.summary, "
                    "a.primary_company_id, pc.name AS primary_company_name "
                    "FROM articles a "
                    "LEFT JOIN companies pc ON pc.id = a.primary_company_id "
                    "WHERE a.is_representative = TRUE "
                    "AND a.found_at >= %s "
                    "AND (a.published = '' OR REPLACE(a.published, '~', '') >= %s) "
                    "ORDER BY "
                    "CASE WHEN a.importance='high' THEN 0 "
                    "     WHEN a.importance='medium' THEN 1 ELSE 2 END, "
                    "a.found_at DESC LIMIT 3000",
                    (cutoff, pub_cutoff)
                )
                articles = [dict(row) for row in cur.fetchall()]
    return {"articles": articles}


def insert_articles(articles: list, user_id: int):
    """新着記事のみDBに登録する（URL・タイトル重複は ON CONFLICT で無視）"""
    if not articles:
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            for article in articles:
                importance = article.get("importance", "low")
                if importance not in ("high", "medium", "low"):
                    importance = "low"
                cur.execute(
                    "INSERT INTO articles "
                    "(keyword, title, url, source, published, found_at, user_id, is_read, date_verified, importance, summary, primary_company_id, is_sports, is_entertainment) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (article.get("keyword", ""), article.get("title", ""), article.get("url", ""),
                     article.get("source", ""), article.get("published", ""),
                     article.get("found_at", ""), user_id,
                     bool(article.get("date_verified", False)),
                     importance,
                     article.get("summary", ""),
                     article.get("primary_company_id"),
                     bool(article.get("is_sports", False)),
                     bool(article.get("is_entertainment", False)))
                )


def load_articles_for_grouping(user_id: int, days: int = 7) -> list:
    """直近 days 日間の記事を id 昇順（古い順）で返す。重複グルーピング処理の対象取得用。"""
    from datetime import datetime, timedelta, timezone
    jst = timezone(timedelta(hours=9))
    cutoff = (datetime.now(jst) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, title, group_id, is_representative, duplicate_count "
                "FROM articles "
                "WHERE user_id = %s AND found_at != '' AND found_at >= %s "
                "ORDER BY id ASC",
                (user_id, cutoff),
            )
            return [dict(r) for r in cur.fetchall()]


def set_article_as_representative(article_id: int):
    """記事を代表にする（group_id を自身の id に、is_representative を TRUE に揃える）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET group_id = id, is_representative = TRUE WHERE id = %s",
                (article_id,),
            )


def add_duplicate_to_group(dup_id: int, rep_id: int):
    """重複記事を代表のグループに紐付け、代表の duplicate_count をインクリメントする"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET group_id = %s, is_representative = FALSE WHERE id = %s",
                (rep_id, dup_id),
            )
            cur.execute(
                "UPDATE articles SET duplicate_count = duplicate_count + 1 WHERE id = %s",
                (rep_id,),
            )


def count_articles_by_keyword(user_id: int, keyword: str) -> int:
    """指定キーワードの収集記事数を返す"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM articles WHERE user_id = %s AND keyword = %s",
                (user_id, keyword),
            )
            return cur.fetchone()[0]


def delete_articles_by_keyword(user_id: int, keyword: str):
    """指定キーワードに紐づく記事を全件削除する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM articles WHERE user_id = %s AND keyword = %s",
                (user_id, keyword),
            )


def delete_keyword_by_text(user_id: int, keyword: str) -> bool:
    """ユーザーの指定キーワードレコードを削除する（text 一致）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM keywords WHERE user_id=%s AND keyword=%s",
                (user_id, keyword),
            )
            return cur.rowcount > 0


def fix_tdnet_company_names() -> int:
    """一時関数: tdnet_disclosures.company_name の文字間スペース（半角/全角）を除去。
    既存データの一括修正用。完了後は本関数を削除してよい。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tdnet_disclosures "
                "SET company_name = REPLACE(REPLACE(company_name, ' ', ''), '　', '') "
                "WHERE company_name LIKE '% %' OR company_name LIKE '%　%'"
            )
            n = cur.rowcount
            logger.info("[fix_tdnet_company_names] updated=%d", n)
            return n


def fetch_and_save_tdnet() -> list:
    """やのしんAPIから最新100件の TDnet 適時開示を取得し、tdnet_disclosures に保存する。
    重複（document_id 一致）は ON CONFLICT DO NOTHING でスキップ。
    返り値: 今回新規保存した document_id の list。
    HTTPエラー / タイムアウト / 空レスポンス / JSON解析失敗 は TdnetFetchError を送出する。"""
    import requests as _requests
    url = "https://webapi.yanoshin.jp/webapi/tdnet/list/recent.json?limit=100"
    try:
        resp = _requests.get(url, timeout=15)
    except _requests.Timeout as e:
        logger.error("[tdnet] timeout: %s", e)
        raise TdnetFetchError(f"timeout: {e}")
    except _requests.RequestException as e:
        logger.error("[tdnet] request error: %s", e)
        raise TdnetFetchError(f"request error: {e}")
    if resp.status_code >= 400:
        logger.error("[tdnet] HTTP %s", resp.status_code)
        raise TdnetFetchError(f"HTTP {resp.status_code}")
    if not resp.content:
        logger.error("[tdnet] empty response")
        raise TdnetFetchError("empty response")
    try:
        data = resp.json()
    except ValueError as e:
        logger.error("[tdnet] JSON parse failed: %s", e)
        raise TdnetFetchError(f"JSON parse failed: {e}")
    if not isinstance(data, dict):
        raise TdnetFetchError("unexpected response shape")
    items = data.get("items") or []
    saved_ids: list = []

    import re as _re

    def clean(s):
        """NULL文字のみ除去（title / doc_url / pubdate / doc_id 用）"""
        if not isinstance(s, str):
            return ""
        return s.replace("\x00", "")

    def clean_company(s):
        """company_name 用。NULL文字 + 各種空白類（全角/ゼロ幅含む）を全除去"""
        if not isinstance(s, str):
            return ""
        s = s.replace("\x00", "")
        # \s はASCII空白＋Unicode空白をカバー。加えて明示的に特殊空白類も指定
        s = _re.sub(r"[\s\u3000\u00a0\u200b\u200c\u200d\ufeff]+", "", s)
        return s.strip()

    with _conn() as conn:
        with conn.cursor() as cur:
            for item in items:
                t = item.get("Tdnet") or {}
                doc_id = clean(str(t.get("id") or "")).strip()
                company = clean_company(t.get("company_name") or "")
                title = clean(t.get("title") or "").strip()
                pubdate = clean(t.get("pubdate") or "").strip()
                doc_url = clean(t.get("document_url") or "").strip()
                sec_code = clean(str(t.get("company_code") or "")).strip()
                # TDnet 5桁形式 → JPX 4桁に正規化（末尾0を除く）。
                # 「60000」のようなコードを rstrip('0') で壊さないよう、
                # 長さ5 かつ末尾 '0' のときだけ先頭4文字にする。
                if len(sec_code) == 5 and sec_code.endswith("0"):
                    sec_code = sec_code[:-1]
                if not (doc_id and company and title and pubdate and doc_url):
                    continue
                try:
                    cur.execute(
                        "INSERT INTO tdnet_disclosures "
                        "(document_id, company_name, title, disclosed_at, document_url, securities_code) "
                        "VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (document_id) DO NOTHING",
                        (doc_id, company, title, pubdate, doc_url, sec_code or None),
                    )
                    if cur.rowcount > 0:
                        saved_ids.append(doc_id)
                    elif sec_code:
                        # 既存レコード: securities_code が未設定なら埋め直す（NULL のレコードに対する後方互換）
                        cur.execute(
                            "UPDATE tdnet_disclosures SET securities_code = %s "
                            "WHERE document_id = %s AND (securities_code IS NULL OR securities_code = '')",
                            (sec_code, doc_id),
                        )
                except Exception as e:
                    logger.warning("[tdnet] insert skipped doc_id=%s err=%s", doc_id, e)
    logger.info("[tdnet] fetched=%d saved=%d", len(items), len(saved_ids))
    return saved_ids


def get_tdnet_by_document_ids(doc_ids: list) -> list:
    """document_id のリストから tdnet_disclosures を取得する（通知用）"""
    if not doc_ids:
        return []
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT document_id, company_name, title, disclosed_at, document_url, securities_code "
                "FROM tdnet_disclosures WHERE document_id = ANY(%s) "
                "ORDER BY disclosed_at DESC",
                (doc_ids,),
            )
            return [dict(r) for r in cur.fetchall()]


def get_tdnet_by_securities_code(code: str, limit: int = 10) -> list:
    """証券コードに紐づく TDnet 開示情報を disclosed_at 降順で返す（企業詳細用）"""
    if not code:
        return []
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT document_id, company_name, title, disclosed_at, document_url "
                "FROM tdnet_disclosures WHERE securities_code = %s "
                "ORDER BY disclosed_at DESC LIMIT %s",
                (code, limit),
            )
            return [dict(r) for r in cur.fetchall()]


def get_pro_users() -> list:
    """plan = 'pro' の全ユーザーを返す（通知用）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email FROM users WHERE plan = 'pro' AND email <> ''"
            )
            return [dict(r) for r in cur.fetchall()]


def get_admin_users() -> list:
    """is_admin = TRUE の全ユーザーを返す（通知用）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email FROM users WHERE is_admin = TRUE AND email <> ''"
            )
            return [dict(r) for r in cur.fetchall()]


# ---- system_status ----
def set_system_status(key: str, value: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO system_status (key, value, updated_at) "
                "VALUES (%s, %s, NOW()) "
                "ON CONFLICT (key) DO UPDATE "
                "SET value = EXCLUDED.value, updated_at = NOW()",
                (key, value),
            )


def get_system_status(key: str) -> str | None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM system_status WHERE key = %s", (key,)
            )
            row = cur.fetchone()
            return row[0] if row else None


class TdnetFetchError(Exception):
    """TDnet API 取得時のエラーを示す例外"""
    pass


JPX_XLS_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


def fetch_and_save_securities_master() -> int:
    """JPX上場銘柄一覧の XLS を取得して securities_master に保存する。
    ON CONFLICT(code) DO UPDATE で更新。取得件数を返す。"""
    import requests as _requests
    import xlrd as _xlrd

    resp = _requests.get(JPX_XLS_URL, timeout=60)
    resp.raise_for_status()
    wb = _xlrd.open_workbook(file_contents=resp.content)
    sh = wb.sheet_by_index(0)
    saved = 0
    with _conn() as conn:
        with conn.cursor() as cur:
            for i in range(1, sh.nrows):
                row = sh.row_values(i)
                if len(row) < 3:
                    continue
                # code は float の場合があるので int 経由で文字列化
                raw_code = row[1]
                if isinstance(raw_code, float):
                    code = str(int(raw_code))
                else:
                    code = str(raw_code).strip()
                name = str(row[2]).strip() if len(row) > 2 else ""
                market = str(row[3]).strip() if len(row) > 3 else ""
                if not code or not name:
                    continue
                try:
                    cur.execute(
                        "INSERT INTO securities_master (code, name, market, updated_at) "
                        "VALUES (%s, %s, %s, NOW()) "
                        "ON CONFLICT (code) DO UPDATE SET "
                        "name = EXCLUDED.name, market = EXCLUDED.market, updated_at = NOW()",
                        (code, name, market),
                    )
                    saved += 1
                except Exception as e:
                    logger.warning("[securities_master] skip code=%s err=%s", code, e)
    logger.info("[securities_master] saved=%d", saved)
    return saved


def lookup_securities_master_by_code(code: str) -> list:
    """securities_master から code LIKE '%code%' で企業名を最大5件返す"""
    if not code:
        return []
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT name FROM securities_master "
                "WHERE code LIKE %s ORDER BY name LIMIT 5",
                (f"%{code}%",),
            )
            return [r[0] for r in cur.fetchall() if r[0]]


def get_tdnet_for_user(user_id: int) -> list:
    """ユーザーの登録企業に紐づく TDnet 開示情報を disclosed_at 降順で返す。
      ステップ1 (最優先): companies.securities_code が登録されている企業は、
         tdnet_disclosures.securities_code との完全一致で照合する。
      ステップ2 (フォールバック): securities_code が未登録の企業のみ、
         company_name の部分一致 ('%name%') で照合する。
         誤ヒット防止のため name 長が 2 以下の企業名はスキップする。
      ステップ3: document_id で重複排除し、disclosed_at 降順で最大500件返す。
    """
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT name, securities_code FROM companies "
                "WHERE user_id = %s AND name <> ''",
                (user_id,),
            )
            rows = cur.fetchall()
            if not rows:
                return []

            codes: list = []      # 証券コード登録済みの企業
            names_no_code: list = []  # 証券コード未登録の企業（長さ>2 のみ）
            for r in rows:
                code = (r["securities_code"] or "").strip()
                name = r["name"] or ""
                if code:
                    codes.append(code)
                elif len(name) > 2:
                    names_no_code.append(name)

            logger.info(
                "[tdnet] user_id=%s codes=%s names_no_code=%s",
                user_id, codes, names_no_code,
            )

            merged: dict = {}

            # ステップ1: 証券コード完全一致
            if codes:
                placeholders = ",".join(["%s"] * len(codes))
                cur.execute(
                    "SELECT document_id, company_name, title, disclosed_at, document_url "
                    f"FROM tdnet_disclosures WHERE securities_code IN ({placeholders}) "
                    "ORDER BY disclosed_at DESC LIMIT 500",
                    codes,
                )
                found = cur.fetchall()
                for r in found:
                    d = dict(r)
                    merged.setdefault(d["document_id"], d)
                logger.info("[tdnet] code-exact-match codes=%s got=%d", codes, len(found))

            # ステップ2: 名前部分一致（コード未登録かつ長さ>2 の企業のみ）
            if names_no_code:
                where_parts = " OR ".join(["company_name LIKE %s"] * len(names_no_code))
                params = [f"%{n}%" for n in names_no_code]
                cur.execute(
                    "SELECT document_id, company_name, title, disclosed_at, document_url "
                    f"FROM tdnet_disclosures WHERE {where_parts} "
                    "ORDER BY disclosed_at DESC LIMIT 500",
                    params,
                )
                name_hits = 0
                for r in cur.fetchall():
                    d = dict(r)
                    if d["document_id"] not in merged:
                        merged[d["document_id"]] = d
                        name_hits += 1
                logger.info(
                    "[tdnet] name-partial-match patterns=%s new=%d",
                    names_no_code, name_hits,
                )

            # ステップ3: マージ結果を disclosed_at DESC でソートして最大500件
            results = sorted(
                merged.values(),
                key=lambda r: r.get("disclosed_at") or "",
                reverse=True,
            )[:500]
            logger.info("[tdnet] user_id=%s found=%d (merged)", user_id, len(results))
            return results


def delete_old_articles(days: int = 30) -> int:
    """found_at が指定日数より古い記事を articles テーブルから削除する。
    返り値: 削除件数。found_at が空文字のレコードは削除対象外。
    注: change_history など他テーブルは一切影響を受けない。"""
    from datetime import datetime, timedelta, timezone
    jst = timezone(timedelta(hours=9))
    cutoff = (datetime.now(jst) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM articles WHERE found_at != '' AND found_at < %s",
                (cutoff,),
            )
            return cur.rowcount


def mark_article_read(user_id: int, article_id: int) -> bool:
    """記事を既読にする（同一URLの全行を更新）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET is_read = TRUE "
                "WHERE user_id = %s AND url = ("
                "  SELECT url FROM articles WHERE id = %s AND user_id = %s"
                ")",
                (user_id, article_id, user_id),
            )
            return cur.rowcount > 0


def mark_article_unread(user_id: int, article_id: int) -> bool:
    """記事を未読に戻す（同一URLの全行を更新）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET is_read = FALSE "
                "WHERE user_id = %s AND url = ("
                "  SELECT url FROM articles WHERE id = %s AND user_id = %s"
                ")",
                (user_id, article_id, user_id),
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
                "UPDATE running_tasks SET completed_at = NOW(), error_message = NULL "
                "WHERE task_type = %s AND key = %s",
                (task_type, key)
            )


def fail_running_task(task_type: str, key: str, error_message: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE running_tasks SET completed_at = NOW(), error_message = %s "
                "WHERE task_type = %s AND key = %s",
                (error_message[:500], task_type, key)
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


def get_user_notify_timing(user_id: int) -> str:
    """ユーザーのダイジェスト送信タイミング設定を返す (digest_07 等)"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(notify_timing, 'digest_07') FROM users WHERE id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            return row[0] if row else "digest_07"


_VALID_TIMINGS = {
    "digest_05", "digest_06", "digest_07", "digest_08",
    "digest_09", "digest_16", "digest_17", "digest_18",
}


def get_user_notify_days(user_id: int) -> str:
    """ユーザーの通知曜日設定を返す（カンマ区切り、例: '1,2,3,4,5'）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(notify_days, '0,1,2,3,4,5,6') FROM users WHERE id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            return row[0] if row else "0,1,2,3,4,5,6"


def set_user_notify_days(user_id: int, days: str) -> bool:
    """ユーザーの通知曜日設定を更新する。days は '0,1,2,3,4,5' のようなカンマ区切り文字列。"""
    values = [v.strip() for v in days.split(",") if v.strip()]
    valid = {"0", "1", "2", "3", "4", "5", "6"}
    if any(v not in valid for v in values):
        return False
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET notify_days = %s WHERE id = %s",
                (",".join(values), user_id),
            )
            return cur.rowcount > 0


def set_user_notify_timing(user_id: int, timing: str) -> bool:
    """ユーザーのダイジェスト送信タイミングを更新する。
    timing はカンマ区切り文字列（例: "digest_05,digest_18"）。
    """
    values = [v.strip() for v in timing.split(",") if v.strip()]
    if not values:
        return False
    if any(v not in _VALID_TIMINGS for v in values):
        return False
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET notify_timing = %s WHERE id = %s",
                (timing, user_id),
            )
            return cur.rowcount > 0


# ---- ダッシュボードカード表示設定 ----
def _default_dashboard_settings(user_row: dict, saved: dict = None) -> dict:
    """デフォルト設定を返す。saved があれば定義済みキーのみ上書きする。"""
    is_pro = (user_row or {}).get("plan") == "pro"
    default_order = ["today_companies", "prev_companies", "alert", "unread"]
    if is_pro:
        default_order += ["tdnet_today", "tdnet_prev"]
    defaults = {
        "card_count": 4,
        "card_order": default_order,
        "card_visible": ["today_companies", "prev_companies", "alert", "unread"],
    }
    if not saved:
        return defaults
    result = defaults.copy()
    result.update({k: v for k, v in saved.items() if k in defaults})
    return result


def get_dashboard_settings(user_id: int) -> dict:
    """ユーザーのダッシュボード設定を返す。未設定時はデフォルト値を返す。"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT dashboard_settings, plan FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return _default_dashboard_settings(row)
            return _default_dashboard_settings(row, row["dashboard_settings"])


def save_dashboard_settings(user_id: int, settings: dict) -> None:
    """ダッシュボード設定を保存する。"""
    import json
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET dashboard_settings=%s WHERE id=%s",
                (json.dumps(settings), user_id),
            )


def get_users_for_digest_hour(hour: int) -> list:
    """指定時刻（整数）のダイジェスト対象ユーザー ID 一覧を返す。
    notify_timing カラムに "digest_HH" 形式の文字列が含まれるユーザーを返す。
    """
    key = f"digest_{hour:02d}"
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM users WHERE STRPOS(notify_timing, %s) > 0",
                (key,),
            )
            return [row[0] for row in cur.fetchall()]


def load_unnotified_articles(user_id: int, hide_sports: bool = False, hide_entertainment: bool = False) -> list:
    """未通知（notified_at IS NULL）の記事を返す。
    企業通知OFF（companies.notify_enabled=FALSE）の記事は除外する。
    company_id=NULL のキーワードは従来通り含める。
    """
    sports_clause = "AND COALESCE(a.is_sports, FALSE) = FALSE " if hide_sports else ""
    ent_clause = "AND COALESCE(a.is_entertainment, FALSE) = FALSE " if hide_entertainment else ""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT a.id, a.keyword, a.title, a.url, a.source, a.published, "
                "       a.date_verified, "
                "       COALESCE(k.notify_enabled, TRUE) AS notify_enabled "
                "FROM articles a "
                "LEFT JOIN keywords k "
                "  ON k.user_id = a.user_id AND k.keyword = a.keyword "
                "LEFT JOIN companies c "
                "  ON c.id = k.company_id "
                "WHERE a.user_id = %s AND a.notified_at IS NULL "
                "AND (k.company_id IS NULL OR COALESCE(c.notify_enabled, TRUE) = TRUE) "
                + sports_clause + ent_clause +
                "ORDER BY a.published DESC, a.id DESC",
                (user_id,),
            )
            return [dict(row) for row in cur.fetchall()]


def mark_articles_notified_by_urls(user_id: int, urls: list):
    """指定URLの記事を通知済みにする（即時通知後）"""
    if not urls:
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET notified_at = NOW() "
                "WHERE user_id = %s AND url = ANY(%s) AND notified_at IS NULL",
                (user_id, urls),
            )


def mark_all_unnotified_notified(user_id: int):
    """全未通知記事を通知済みにする（ダイジェスト送信後）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET notified_at = NOW() "
                "WHERE user_id = %s AND notified_at IS NULL",
                (user_id,),
            )


def get_all_running_tasks() -> dict:
    """後方互換用: 実行中・完了猶予期間内タスクのキーセットを返す。"""
    statuses = get_running_task_statuses()
    return {task_type: set(keys.keys()) for task_type, keys in statuses.items()}


# ============================================================
# Alert Keywords
# ============================================================

def load_alert_keywords(user_id: int) -> list:
    """ユーザー全体のアラートキーワード一覧を返す（user-wide のみ）。
    企業単位の重要アラート（company_alert_keywords）は per-company 判定側で参照する。"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, keyword FROM alert_keywords WHERE user_id = %s ORDER BY id",
                (user_id,),
            )
            return [dict(row) for row in cur.fetchall()]


def get_all_company_alert_keywords_for_user(user_id: int) -> list:
    """ユーザーが所有する全企業の company_alert_keywords を一括取得する。
    返り値: [{id, company_id, keyword}, ...]
    per-article アラート判定で {company_id: set[lower_kw]} を組むための元データ。"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, company_id, keyword FROM company_alert_keywords "
                "WHERE user_id = %s ORDER BY company_id, id",
                (user_id,),
            )
            return [dict(row) for row in cur.fetchall()]


def get_user_keyword_company_id(user_id: int, keyword: str):
    """ユーザーの指定キーワードに紐づく company_id を返す（未紐づけ時は None）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT company_id FROM keywords WHERE user_id=%s AND keyword=%s LIMIT 1",
                (user_id, keyword),
            )
            row = cur.fetchone()
            return row[0] if row else None


def add_alert_keyword(user_id: int, keyword: str):
    """アラートキーワードを追加する。成功時は新規ID(int)、重複時は False を返す"""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO alert_keywords (user_id, keyword) VALUES (%s, %s) RETURNING id",
                    (user_id, keyword),
                )
                row = cur.fetchone()
        return row[0] if row else True
    except psycopg2.errors.UniqueViolation:
        return False


def delete_alert_keyword(user_id: int, keyword_id: int) -> bool:
    """アラートキーワードを削除する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM alert_keywords WHERE id = %s AND user_id = %s",
                (keyword_id, user_id),
            )
            return cur.rowcount > 0


def get_exclude_keywords(user_id: int) -> list:
    """除外キーワード一覧を返す"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, keyword FROM exclude_keywords WHERE user_id = %s ORDER BY id",
                (user_id,),
            )
            return [dict(row) for row in cur.fetchall()]


# ---- 企業単位の除外ワード ----
def get_company_exclude_words(company_id: int) -> list:
    """指定企業に紐づく除外ワード一覧（id, exclude_word）を返す"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, exclude_word FROM company_exclude_keywords "
                "WHERE company_id = %s ORDER BY id",
                (company_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def add_company_exclude_word(user_id: int, company_id: int, exclude_word: str):
    """企業単位の除外ワードを追加する。
    対象 company_id が user_id の所有であるか検証する。
    返り値: 成功時はID(int)、重複時は False、所有権エラー時は None。"""
    exclude_word = (exclude_word or "").strip()
    if not exclude_word:
        return False
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM companies WHERE id = %s AND user_id = %s",
                (company_id, user_id),
            )
            if not cur.fetchone():
                return None
            try:
                cur.execute(
                    "INSERT INTO company_exclude_keywords "
                    "(company_id, user_id, exclude_word) VALUES (%s, %s, %s) "
                    "RETURNING id",
                    (company_id, user_id, exclude_word),
                )
                row = cur.fetchone()
                return row[0] if row else True
            except psycopg2.errors.UniqueViolation:
                return False


def delete_company_exclude_word(user_id: int, company_id: int, exclude_word_id: int) -> bool:
    """企業単位の除外ワードを削除する（所有権確認込み）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM company_exclude_keywords "
                "WHERE id = %s AND company_id = %s AND user_id = %s",
                (exclude_word_id, company_id, user_id),
            )
            return cur.rowcount > 0


# ---- 企業単位の重要アラートキーワード ----
def get_company_alert_keywords(company_id: int) -> list:
    """指定企業に紐づく重要アラートキーワード一覧（id, keyword）を返す"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, keyword FROM company_alert_keywords "
                "WHERE company_id = %s ORDER BY id",
                (company_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def add_company_alert_keyword(user_id: int, company_id: int, keyword: str):
    """企業単位の重要アラートキーワードを追加する（所有権確認込み）。
    返り値: 成功時はID(int)、重複時は False、所有権エラー時は None。"""
    keyword = (keyword or "").strip()
    if not keyword:
        return False
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM companies WHERE id = %s AND user_id = %s",
                (company_id, user_id),
            )
            if not cur.fetchone():
                return None
            try:
                cur.execute(
                    "INSERT INTO company_alert_keywords "
                    "(company_id, user_id, keyword) VALUES (%s, %s, %s) "
                    "RETURNING id",
                    (company_id, user_id, keyword),
                )
                row = cur.fetchone()
                return row[0] if row else True
            except psycopg2.errors.UniqueViolation:
                return False


def delete_company_alert_keyword(user_id: int, company_id: int, keyword_id: int) -> bool:
    """企業単位の重要アラートキーワードを削除する（所有権確認込み）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM company_alert_keywords "
                "WHERE id = %s AND company_id = %s AND user_id = %s",
                (keyword_id, company_id, user_id),
            )
            return cur.rowcount > 0


def get_alert_keywords_set(user_id: int) -> set:
    """アラートキーワードを小文字セットで返す（マッチング用）"""
    rows = load_alert_keywords(user_id)
    return {r["keyword"].lower() for r in rows}


# ============================================================
# Domain Overrides
# ============================================================

def get_all_domain_overrides() -> list:
    """全ドメインオーバーライドを返す"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, domain, suggested_url, company_name, company_name_kana, "
                "COALESCE(is_exception, FALSE) AS is_exception, created_at "
                "FROM domain_overrides ORDER BY company_name_kana, domain"
            )
            return [dict(row) for row in cur.fetchall()]


def get_domain_overrides_dict() -> dict:
    """ドメインオーバーライドを {正規化domain: suggested_url} の辞書で返す (is_exception=FALSE のみ)"""
    rows = get_all_domain_overrides()
    result = {}
    for r in rows:
        if r.get("is_exception"):
            continue
        key = normalize_domain(r["domain"])
        if not key:
            continue
        url = r.get("suggested_url", "")
        if key not in result or not result[key]:
            result[key] = url
        elif url and not result[key]:
            result[key] = url
    return result


def get_domain_exceptions_dict() -> dict:
    """正規化例外エントリを {元ホスト名: suggested_url} の辞書で返す"""
    rows = get_all_domain_overrides()
    result = {}
    for r in rows:
        if not r.get("is_exception"):
            continue
        key = r["domain"].strip().lower()
        if not key:
            continue
        url = r.get("suggested_url", "")
        if key not in result or not result[key]:
            result[key] = url
    return result


def add_domain_override(domain: str, suggested_url: str,
                        company_name: str = "", company_name_kana: str = "",
                        is_exception: bool = False) -> dict:
    """ドメインオーバーライドを追加する"""
    if is_exception:
        dom = clean_hostname(domain)
    else:
        dom = normalize_domain(domain)
    if not dom:
        return {"id": None, "domain": "", "suggested_url": "", "error": "invalid_domain"}
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO domain_overrides (domain, suggested_url, company_name, company_name_kana, is_exception) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (domain) DO UPDATE SET suggested_url = EXCLUDED.suggested_url, "
                "company_name = EXCLUDED.company_name, company_name_kana = EXCLUDED.company_name_kana, "
                "is_exception = EXCLUDED.is_exception "
                "RETURNING id",
                (dom, suggested_url.strip(),
                 company_name.strip(), company_name_kana.strip(), is_exception),
            )
            row = cur.fetchone()
            return {"id": row[0], "domain": dom, "suggested_url": suggested_url.strip()}


def update_domain_override(override_id: int, domain: str, suggested_url: str,
                           company_name: str = "", company_name_kana: str = "",
                           is_exception: bool = False) -> bool:
    """ドメインオーバーライドを更新する"""
    if is_exception:
        dom = clean_hostname(domain)
    else:
        dom = normalize_domain(domain)
    if not dom:
        return False
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE domain_overrides SET domain = %s, suggested_url = %s, "
                "company_name = %s, company_name_kana = %s, is_exception = %s WHERE id = %s",
                (dom, suggested_url.strip(),
                 company_name.strip(), company_name_kana.strip(), is_exception, override_id),
            )
            return cur.rowcount > 0


def delete_domain_override(override_id: int) -> bool:
    """ドメインオーバーライドを削除する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM domain_overrides WHERE id = %s", (override_id,))
            return cur.rowcount > 0


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


def create_magic_token(user_id: int, ttl_minutes: int = 15) -> str:
    """マジックリンク用トークンを生成してDBに保存し、トークン文字列を返す。
    既存の未使用トークンは削除してから新規作成する。"""
    token = secrets.token_urlsafe(32)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM magic_tokens WHERE user_id = %s AND used_at IS NULL", (user_id,))
            cur.execute(
                "INSERT INTO magic_tokens (user_id, token, expires_at) "
                "VALUES (%s, %s, NOW() + (INTERVAL '1 minute' * %s))",
                (user_id, token, ttl_minutes),
            )
    return token


def consume_magic_token(token: str):
    """マジックリンクトークンを検証し、有効なら used_at をセットして user_id を返す。
    無効・期限切れ・使用済みの場合は None を返す。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, user_id FROM magic_tokens "
                "WHERE token = %s AND used_at IS NULL AND expires_at > NOW()",
                (token,),
            )
            row = cur.fetchone()
            if not row:
                return None
            token_id, user_id = row
            cur.execute(
                "UPDATE magic_tokens SET used_at = NOW() WHERE id = %s",
                (token_id,),
            )
            return user_id


# ============================================================
# Companies
# ============================================================

def count_active_companies_today(user_id: int) -> int:
    """当日 JST 0:00 以降に活動のあった企業数。
    company_id が明示的に設定されたサイト・キーワードのみ対象（過大計上しない）。

    タイムスタンプは monitor.py が datetime.now()（UTC）で保存した TEXT 文字列。
    日本時間（JST = UTC+9）の今日 0:00 を UTC に換算した文字列と比較することで、
    Render サーバーが UTC であっても JST 基準の「当日」を正確に判定する。
    例: JST 2026-04-04 00:00 → UTC 2026-04-03 15:00:00 → today_utc_str = "2026-04-03 15:00:00"
    """
    from datetime import datetime, timezone, timedelta
    jst = timezone(timedelta(hours=9))
    now_jst = datetime.now(jst)
    # JST 今日 0:00 を UTC 換算
    jst_midnight = now_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    utc_midnight = jst_midnight.astimezone(timezone.utc)
    today_utc_str = utc_midnight.strftime("%Y-%m-%d %H:%M:%S")

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT company_id) FROM (
                    SELECT s.company_id
                    FROM sites s
                    JOIN change_history ch ON ch.url = s.url
                    WHERE s.user_id = %s
                      AND s.company_id IS NOT NULL
                      AND ch.timestamp >= %s
                    UNION
                    SELECT k.company_id
                    FROM keywords k
                    JOIN articles a
                      ON a.user_id = k.user_id AND a.keyword = k.keyword
                    WHERE k.user_id = %s
                      AND k.company_id IS NOT NULL
                      AND a.found_at >= %s
                ) sub
                """,
                (user_id, today_utc_str, user_id, today_utc_str),
            )
            row = cur.fetchone()
            return row[0] if row else 0


def load_active_companies_today(user_id: int) -> list:
    """当日 JST 0:00 以降に活動のあった企業を返す。
    各企業に changes（最大3件）と news_count を付与する。
    """
    from datetime import datetime, timezone, timedelta
    jst = timezone(timedelta(hours=9))
    now_jst = datetime.now(jst)
    jst_midnight = now_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    # change_history.timestamp は JST 文字列で保存されているため JST 基準で比較
    jst_midnight_str = jst_midnight.strftime("%Y-%m-%d %H:%M:%S")
    # articles.found_at は UTC 基準のため UTC 変換
    utc_midnight_str = jst_midnight.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # ── 1. 本日活動した企業を取得 ──
            cur.execute(
                """
                SELECT DISTINCT c.id, c.name
                FROM companies c
                WHERE c.user_id = %s
                  AND c.id IN (
                    SELECT s.company_id
                    FROM sites s
                    JOIN change_history ch ON ch.url = s.url
                    WHERE s.user_id = %s
                      AND s.company_id IS NOT NULL
                      AND ch.timestamp >= %s
                    UNION
                    SELECT k.company_id
                    FROM keywords k
                    JOIN articles a
                      ON a.user_id = k.user_id AND a.keyword = k.keyword
                    WHERE k.user_id = %s
                      AND k.company_id IS NOT NULL
                      AND a.found_at >= %s
                  )
                ORDER BY c.name
                """,
                (user_id, user_id, jst_midnight_str, user_id, utc_midnight_str),
            )
            companies = [dict(row) for row in cur.fetchall()]

        if not companies:
            return []

        company_ids = [c["id"] for c in companies]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # ── 2. 企業ごとの変更検知（最大3件）を取得 ──
            cur.execute(
                """
                SELECT s.company_id, ch.timestamp
                FROM change_history ch
                JOIN sites s ON s.url = ch.url
                WHERE s.user_id = %s
                  AND s.company_id = ANY(%s)
                  AND ch.timestamp >= %s
                ORDER BY ch.timestamp DESC
                """,
                (user_id, company_ids, jst_midnight_str),
            )
            changes_map: dict = {}
            for row in cur.fetchall():
                cid = row["company_id"]
                if cid not in changes_map:
                    changes_map[cid] = []
                if len(changes_map[cid]) < 3:
                    ts = row["timestamp"]
                    # "YYYY-MM-DD HH:MM:SS" → "HH:MM"
                    hhmm = ts[11:16] if len(ts) >= 16 else ts
                    changes_map[cid].append({"timestamp": hhmm})

            # ── 3. 企業ごとの本日ニュース記事数を取得 ──
            cur.execute(
                """
                SELECT k.company_id, COUNT(*) AS cnt
                FROM articles a
                JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword
                WHERE k.user_id = %s
                  AND k.company_id = ANY(%s)
                  AND a.found_at >= %s
                GROUP BY k.company_id
                """,
                (user_id, company_ids, utc_midnight_str),
            )
            news_map = {row["company_id"]: row["cnt"] for row in cur.fetchall()}

    for c in companies:
        c["changes"]    = changes_map.get(c["id"], [])
        c["news_count"] = news_map.get(c["id"], 0)

    return companies


def load_active_companies_since(user_id: int, since_dt) -> list:
    """since_dt 以降にサイト変更またはニュース記事があった企業を返す。
    since_dt は timezone-aware な datetime オブジェクト。
    load_active_companies_today() と同じ構造で since_dt を起点にする。
    """
    from datetime import timezone, timedelta
    jst = timezone(timedelta(hours=9))
    # change_history.timestamp は JST 文字列
    since_jst_str = since_dt.astimezone(jst).strftime("%Y-%m-%d %H:%M:%S")
    # articles.found_at は UTC
    since_utc_str = since_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT c.id, c.name
                FROM companies c
                WHERE c.user_id = %s
                  AND c.id IN (
                    SELECT s.company_id
                    FROM sites s
                    JOIN change_history ch ON ch.url = s.url
                    WHERE s.user_id = %s
                      AND s.company_id IS NOT NULL
                      AND ch.timestamp >= %s
                    UNION
                    SELECT k.company_id
                    FROM keywords k
                    JOIN articles a
                      ON a.user_id = k.user_id AND a.keyword = k.keyword
                    WHERE k.user_id = %s
                      AND k.company_id IS NOT NULL
                      AND a.found_at >= %s
                  )
                ORDER BY c.name
                """,
                (user_id, user_id, since_jst_str, user_id, since_utc_str),
            )
            companies = [dict(row) for row in cur.fetchall()]

        if not companies:
            return []

        company_ids = [c["id"] for c in companies]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT s.company_id, ch.timestamp
                FROM change_history ch
                JOIN sites s ON s.url = ch.url
                WHERE s.user_id = %s
                  AND s.company_id = ANY(%s)
                  AND ch.timestamp >= %s
                ORDER BY ch.timestamp DESC
                """,
                (user_id, company_ids, since_jst_str),
            )
            changes_map: dict = {}
            for row in cur.fetchall():
                cid = row["company_id"]
                if cid not in changes_map:
                    changes_map[cid] = []
                if len(changes_map[cid]) < 3:
                    ts = row["timestamp"]
                    hhmm = ts[11:16] if len(ts) >= 16 else ts
                    changes_map[cid].append({"timestamp": hhmm})

            cur.execute(
                """
                SELECT k.company_id, COUNT(*) AS cnt
                FROM articles a
                JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword
                WHERE k.user_id = %s
                  AND k.company_id = ANY(%s)
                  AND a.found_at >= %s
                GROUP BY k.company_id
                """,
                (user_id, company_ids, since_utc_str),
            )
            news_map = {row["company_id"]: row["cnt"] for row in cur.fetchall()}

    for c in companies:
        c["changes"]    = changes_map.get(c["id"], [])
        c["news_count"] = news_map.get(c["id"], 0)

    return companies


def load_companies(user_id: int) -> list:
    """企業一覧をユーザー設定の並び順で返す。"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name, name_kana, website_url, memo, created_at, updated_at, sort_order, securities_code, notify_enabled, notify_instant, youtube_channel_id "
                "FROM companies WHERE user_id = %s ORDER BY sort_order ASC, id ASC",
                (user_id,),
            )
            companies = [dict(row) for row in cur.fetchall()]
    return companies


def get_company(user_id: int, company_id: int) -> dict | None:
    """1件の企業情報を返す（ユーザー所有確認込み）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name, name_kana, website_url, memo, created_at, updated_at, securities_code, notify_enabled, notify_instant, youtube_channel_id "
                "FROM companies WHERE id = %s AND user_id = %s",
                (company_id, user_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def create_company(user_id: int, name: str, name_kana: str = "",
                   website_url: str = "", memo: str = "",
                   securities_code: str = "") -> int:
    """企業を追加して id を返す。sort_order は既存の最大値+1 にする。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(sort_order), 0) FROM companies WHERE user_id = %s",
                (user_id,),
            )
            next_order = cur.fetchone()[0] + 1
            cur.execute(
                "INSERT INTO companies (user_id, name, name_kana, website_url, memo, sort_order, securities_code) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (user_id, name, name_kana, website_url, memo, next_order,
                 (securities_code or None)),
            )
            return cur.fetchone()[0]


def update_companies_order(user_id: int, ids: list) -> None:
    """企業の sort_order を ids リストの順番で更新する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            for order, company_id in enumerate(ids):
                cur.execute(
                    "UPDATE companies SET sort_order = %s WHERE id = %s AND user_id = %s",
                    (order, company_id, user_id),
                )


def update_company(user_id: int, company_id: int, name: str, name_kana: str = "",
                   website_url: str = "", memo: str = "",
                   securities_code: str = "") -> bool:
    """企業情報を更新する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE companies SET name=%s, name_kana=%s, website_url=%s, memo=%s, "
                "securities_code=%s, updated_at=NOW() WHERE id=%s AND user_id=%s",
                (name, name_kana, website_url, memo,
                 (securities_code or None), company_id, user_id),
            )
            return cur.rowcount > 0


def delete_company(user_id: int, company_id: int) -> bool:
    """企業を削除する。関連する記事・変更履歴・キーワードもまとめて削除する。
      1) 企業に紐づくキーワード名を取得
      2) 各キーワードの articles を削除
      3) そのキーワードレコード自体を keywords から削除（SET NULL の副作用で孤立しないように）
      4) 企業に紐づくサイトURLの change_history を削除
      5) companies 本体を削除
    各ステップで削除件数を logger.info に出力する。
    """
    with _conn() as conn:
        with conn.cursor() as cur:
            # 1) この企業に紐付くキーワード一覧
            cur.execute(
                "SELECT keyword FROM keywords WHERE user_id = %s AND company_id = %s",
                (user_id, company_id),
            )
            kws = [r[0] for r in cur.fetchall()]
            logger.info(
                "[delete_company] user_id=%s company_id=%s keywords=%s",
                user_id, company_id, kws,
            )

            # 2) 各キーワードで記事削除（個別に実行して件数をログ）
            total_articles_deleted = 0
            for kw in kws:
                cur.execute(
                    "DELETE FROM articles WHERE user_id = %s AND keyword = %s",
                    (user_id, kw),
                )
                n = cur.rowcount
                total_articles_deleted += n
                logger.info(
                    "[delete_company] deleted %d articles for keyword=%r", n, kw,
                )
            logger.info(
                "[delete_company] total_articles_deleted=%d", total_articles_deleted,
            )

            # 3) そのキーワードレコード自体を削除（SET NULL で残らないように）
            cur.execute(
                "DELETE FROM keywords WHERE user_id = %s AND company_id = %s",
                (user_id, company_id),
            )
            kw_deleted = cur.rowcount
            logger.info("[delete_company] deleted %d keyword rows", kw_deleted)

            # 4) この企業に紐付くサイトURL一覧 → change_history を削除
            cur.execute(
                "SELECT url FROM sites WHERE user_id = %s AND company_id = %s",
                (user_id, company_id),
            )
            urls = [r[0] for r in cur.fetchall()]
            total_history_deleted = 0
            for url in urls:
                cur.execute("DELETE FROM change_history WHERE url = %s", (url,))
                total_history_deleted += cur.rowcount
            logger.info(
                "[delete_company] deleted %d change_history rows for %d urls",
                total_history_deleted, len(urls),
            )

            # 5) 企業本体を削除
            cur.execute(
                "DELETE FROM companies WHERE id = %s AND user_id = %s",
                (company_id, user_id),
            )
            ok = cur.rowcount > 0
            logger.info("[delete_company] company deleted=%s", ok)
            return ok


def get_company_summary(user_id: int, company_id: int, alert_kws: set) -> dict:
    """企業の集計情報を返す（サイト数、キーワード数、未読数、アラート数、最終更新）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sites WHERE user_id=%s AND company_id=%s",
                (user_id, company_id),
            )
            site_count = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM keywords WHERE user_id=%s AND company_id=%s",
                (user_id, company_id),
            )
            keyword_count = cur.fetchone()[0]

            # 全件数（LIMIT なし）
            cur.execute(
                "SELECT COUNT(DISTINCT a.id) FROM articles a "
                "JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword "
                "WHERE a.user_id=%s AND k.company_id=%s",
                (user_id, company_id),
            )
            article_count = cur.fetchone()[0]

            # 未読件数（同条件 + is_read=FALSE）
            cur.execute(
                "SELECT COUNT(DISTINCT a.id) FROM articles a "
                "JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword "
                "WHERE a.user_id=%s AND k.company_id=%s AND a.is_read = FALSE",
                (user_id, company_id),
            )
            unread_count = cur.fetchone()[0]

            # アラート集計用に直近30件のタイトルを取得
            cur.execute(
                "SELECT a.title FROM articles a "
                "JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword "
                "WHERE a.user_id=%s AND k.company_id=%s AND a.is_read = FALSE "
                "ORDER BY a.found_at DESC LIMIT 30",
                (user_id, company_id),
            )
            rows = cur.fetchall()
            alert_count  = sum(
                1 for r in rows
                if any(kw in r[0].lower() for kw in alert_kws)
            )

            # 最終更新: 記事の最新 found_at か企業の updated_at の新しい方
            cur.execute(
                "SELECT MAX(a.found_at) FROM articles a "
                "JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword "
                "WHERE a.user_id=%s AND k.company_id=%s",
                (user_id, company_id),
            )
            latest_article = cur.fetchone()[0]

    return {
        "site_count":     site_count,
        "keyword_count":  keyword_count,
        "article_count":  article_count,
        "unread_count":   unread_count,
        "alert_count":    alert_count,
        "latest_article": latest_article,
    }


def load_company_sites(user_id: int, company_id: int) -> list:
    """企業に紐づくサイト一覧"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, url, name, COALESCE(max_pages, 1) AS max_pages FROM sites "
                "WHERE user_id=%s AND company_id=%s ORDER BY id",
                (user_id, company_id),
            )
            return [dict(row) for row in cur.fetchall()]


def load_company_youtube_channels(company_id: int) -> list:
    """企業に紐づく YouTube チャンネル一覧を返す"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, channel_id, label FROM company_youtube_channels "
                "WHERE company_id = %s ORDER BY id",
                (company_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def add_company_youtube_channel(user_id: int, company_id: int,
                                channel_id: str, label: str = "") -> int | None:
    """YouTube チャンネルを追加する。成功時は新規 ID、5件上限超過時は -1、重複時は None。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM companies WHERE id = %s AND user_id = %s",
                (company_id, user_id),
            )
            if not cur.fetchone():
                return None
            cur.execute(
                "SELECT COUNT(*) FROM company_youtube_channels WHERE company_id = %s",
                (company_id,),
            )
            if cur.fetchone()[0] >= 5:
                return -1
            try:
                cur.execute(
                    "INSERT INTO company_youtube_channels (company_id, channel_id, label) "
                    "VALUES (%s, %s, %s) RETURNING id",
                    (company_id, channel_id.strip(), (label or "").strip() or None),
                )
                return cur.fetchone()[0]
            except psycopg2.errors.UniqueViolation:
                return None


def delete_company_youtube_channel(user_id: int, channel_db_id: int) -> bool:
    """YouTube チャンネルを削除する（所有権チェック付き）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM company_youtube_channels "
                "WHERE id = %s AND company_id IN "
                "(SELECT id FROM companies WHERE user_id = %s)",
                (channel_db_id, user_id),
            )
            return cur.rowcount > 0


def load_all_youtube_channels_for_user(user_id: int) -> list:
    """ユーザーの全企業の YouTube チャンネルを返す（収集用）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT yc.channel_id, yc.company_id, c.name AS company_name "
                "FROM company_youtube_channels yc "
                "JOIN companies c ON c.id = yc.company_id "
                "WHERE c.user_id = %s ORDER BY yc.company_id, yc.id",
                (user_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def update_company_youtube(user_id: int, company_id: int, channel_id: str):
    """企業の YouTube チャンネル ID を更新する。空文字は解除扱い。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE companies SET youtube_channel_id = %s "
                "WHERE id = %s AND user_id = %s",
                (channel_id or None, company_id, user_id),
            )


def is_company_instant(user_id: int, company_id: int) -> bool:
    """企業の即時通知が有効か（company_id=Noneまたは未登録はFalse）"""
    if not company_id:
        return False
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT notify_instant FROM companies WHERE id = %s AND user_id = %s",
                (company_id, user_id),
            )
            row = cur.fetchone()
            return bool(row[0]) if row else False


def update_company_notify_setting(user_id: int, company_id: int,
                                   notify_enabled: bool, notify_instant: bool) -> bool:
    """企業の通知設定を更新する。所有外は False。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE companies SET notify_enabled = %s, notify_instant = %s "
                "WHERE id = %s AND user_id = %s",
                (notify_enabled, notify_instant, company_id, user_id),
            )
            return cur.rowcount > 0


def toggle_company_notify(user_id: int, company_id: int) -> bool | None:
    """companies.notify_enabled を反転し、新しい値を返す。所有外は None。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE companies SET notify_enabled = NOT notify_enabled "
                "WHERE id = %s AND user_id = %s RETURNING notify_enabled",
                (company_id, user_id),
            )
            row = cur.fetchone()
            return row[0] if row else None


def is_company_notify_enabled(user_id: int, company_id: int) -> bool:
    """企業の通知が有効か（未登録の場合は True 扱い）"""
    if not company_id:
        return True
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT notify_enabled FROM companies WHERE id = %s AND user_id = %s",
                (company_id, user_id),
            )
            row = cur.fetchone()
            return row[0] if row else True


def load_badge_feedback(limit: int = 50, offset: int = 0) -> list:
    """管理者用: フィードバック一覧を取得する（JOIN済み）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT bf.id, bf.created_at, bf.reason_type, bf.reason_text, "
                "bf.importance_feedback, "
                "u.email AS user_email, u.company_name AS user_company, u.industry AS user_industry, "
                "a.title AS article_title, a.url AS article_url, a.keyword, a.importance AS article_importance, "
                "a.primary_company_id, pc.name AS primary_company_name, "
                "cc.name AS correct_company_name "
                "FROM badge_feedback bf "
                "JOIN users u ON u.id = bf.user_id "
                "JOIN articles a ON a.id = bf.article_id "
                "LEFT JOIN companies pc ON pc.id = a.primary_company_id "
                "LEFT JOIN companies cc ON cc.id = bf.correct_company_id "
                "ORDER BY bf.created_at DESC "
                "LIMIT %s OFFSET %s",
                (limit, offset),
            )
            return [dict(r) for r in cur.fetchall()]


def load_excluded_sources(user_id: int) -> list:
    """ユーザーの除外配信元一覧を返す"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, source_name FROM excluded_sources "
                "WHERE user_id = %s ORDER BY id",
                (user_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def add_excluded_source(user_id: int, source_name: str) -> int | None:
    """除外配信元を追加する。成功時はID、重複時はNone。"""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO excluded_sources (user_id, source_name) "
                    "VALUES (%s, %s) RETURNING id",
                    (user_id, source_name.strip()),
                )
                return cur.fetchone()[0]
    except psycopg2.errors.UniqueViolation:
        return None


def delete_excluded_source(user_id: int, source_id: int) -> bool:
    """除外配信元を削除する"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM excluded_sources WHERE id = %s AND user_id = %s",
                (source_id, user_id),
            )
            return cur.rowcount > 0


def load_feedback_article_ids(user_id: int) -> set:
    """ユーザーがフィードバック済みの article_id の集合を返す。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT article_id FROM badge_feedback WHERE user_id = %s",
                (user_id,),
            )
            return {row[0] for row in cur.fetchall()}


def load_feedback_examples_for_user(user_id: int, user_limit: int = 10,
                                    global_limit: int = 5) -> dict:
    """AI判定の few-shot 学習例をフィードバックから取得する。"""
    result = {"user_examples": [], "global_examples": []}

    def _format_row(row):
        reason_type = row.get("reason_type")
        if reason_type is None and row.get("importance_feedback"):
            return {
                "title": row.get("title", ""),
                "verdict": row.get("original_badge_name"),
                "reason": "correct",
                "importance": row.get("importance_feedback"),
            }
        elif reason_type == "wrong_company":
            return {
                "title": row.get("title", ""),
                "verdict": row.get("correct_company_name"),
                "reason": "wrong_company",
                "importance": row.get("importance_feedback"),
            }
        elif reason_type == "not_company_news":
            return {
                "title": row.get("title", ""),
                "verdict": None,
                "reason": "not_company_news",
                "importance": row.get("importance_feedback"),
            }
        return None

    base_join = (
        "FROM badge_feedback bf "
        "JOIN articles a ON bf.article_id = a.id "
        "LEFT JOIN companies c ON bf.correct_company_id = c.id "
        "LEFT JOIN companies ao_company ON a.primary_company_id = ao_company.id "
    )
    base_where = (
        "AND ("
        "  bf.reason_type IN ('wrong_company', 'not_company_news') "
        "  OR (bf.reason_type IS NULL AND bf.importance_feedback IS NOT NULL)"
        ") "
    )
    base_select = (
        "SELECT bf.article_id, a.title, bf.reason_type, bf.correct_company_id, "
        "c.name AS correct_company_name, bf.importance_feedback, "
        "a.importance AS original_importance, "
        "ao_company.name AS original_badge_name "
    )

    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                base_select + base_join +
                "WHERE bf.user_id = %s " + base_where +
                "ORDER BY bf.created_at DESC LIMIT %s",
                (user_id, user_limit),
            )
            user_rows = [dict(r) for r in cur.fetchall()]
            user_article_ids = [r["article_id"] for r in user_rows]

            for r in user_rows:
                fmt = _format_row(r)
                if fmt:
                    result["user_examples"].append(fmt)

            global_where = (
                "WHERE bf.user_id IS NOT NULL AND bf.user_id != %s " + base_where
            )
            params = [user_id]
            if user_article_ids:
                global_where += "AND bf.article_id != ALL(%s) "
                params.append(user_article_ids)
            params.append(global_limit)

            cur.execute(
                base_select + base_join + global_where +
                "ORDER BY bf.created_at DESC LIMIT %s",
                params,
            )
            for r in cur.fetchall():
                fmt = _format_row(dict(r))
                if fmt:
                    result["global_examples"].append(fmt)

    return result


def save_badge_feedback(article_id: int, user_id: int,
                        correct_company_id: int | None,
                        reason_type: str, reason_text: str = "",
                        importance_feedback: str | None = None) -> bool:
    """バッジ・重要度フィードバックを保存する"""
    if importance_feedback and importance_feedback not in ("high", "medium", "low"):
        importance_feedback = None
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO badge_feedback "
                "(article_id, user_id, correct_company_id, reason_type, reason_text, importance_feedback) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (article_id, user_id, correct_company_id, reason_type,
                 (reason_text or "")[:500], importance_feedback),
            )
            return cur.rowcount > 0


def count_user_unread(user_id: int, hide_sports: bool = False, hide_entertainment: bool = False) -> int:
    """ユーザーの未読記事数を返す（load_articles_data と同一条件）"""
    from datetime import datetime, timedelta, timezone
    jst = timezone(timedelta(hours=9))
    cutoff = (datetime.now(jst) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    pub_cutoff = (datetime.now(jst) - timedelta(days=7)).strftime("%Y-%m-%d")
    sports_clause = "AND COALESCE(a.is_sports, FALSE) = FALSE " if hide_sports else ""
    ent_clause = "AND COALESCE(a.is_entertainment, FALSE) = FALSE " if hide_entertainment else ""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(DISTINCT a.id) FROM articles a "
                "INNER JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword "
                "WHERE a.user_id = %s AND a.is_read = FALSE "
                "AND a.is_representative = TRUE "
                "AND a.found_at >= %s "
                "AND (a.published = '' OR REPLACE(a.published, '~', '') >= %s) "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM excluded_sources es "
                "  WHERE es.user_id = a.user_id AND es.source_name = a.source"
                ") "
                + sports_clause + ent_clause,
                (user_id, cutoff, pub_cutoff),
            )
            return cur.fetchone()[0]


def count_user_high_importance_unread(user_id: int, hide_sports: bool = False, hide_entertainment: bool = False) -> int:
    """ユーザーの importance='high' かつ未読の記事数を返す"""
    sports_clause = "AND COALESCE(is_sports, FALSE) = FALSE " if hide_sports else ""
    ent_clause = "AND COALESCE(is_entertainment, FALSE) = FALSE " if hide_entertainment else ""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM articles "
                "WHERE user_id = %s AND is_read = FALSE AND importance = 'high' "
                + sports_clause + ent_clause,
                (user_id,),
            )
            return cur.fetchone()[0]


def update_keyword_order(user_id: int, keyword_ids: list):
    """keyword_ids の配列順に sort_order を振り直す。自ユーザーのキーワードのみ更新する。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            for i, kid in enumerate(keyword_ids):
                cur.execute(
                    "UPDATE keywords SET sort_order = %s WHERE id = %s AND user_id = %s",
                    (i, int(kid), user_id),
                )


def load_company_keywords(user_id: int, company_id: int) -> list:
    """企業に紐づくキーワード一覧"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, keyword, notify_enabled FROM keywords "
                "WHERE user_id=%s AND company_id=%s ORDER BY sort_order, id",
                (user_id, company_id),
            )
            return [dict(row) for row in cur.fetchall()]


def load_company_articles(user_id: int, company_id: int, limit: int = 20,
                          hide_sports: bool = False, hide_entertainment: bool = False) -> list:
    """企業に紐づくキーワードの最新記事。
    company_exclude_keywords に登録された除外ワードを含む記事は表示時にも弾く。"""
    sports_clause = "AND COALESCE(a.is_sports, FALSE) = FALSE " if hide_sports else ""
    ent_clause = "AND COALESCE(a.is_entertainment, FALSE) = FALSE " if hide_entertainment else ""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT a.id, a.keyword, a.title, a.url, a.source, a.published, "
                "       a.found_at, a.is_read, a.date_verified "
                "FROM articles a "
                "JOIN keywords k ON k.user_id = a.user_id AND k.keyword = a.keyword "
                "WHERE a.user_id=%s AND k.company_id=%s "
                "AND NOT EXISTS ("
                "    SELECT 1 FROM company_exclude_keywords cek "
                "    WHERE cek.company_id = k.company_id "
                # psycopg2: SQL リテラル中の '%' は '%%' にエスケープ必須
                "    AND LOWER(a.title) LIKE '%%' || LOWER(cek.exclude_word) || '%%'"
                ") "
                + sports_clause + ent_clause +
                "ORDER BY a.is_read ASC, a.published DESC, a.id DESC LIMIT %s",
                (user_id, company_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]


def load_company_change_history(user_id: int, company_id: int, limit: int = 10) -> list:
    """企業に紐づくサイトのモニターサイト更新履歴"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT url FROM sites WHERE user_id=%s AND company_id=%s",
                (user_id, company_id),
            )
            urls = [row["url"] for row in cur.fetchall()]
            if not urls:
                return []
            cur.execute(
                "SELECT timestamp, url, name, diff FROM change_history "
                "WHERE url = ANY(%s) ORDER BY id DESC LIMIT %s",
                (urls, limit),
            )
            return [dict(row) for row in cur.fetchall()]


def set_site_company(user_id: int, site_url: str, company_id) -> bool:
    """サイトの company_id を設定（None で解除）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sites SET company_id=%s WHERE user_id=%s AND url=%s",
                (company_id, user_id, site_url),
            )
            return cur.rowcount > 0


def set_keyword_company(user_id: int, keyword: str, company_id) -> bool:
    """キーワードの company_id を設定（None で解除）"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE keywords SET company_id=%s WHERE user_id=%s AND keyword=%s",
                (company_id, user_id, keyword),
            )
            return cur.rowcount > 0


def create_site_and_link(user_id: int, url: str, name: str, company_id: int,
                         max_pages: int = 1) -> bool:
    """新規サイトを作成して company_id を同時に紐づける。URL重複時は紐づけのみ更新。"""
    if max_pages not in (1, 2, 3, 5, 10):
        max_pages = 1
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sites (url, name, user_id, company_id, max_pages) VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (user_id, url) DO UPDATE SET name = EXCLUDED.name, company_id = EXCLUDED.company_id, max_pages = EXCLUDED.max_pages",
                (url, name, user_id, company_id, max_pages),
            )
            return True


def create_keyword_and_link(user_id: int, keyword: str, company_id: int) -> bool:
    """新規キーワードを作成して company_id を同時に紐づける。重複時は紐づけのみ更新。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO keywords (keyword, user_id, notify_enabled, company_id) VALUES (%s, %s, TRUE, %s) "
                "ON CONFLICT (user_id, keyword) DO UPDATE SET company_id = EXCLUDED.company_id",
                (keyword, user_id, company_id),
            )
            return True


def load_sites_with_company(user_id: int) -> list:
    """全サイトを company_id 付きで返す（詳細画面の紐づけドロップダウン用）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, url, name, company_id FROM sites WHERE user_id=%s ORDER BY id",
                (user_id,),
            )
            return [dict(row) for row in cur.fetchall()]


def load_keywords_with_company(user_id: int) -> list:
    """全キーワードを company_id 付きで返す（詳細画面の紐づけドロップダウン用）"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, keyword, notify_enabled, company_id FROM keywords "
                "WHERE user_id=%s ORDER BY sort_order, id",
                (user_id,),
            )
            return [dict(row) for row in cur.fetchall()]


# ── ソースヘルス管理 ──────────────────────────────────────────

def update_source_health(source: str, success: bool, error: str = None):
    """ニュースソースの成否を記録する。成功時は連続失敗カウントをリセット。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            if success:
                cur.execute("""
                    INSERT INTO source_health (source, consecutive_failures, last_error, last_checked_at)
                    VALUES (%s, 0, NULL, NOW())
                    ON CONFLICT (source) DO UPDATE SET
                        consecutive_failures = 0,
                        last_error           = NULL,
                        last_checked_at      = NOW()
                """, (source,))
            else:
                cur.execute("""
                    INSERT INTO source_health (source, consecutive_failures, last_error, last_checked_at)
                    VALUES (%s, 1, %s, NOW())
                    ON CONFLICT (source) DO UPDATE SET
                        consecutive_failures = source_health.consecutive_failures + 1,
                        last_error           = EXCLUDED.last_error,
                        last_checked_at      = NOW()
                """, (source, (error or "")[:500]))


def get_source_health() -> dict:
    """全ソースのヘルス情報を {source: {...}} 形式で返す。"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM source_health")
            return {row["source"]: dict(row) for row in cur.fetchall()}


def set_source_error_notified(source: str):
    """エラー通知済みタイムスタンプを現在時刻に更新する。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO source_health (source, error_notified_at)
                VALUES (%s, NOW())
                ON CONFLICT (source) DO UPDATE SET error_notified_at = NOW()
            """, (source,))


def count_error_sites() -> int:
    """status='error' のサイト件数を返す。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM last_checks WHERE status = 'error'")
            row = cur.fetchone()
            return row[0] if row else 0


# ============================================================
# JPX 上場企業一覧 (listed_companies)
# ============================================================

def upsert_listed_companies(rows):
    with _conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO listed_companies (securities_code, company_name, company_name_kana, market, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (securities_code) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        company_name_kana = EXCLUDED.company_name_kana,
                        market = EXCLUDED.market,
                        updated_at = NOW()
                    """,
                    (row['securities_code'], row['company_name'], row['company_name_kana'], row['market'])
                )
    return len(rows)


def get_domain_override_url(company_name: str):
    """domain_overrides から企業名に一致する URL を1件返す（無ければ None）。
    実カラムは suggested_url。company_name は空文字デフォルトのため空は弾く。"""
    if not company_name:
        return None
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT suggested_url FROM domain_overrides "
                "WHERE company_name = %s AND suggested_url <> '' LIMIT 1",
                (company_name,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def search_listed_company(company_name):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT securities_code, company_name, company_name_kana, website_url "
                "FROM listed_companies WHERE company_name = %s LIMIT 1",
                (company_name,)
            )
            row = cur.fetchone()
            return dict(row) if row else None


def load_listed_companies_with_url() -> list:
    """website_url が非空の listed_companies を返す（URL 死活監視用）。
    戻り値: [{securities_code, company_name, website_url}, ...]"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT securities_code, company_name, website_url "
                "FROM listed_companies WHERE website_url <> '' "
                "ORDER BY securities_code"
            )
            return [dict(r) for r in cur.fetchall()]


def update_listed_company_url_check(securities_code: str,
                                    status: str,
                                    final_url: str = None) -> None:
    """1企業の URL チェック結果を反映する。final_url が与えられた場合は website_url を上書き。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            if final_url:
                cur.execute(
                    "UPDATE listed_companies "
                    "SET url_status = %s, url_checked_at = NOW(), website_url = %s "
                    "WHERE securities_code = %s",
                    (status, final_url, securities_code),
                )
            else:
                cur.execute(
                    "UPDATE listed_companies "
                    "SET url_status = %s, url_checked_at = NOW() "
                    "WHERE securities_code = %s",
                    (status, securities_code),
                )


def get_url_check_summary() -> dict:
    """url_status の集計（対象は website_url 非空のみ）を返す。"""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT url_status, COUNT(*) FROM listed_companies "
                "WHERE website_url <> '' GROUP BY url_status"
            )
            counts = {row[0]: row[1] for row in cur.fetchall()}
    return {
        "ok":        counts.get("ok", 0),
        "error":     counts.get("error", 0),
        "unchecked": counts.get("unchecked", 0),
    }


def get_url_check_errors() -> list:
    """url_status='error' の企業一覧を返す。"""
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT securities_code, company_name, website_url, url_checked_at "
                "FROM listed_companies "
                "WHERE url_status = 'error' "
                "ORDER BY url_checked_at DESC NULLS LAST, company_name"
            )
            return [dict(r) for r in cur.fetchall()]


def get_listed_companies_count() -> int:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM listed_companies")
            row = cur.fetchone()
            return row[0] if row else 0


def get_listed_company_by_code(securities_code: str) -> dict | None:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT securities_code, company_name, website_url, url_status "
                "FROM listed_companies WHERE securities_code = %s",
                (securities_code,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def apply_fixed_url(securities_code: str, new_url: str, company_name: str, old_url: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE listed_companies SET website_url = %s, url_status = 'ok' "
                "WHERE securities_code = %s",
                (new_url, securities_code),
            )
            cur.execute(
                "INSERT INTO fix_url_log (securities_code, company_name, old_url, new_url) "
                "VALUES (%s, %s, %s, %s)",
                (securities_code, company_name, old_url, new_url),
            )


def load_fix_url_log(limit: int = 50) -> list:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT securities_code, company_name, old_url, new_url, fixed_at "
                "FROM fix_url_log ORDER BY fixed_at DESC LIMIT %s",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]


def execute_auto_merge(groups: dict, executed_by: str = "admin"):
    """auto_merge_safe=yes のグループを一括マージする。1トランザクション。"""
    import json
    merged_groups = 0
    merged_entries = 0
    with _conn() as conn:
        with conn.cursor() as cur:
            for norm_key, entries in groups.items():
                logger.info("[auto_merge] processing: %s (%d entries)", norm_key, len(entries))
                entries_with_url = [e for e in entries if e.get("suggested_url")]
                if entries_with_url:
                    keep = min(entries_with_url, key=lambda e: e["id"])
                else:
                    keep = min(entries, key=lambda e: e["id"])
                keep_cn = keep["company_name"]
                keep_cnk = keep["company_name_kana"]
                for e in entries:
                    if e["id"] == keep["id"]:
                        continue
                    if not keep_cn and e.get("company_name"):
                        keep_cn = e["company_name"]
                    if not keep_cnk and e.get("company_name_kana"):
                        keep_cnk = e["company_name_kana"]
                norm = normalize_domain(keep["original_domain"])
                delete_ids = [e["id"] for e in entries if e["id"] != keep["id"]]
                deleted_info = [
                    {"id": e["id"], "domain": e["original_domain"],
                     "company_name": e.get("company_name", ""),
                     "company_name_kana": e.get("company_name_kana", ""),
                     "suggested_url": e.get("suggested_url", "")}
                    for e in entries if e["id"] != keep["id"]
                ]
                logger.info("[auto_merge] keep id=%s, delete ids=%s", keep["id"], delete_ids)
                for did in delete_ids:
                    cur.execute("DELETE FROM domain_overrides WHERE id=%s", (did,))
                cur.execute(
                    "UPDATE domain_overrides SET domain=%s, company_name=%s, company_name_kana=%s WHERE id=%s",
                    (norm, keep_cn, keep_cnk, keep["id"]),
                )
                logger.info("[auto_merge] inserting log for: %s", norm_key)
                cur.execute(
                    "INSERT INTO merge_log (action, normalized_domain, kept_entry_id, "
                    "kept_domain, kept_company_name, kept_suggested_url, "
                    "deleted_entries, executed_by) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    ("auto_merge", norm_key, keep["id"], norm, keep_cn,
                     keep.get("suggested_url", ""),
                     json.dumps(deleted_info, ensure_ascii=False),
                     executed_by),
                )
                merged_groups += 1
                merged_entries += len(delete_ids)
    logger.info("[auto_merge] completed: %d groups, %d entries", merged_groups, merged_entries)
    return merged_groups, merged_entries


def execute_manual_merge(norm_key: str, keep_id: int | None, entries: list,
                          action: str, domain: str = "", company_name: str = "",
                          company_name_kana: str = "", suggested_url: str = "",
                          skip_session_id: str | None = None,
                          executed_by: str = "admin",
                          entry_edits: dict | None = None):
    """手動マージ/全削除/スキップを1トランザクションで実行する。"""
    import json
    with _conn() as conn:
        with conn.cursor() as cur:
            if action == "skip":
                cur.execute(
                    "INSERT INTO merge_log (action, normalized_domain, skip_session_id, executed_by) "
                    "VALUES (%s, %s, %s, %s)",
                    ("skip", norm_key, skip_session_id, executed_by),
                )
                return
            if action == "keep_both_as_exception":
                edits = entry_edits or {}
                for e in entries:
                    host = clean_hostname(e["original_domain"])
                    if not host:
                        continue
                    edit = edits.get(e["id"], {})
                    cn = edit.get("company_name", "")
                    cnk = edit.get("company_name_kana", "")
                    surl = edit.get("suggested_url", "")
                    updates = ["domain=%s", "is_exception=TRUE"]
                    params = [host]
                    if cn:
                        updates.append("company_name=%s")
                        params.append(cn)
                    if cnk:
                        updates.append("company_name_kana=%s")
                        params.append(cnk)
                    if surl:
                        updates.append("suggested_url=%s")
                        params.append(surl)
                    params.append(e["id"])
                    cur.execute(
                        f"UPDATE domain_overrides SET {', '.join(updates)} WHERE id=%s",
                        params,
                    )
                kept_id = min(e["id"] for e in entries)
                cur.execute(
                    "INSERT INTO merge_log (action, normalized_domain, kept_entry_id, executed_by) "
                    "VALUES (%s, %s, %s, %s)",
                    ("keep_both_as_exception", norm_key, kept_id, executed_by),
                )
                return
            if action == "delete_all":
                deleted_info = [
                    {"id": e["id"], "domain": e["original_domain"],
                     "company_name": e.get("company_name", ""),
                     "company_name_kana": e.get("company_name_kana", ""),
                     "suggested_url": e.get("suggested_url", "")}
                    for e in entries
                ]
                for e in entries:
                    cur.execute("DELETE FROM domain_overrides WHERE id=%s", (e["id"],))
                cur.execute(
                    "INSERT INTO merge_log (action, normalized_domain, deleted_entries, executed_by) "
                    "VALUES (%s, %s, %s, %s)",
                    ("delete_all", norm_key, json.dumps(deleted_info, ensure_ascii=False), executed_by),
                )
                return
            norm = normalize_domain(domain) or norm_key
            cur.execute(
                "UPDATE domain_overrides SET domain=%s, suggested_url=%s, "
                "company_name=%s, company_name_kana=%s WHERE id=%s",
                (norm, suggested_url, company_name, company_name_kana, keep_id),
            )
            deleted_info = [
                {"id": e["id"], "domain": e["original_domain"],
                 "company_name": e.get("company_name", ""),
                 "company_name_kana": e.get("company_name_kana", ""),
                 "suggested_url": e.get("suggested_url", "")}
                for e in entries if e["id"] != keep_id
            ]
            for e in entries:
                if e["id"] != keep_id:
                    cur.execute("DELETE FROM domain_overrides WHERE id=%s", (e["id"],))
            cur.execute(
                "INSERT INTO merge_log (action, normalized_domain, kept_entry_id, "
                "kept_domain, kept_company_name, kept_suggested_url, "
                "deleted_entries, executed_by) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                ("manual_merge", norm_key, keep_id, norm, company_name,
                 suggested_url, json.dumps(deleted_info, ensure_ascii=False),
                 executed_by),
            )


def get_merge_log_latest_action(normalized_domain: str) -> dict | None:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT action, skip_session_id FROM merge_log "
                "WHERE normalized_domain=%s ORDER BY executed_at DESC LIMIT 1",
                (normalized_domain,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def load_merge_log(limit: int = 100) -> list:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, executed_at, action, normalized_domain, kept_entry_id, "
                "kept_domain, deleted_entries, executed_by "
                "FROM merge_log ORDER BY executed_at DESC LIMIT %s",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]
