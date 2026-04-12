#!/usr/bin/env python3
"""Render Shellで実行するデバッグスクリプト.

Usage:
    python3 scripts/debug_kw.py                # okada_jp のキーワード一覧を表示
    python3 scripts/debug_kw.py "トヨタ"        # 指定キーワードでニュース取得をテスト
    python3 scripts/debug_kw.py --email other@example.com "キーワード"
"""
import sys
import os
import traceback

# リポジトリルートを import パスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db  # noqa: E402


DEFAULT_EMAIL = "okada_jp@hotmail.com"


def main():
    args = sys.argv[1:]
    email = DEFAULT_EMAIL
    keyword = None

    if args and args[0] == "--email":
        email = args[1]
        args = args[2:]
    if args:
        keyword = args[0]

    # --- ユーザー情報 ---
    print(f"=== User lookup: {email} ===")
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, email, created_at, is_admin FROM users WHERE email = %s", (email,))
            row = cur.fetchone()
    if not row:
        print(f"[ERROR] user '{email}' not found")
        sys.exit(1)
    user_id, email_db, created_at, is_admin = row
    print(f"  user_id={user_id}, email={email_db}, created_at={created_at}, is_admin={is_admin}")

    # --- 記事件数 ---
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM articles WHERE user_id = %s", (user_id,))
            total_articles = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM articles WHERE user_id = %s AND is_read = FALSE", (user_id,))
            unread = cur.fetchone()[0]
    print(f"  articles total={total_articles}, unread={unread}")

    # --- キーワード一覧 ---
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, keyword, company_id, COALESCE(notify_enabled, TRUE) "
                "FROM keywords WHERE user_id = %s ORDER BY id",
                (user_id,),
            )
            kws = cur.fetchall()

    print(f"\n=== Keywords ({len(kws)}) ===")
    for kid, kw, cid, notify in kws:
        print(f"  id={kid}, keyword={kw!r}, company_id={cid}, notify={notify}")

    if keyword is None:
        print("\n(キーワードを引数で指定すると、そのキーワードでニュース取得をテストします)")
        return

    # --- 指定キーワードでニュース取得テスト ---
    print(f"\n=== Fetching news for keyword: {keyword!r} (user_id={user_id}) ===")

    # 各取得関数を個別に試す
    import monitor

    seen_urls = db.load_article_seen_urls(user_id)
    seen_titles = db.load_article_seen_titles(user_id)
    exclude_kws = {e["keyword"].lower() for e in db.get_exclude_keywords(user_id)}
    print(f"  seen_urls={len(seen_urls)}, seen_titles={len(seen_titles)}, exclude_kws={len(exclude_kws)}")

    google_articles = []
    yahoo_articles = []
    prtimes_articles = []

    print("\n--- Google News ---")
    try:
        google_articles = monitor.fetch_news_articles(keyword)
        print(f"  取得件数: {len(google_articles)}")
        for a in google_articles[:3]:
            print(f"    - {a.get('title', '')[:80]} | {a.get('url', '')}")
    except Exception as e:
        print(f"  [ERROR] {e}")
        traceback.print_exc()

    print("\n--- Yahoo/Bing News ---")
    try:
        yahoo_articles = monitor.fetch_bing_news_articles(keyword)
        print(f"  取得件数: {len(yahoo_articles)}")
        for a in yahoo_articles[:3]:
            print(f"    - {a.get('title', '')[:80]} | {a.get('url', '')}")
    except Exception as e:
        print(f"  [ERROR] {e}")
        traceback.print_exc()

    print("\n--- PR TIMES ---")
    try:
        prtimes_articles = monitor.fetch_prtimes_articles(keyword)
        print(f"  取得件数: {len(prtimes_articles)}")
        for a in prtimes_articles[:3]:
            print(f"    - {a.get('title', '')[:80]} | {a.get('url', '')}")
    except Exception as e:
        print(f"  [ERROR] {e}")
        traceback.print_exc()

    # --- 新着判定 ---
    all_articles = google_articles + yahoo_articles + prtimes_articles
    new_articles = []
    skipped_seen = 0
    skipped_excluded = 0
    for article in all_articles:
        url = article.get("url", "")
        title = article.get("title", "")
        title_key = f"{keyword}::{title}"
        if exclude_kws and any(ex in title.lower() for ex in exclude_kws):
            skipped_excluded += 1
            continue
        if url and url not in seen_urls and title_key not in seen_titles:
            new_articles.append(article)
            seen_urls.add(url)
            seen_titles.add(title_key)
        else:
            skipped_seen += 1

    print(f"\n=== Summary ===")
    print(f"  全取得件数: {len(all_articles)} (Google={len(google_articles)}, Yahoo={len(yahoo_articles)}, PRTimes={len(prtimes_articles)})")
    print(f"  新着判定: {len(new_articles)} 件が新着")
    print(f"  既出スキップ: {skipped_seen}")
    print(f"  除外KWスキップ: {skipped_excluded}")

    if new_articles:
        print(f"\n=== New articles preview (max 5) ===")
        for a in new_articles[:5]:
            print(f"  - {a.get('title', '')[:80]}")
            print(f"    url={a.get('url', '')}")
            print(f"    source={a.get('source', '')}, published={a.get('published', '')}")

    # --- 実際にDB保存するかは尋ねない（ドライラン） ---
    print(f"\n(このスクリプトは DRY RUN です。DBへの保存・メール送信は行いません)")


if __name__ == "__main__":
    main()
