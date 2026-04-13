#!/usr/bin/env python3
"""孤立した articles / keywords を一括削除する一時スクリプト.

対象:
  1) company_id IS NULL の keywords に紐づく articles
  2) company_id IS NULL の keywords 自体

Render Shell で実行する想定:
    python3 scripts/cleanup_orphan_articles.py

完了後、本スクリプトと db.delete_orphan_articles() は削除してよい。
"""
import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

import db  # noqa: E402


def main() -> int:
    print(f"[cleanup] 開始 {datetime.now().isoformat()}")
    try:
        # 事前カウント
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM articles")
                articles_before = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM keywords")
                keywords_before = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM articles "
                    "WHERE (user_id, keyword) IN "
                    "(SELECT user_id, keyword FROM keywords WHERE company_id IS NULL)"
                )
                orphan_articles = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM keywords WHERE company_id IS NULL")
                orphan_keywords = cur.fetchone()[0]
        print(f"[cleanup] articles before={articles_before}, orphan_candidates={orphan_articles}")
        print(f"[cleanup] keywords before={keywords_before}, orphan_candidates={orphan_keywords}")

        result = db.delete_orphan_articles()
        print(f"[cleanup] deleted articles={result['articles']}, keywords={result['keywords']}")

        # 事後カウント
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM articles")
                articles_after = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM keywords")
                keywords_after = cur.fetchone()[0]
        print(f"[cleanup] articles after={articles_after}, keywords after={keywords_after}")

    except Exception as e:
        import traceback
        print(f"[cleanup][ERROR] {e}")
        traceback.print_exc()
        return 1

    print(f"[cleanup] 完了 {datetime.now().isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
