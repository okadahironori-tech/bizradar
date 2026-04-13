#!/usr/bin/env python3
"""孤立した articles（どのユーザーの keywords にも紐付かないレコード）を一括削除する一時スクリプト.

Render Shell で実行する想定:
    python3 scripts/cleanup_orphan_articles.py

実行後、削除件数が Render のログに出力される。本スクリプトと
db.py の delete_orphan_articles() は完了後に削除してよい。
"""
import os
import sys
import logging
from datetime import datetime

# リポジトリルートを import パスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 標準ログを出力に流す
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

import db  # noqa: E402


def main() -> int:
    print(f"[cleanup] 開始 {datetime.now().isoformat()}")
    try:
        # 事前カウント
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM articles")
                before = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM articles "
                    "WHERE (user_id, keyword) NOT IN (SELECT user_id, keyword FROM keywords)"
                )
                orphan = cur.fetchone()[0]
        print(f"[cleanup] articles total={before}, orphans={orphan}")

        deleted = db.delete_orphan_articles()
        print(f"[cleanup] deleted={deleted}")

        # 事後カウント
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM articles")
                after = cur.fetchone()[0]
        print(f"[cleanup] articles after={after}")

    except Exception as e:
        import traceback
        print(f"[cleanup][ERROR] {e}")
        traceback.print_exc()
        return 1

    print(f"[cleanup] 完了 {datetime.now().isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
