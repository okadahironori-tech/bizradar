#!/usr/bin/env python3
"""DBに保存されている記事件数を確認するスクリプト.

Render Shellで実行する想定。
表示内容:
  1) 全記事件数
  2) 90日以内の記事件数（found_at 基準）
  3) ユーザー別 90日以内記事数 TOP 10（email と件数）

Usage:
    python3 scripts/check_article_count.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

# リポジトリルートを import パスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db  # noqa: E402


def main() -> int:
    jst = timezone(timedelta(hours=9))
    cutoff = (datetime.now(jst) - timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"基準日時 (90日前): {cutoff} JST")
    print("-" * 50)

    with db._conn() as conn:
        with conn.cursor() as cur:
            # 1) 全記事件数
            cur.execute("SELECT COUNT(*) FROM articles")
            total_all = cur.fetchone()[0]
            print(f"1) 全記事件数: {total_all:,} 件")

            # found_at が空文字のレコード数（補足）
            cur.execute("SELECT COUNT(*) FROM articles WHERE found_at = ''")
            empty_found_at = cur.fetchone()[0]
            if empty_found_at:
                print(f"   うち found_at 未設定: {empty_found_at:,} 件（期間判定から除外）")

            # 2) 90日以内の記事件数
            cur.execute(
                "SELECT COUNT(*) FROM articles WHERE found_at != '' AND found_at >= %s",
                (cutoff,),
            )
            total_recent = cur.fetchone()[0]
            print(f"2) 90日以内の記事件数: {total_recent:,} 件")

            # 参考: 未読のみ
            cur.execute(
                "SELECT COUNT(*) FROM articles "
                "WHERE found_at != '' AND found_at >= %s AND is_read = FALSE",
                (cutoff,),
            )
            total_recent_unread = cur.fetchone()[0]
            print(f"   うち未読: {total_recent_unread:,} 件")

            print("-" * 50)

            # 3) ユーザー別 TOP 10
            cur.execute(
                """
                SELECT u.id, u.email,
                       COUNT(*) AS n_90d,
                       SUM(CASE WHEN a.is_read = FALSE THEN 1 ELSE 0 END) AS n_unread
                FROM articles a
                JOIN users u ON u.id = a.user_id
                WHERE a.found_at != '' AND a.found_at >= %s
                GROUP BY u.id, u.email
                ORDER BY n_90d DESC
                LIMIT 10
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
            print("3) ユーザー別 90日以内記事数 TOP 10:")
            if not rows:
                print("   （該当ユーザーなし）")
            else:
                print(f"   {'rank':<5}{'user_id':<10}{'email':<40}{'90日以内':>12}{'未読':>10}")
                for i, (uid, email, n90, nunread) in enumerate(rows, 1):
                    print(f"   {i:<5}{uid:<10}{email:<40}{n90:>12,}{nunread:>10,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
