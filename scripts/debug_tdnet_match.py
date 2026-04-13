#!/usr/bin/env python3
"""一時診断スクリプト: companies と tdnet_disclosures の照合状況を表示する.

Render Shell で実行:
    python3 scripts/debug_tdnet_match.py [user_id]
    例: python3 scripts/debug_tdnet_match.py 2

user_id を省略すると全ユーザーの集計を出す。
完了後、本スクリプトは削除して良い。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db  # noqa: E402


def main() -> int:
    user_id_arg = None
    if len(sys.argv) >= 2:
        try:
            user_id_arg = int(sys.argv[1])
        except ValueError:
            print(f"[ERROR] user_id must be integer: {sys.argv[1]}")
            return 1

    with db._conn() as conn:
        with conn.cursor() as cur:
            if user_id_arg is not None:
                print(f"=== user_id={user_id_arg} の企業 × TDnet ヒット数 ===")
                cur.execute(
                    """
                    SELECT c.name, COUNT(t.id) AS hits
                    FROM companies c
                    LEFT JOIN tdnet_disclosures t
                      ON t.company_name LIKE '%' || c.name || '%'
                    WHERE c.user_id = %s
                    GROUP BY c.name
                    ORDER BY hits DESC, c.name
                    """,
                    (user_id_arg,),
                )
                rows = cur.fetchall()
                print(f"{'rank':<5}{'company':<30}{'hits':>8}")
                for i, (name, hits) in enumerate(rows, 1):
                    print(f"  {i:<5}{name:<30}{hits:>8}")

                # ゼロヒット企業について先頭4文字フォールバックも試算
                zero = [n for n, h in rows if h == 0 and n and len(n) >= 4]
                if zero:
                    print()
                    print("=== ゼロヒット企業の先頭4文字フォールバック試算 ===")
                    for n in zero:
                        prefix = n[:4]
                        cur.execute(
                            "SELECT COUNT(*) FROM tdnet_disclosures "
                            "WHERE company_name LIKE %s",
                            (f"%{prefix}%",),
                        )
                        c = cur.fetchone()[0]
                        print(f"  '{n}' → prefix='{prefix}' → {c} hits")
            else:
                print("=== ユーザー別ヒット総数 ===")
                cur.execute(
                    """
                    SELECT c.user_id, u.email,
                           COUNT(DISTINCT t.id) AS hits,
                           COUNT(DISTINCT c.id) AS company_count
                    FROM companies c
                    JOIN users u ON u.id = c.user_id
                    LEFT JOIN tdnet_disclosures t
                      ON t.company_name LIKE '%' || c.name || '%'
                    GROUP BY c.user_id, u.email
                    ORDER BY hits DESC
                    LIMIT 20
                    """
                )
                for uid, email, hits, cc in cur.fetchall():
                    print(f"  user_id={uid} email={email} companies={cc} hits={hits}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
