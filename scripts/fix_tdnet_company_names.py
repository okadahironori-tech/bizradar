#!/usr/bin/env python3
"""一時スクリプト: tdnet_disclosures.company_name のスペースを除去する.

Render Shell で実行:
    python3 scripts/fix_tdnet_company_names.py

完了後、本スクリプトと db.fix_tdnet_company_names() は削除して良い。
"""
import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

import db  # noqa: E402


def main() -> int:
    # 事前カウント
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM tdnet_disclosures "
                "WHERE company_name LIKE '% %' OR company_name LIKE '%　%'"
            )
            before = cur.fetchone()[0]
    print(f"[fix] target rows (with spaces): {before}")

    n = db.fix_tdnet_company_names()
    print(f"[fix] updated: {n}")

    # 事後確認
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM tdnet_disclosures "
                "WHERE company_name LIKE '% %' OR company_name LIKE '%　%'"
            )
            after = cur.fetchone()[0]
    print(f"[fix] remaining rows with spaces: {after}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
