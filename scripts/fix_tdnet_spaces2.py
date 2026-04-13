#!/usr/bin/env python3
"""一時スクリプト: tdnet_disclosures.company_name に残った各種空白類を一括除去する.

対象となる空白類（\\s に加えて明示指定）:
  - 半角スペース / 全角スペース (U+3000)
  - ノーブレークスペース (U+00A0)
  - ゼロ幅スペース / ノーブレークスペース (U+200B, U+200C, U+200D, U+FEFF)
  - タブ・改行等（\\s マッチ）

Render Shell で実行:
    python3 scripts/fix_tdnet_spaces2.py

完了後、本スクリプトは削除して良い。
"""
import os
import re
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

import db  # noqa: E402

_SPACE_RE = re.compile(r"[\s\u3000\u00a0\u200b\u200c\u200d\ufeff]+")


def clean_company(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.replace("\x00", "")
    return _SPACE_RE.sub("", s).strip()


def main() -> int:
    updated = 0
    skipped = 0
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, company_name FROM tdnet_disclosures")
            rows = cur.fetchall()
            for row_id, name in rows:
                new_name = clean_company(name)
                if new_name != name:
                    cur.execute(
                        "UPDATE tdnet_disclosures SET company_name = %s WHERE id = %s",
                        (new_name, row_id),
                    )
                    updated += 1
                else:
                    skipped += 1
    print(f"[fix] updated={updated}, unchanged={skipped}, total={updated + skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
