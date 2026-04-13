#!/usr/bin/env python3
"""一時診断スクリプト: TDnet API から返される company_name の生バイト列を可視化する.

Render Shell で実行:
    python3 scripts/debug_tdnet_bytes.py

出力内容:
  - 各 item について company_name を repr() と ord() 配列で表示
  - どの文字コードの空白類が含まれているかを unicodedata 名称とともに表示
  - DBに既に保存されているレコードについても同様に調査

完了後、本スクリプトは削除して良い。
"""
import os
import sys
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

URL = "https://webapi.yanoshin.jp/webapi/tdnet/list/recent.json?limit=5"


def inspect(label: str, name: str):
    print(f"--- {label} ---")
    print(f"  repr:     {name!r}")
    print(f"  len:      {len(name)}")
    codes = []
    for c in name:
        cat = unicodedata.category(c)
        try:
            uname = unicodedata.name(c)
        except ValueError:
            uname = "(no name)"
        codes.append(f"{hex(ord(c))}({cat}:{uname})")
    print(f"  codes:    {codes}")
    # ASCII/Unicode空白の該当チェック
    import re
    if re.search(r"[\s\u3000\u00a0\u200b\u200c\u200d\ufeff]", name):
        print("  -> contains known whitespace")
    else:
        print("  -> no known whitespace detected")


def main() -> int:
    print("=" * 60)
    print("1) API から直接取得した company_name を診断")
    print("=" * 60)
    resp = requests.get(URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items") or []
    for i, item in enumerate(items[:5]):
        t = item.get("Tdnet") or {}
        name = t.get("company_name") or ""
        inspect(f"API item #{i+1}", name)

    print()
    print("=" * 60)
    print("2) DB に保存済みの tdnet_disclosures から最新5件を診断")
    print("=" * 60)
    try:
        import db  # noqa: E402
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT company_name FROM tdnet_disclosures "
                    "ORDER BY disclosed_at DESC LIMIT 5"
                )
                for j, (name,) in enumerate(cur.fetchall()):
                    inspect(f"DB row #{j+1}", name)
    except Exception as e:
        print(f"[WARN] DB 取得スキップ: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
