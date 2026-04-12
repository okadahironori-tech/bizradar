#!/usr/bin/env python3
"""Render Cron Jobから呼び出す想定のキーワードチェック単独実行スクリプト.

通常のスケジューラー（monitor.main のループ）とは独立して、
check_all_keywords() を1回だけ実行して終了する。

Usage (Render Cron Job):
    python3 scripts/run_keyword_check.py

ローカルテスト:
    python3 scripts/run_keyword_check.py
"""
import os
import sys
import traceback
from datetime import datetime

# リポジトリルートを import パスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import monitor  # noqa: E402


def main() -> int:
    print(f"[Cron] キーワードチェック開始 {datetime.now().isoformat()}")
    try:
        monitor.check_all_keywords()
    except Exception as e:
        print(f"[Cron][ERROR] check_all_keywords が例外で停止: {e}")
        traceback.print_exc()
        return 1
    print(f"[Cron] キーワードチェック完了 {datetime.now().isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
