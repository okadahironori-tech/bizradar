#!/usr/bin/env python3
"""一時スクリプト: hirokun1973@gmail.com の pro プラン設定.

動作:
  1) 該当ユーザーの有無を確認
  2) 存在しない場合: 新規作成（password="BizRadar2024test", plan="pro"）
  3) 存在する場合: plan を 'pro' に更新
  4) 最終状態を表示

Render Shell で実行:
    python3 scripts/setup_user_hirokun.py

完了後、本スクリプトは削除して良い。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db  # noqa: E402

EMAIL = "hirokun1973@gmail.com"
PASSWORD = "BizRadar2024test"
PLAN = "pro"


def main() -> int:
    # 1) 存在確認
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, email, plan FROM users WHERE email = %s",
                (EMAIL,),
            )
            row = cur.fetchone()

    if row is None:
        # 2) 新規作成
        print(f"[setup] user not found. creating {EMAIL} ...")
        user_id = db.create_user(EMAIL, PASSWORD)
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET plan = %s WHERE id = %s",
                    (PLAN, user_id),
                )
        print(f"[setup] created user_id={user_id}")
    else:
        # 3) 既存 → plan 更新
        user_id, email, current_plan = row
        print(f"[setup] user exists: id={user_id} email={email} plan={current_plan}")
        with db._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET plan = %s WHERE email = %s",
                    (PLAN, EMAIL),
                )
        print(f"[setup] updated plan to {PLAN!r}")

    # 4) 最終状態を確認
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, email, plan FROM users WHERE email = %s",
                (EMAIL,),
            )
            row = cur.fetchone()
    print(f"[setup] FINAL: {row}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
