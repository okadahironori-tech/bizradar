"""normalize_domain() の単体テスト"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from db import normalize_domain


def test_basic():
    assert normalize_domain("www.mercari.com") == "mercari.com"
    assert normalize_domain("mercari.com") == "mercari.com"


def test_subdomain():
    assert normalize_domain("about.mercari.com") == "mercari.com"
    assert normalize_domain("corp.toyota.co.jp") == "toyota.co.jp"
    assert normalize_domain("www.toyota.co.jp") == "toyota.co.jp"


def test_url_input():
    assert normalize_domain("https://www.mercari.com/path") == "mercari.com"


def test_empty_none():
    assert normalize_domain("") == ""
    assert normalize_domain(None) == ""


def test_invalid():
    assert normalize_domain("invalid") == ""


def test_whitespace_case():
    assert normalize_domain("  WWW.Mercari.COM  ") == "mercari.com"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS: {name}")
    print("All tests passed.")
