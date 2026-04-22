"""mask_email の単体テスト"""


def mask_email(email):
    if not email or "@" not in email:
        return "***@***.***"
    local, domain = email.rsplit("@", 1)
    masked_local = (local[0] if local else "") + "***"
    parts = domain.rsplit(".", 1)
    if len(parts) == 2:
        masked_domain = (parts[0][0] if parts[0] else "") + "***." + parts[1]
    else:
        masked_domain = (domain[0] if domain else "") + "***"
    return f"{masked_local}@{masked_domain}"


def test_basic():
    assert mask_email("biz@example.com") == "b***@e***.com"

def test_short_local():
    assert mask_email("a@example.com") == "a***@e***.com"

def test_subdomain():
    assert mask_email("user@mail.example.co.jp") == "u***@m***.jp"

def test_empty():
    assert mask_email("") == "***@***.***"
    assert mask_email(None) == "***@***.***"

def test_no_at():
    assert mask_email("invalid") == "***@***.***"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS: {name}")
    print("Done.")
