"""normalize_news_title と転載記事グルーピングの単体テスト
dashboard.py のアプリ初期化を避けるため、関数を直接定義してテストする"""
import hashlib
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta


def normalize_news_title(title):
    if not title or not title.strip():
        return (title or "").strip(), True
    s = unicodedata.normalize("NFKC", title)
    for _ in range(10):
        prev = s
        s = re.sub(r'\s*[（(][^）)]{1,30}[）)]\s*$', '', s)
        for sep in [' | ', ' ｜ ', ' - ', ' – ', '｜', '|']:
            idx = s.rfind(sep)
            if idx < 0:
                continue
            tail = s[idx + len(sep):]
            if len(tail.strip()) > 30:
                continue
            candidate = s[:idx]
            if len(candidate.strip()) < 10:
                continue
            s = candidate
            break
        s = re.sub(r'\s*[（(][^）)]{1,30}[）)]\s*$', '', s)
        if s == prev:
            break
    s = s.strip()
    if not s:
        return title.strip(), True
    return s, False


def _normalize_title_hash(title):
    norm, fallback = normalize_news_title(title)
    if fallback:
        return "", True
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16], False


def _group_syndicated_articles(articles):
    groups = defaultdict(list)
    standalone = []
    for a in articles:
        pub = (a.get("published") or "").lstrip("~").strip()
        if not pub:
            standalone.append(a)
            continue
        title_hash, skip = _normalize_title_hash(a.get("title", ""))
        if skip or not title_hash:
            standalone.append(a)
            continue
        key = (a.get("keyword", ""), title_hash)
        groups[key].append(a)
    result = []
    for key, members in groups.items():
        if len(members) < 2:
            for a in members:
                a["group_size"] = 1
                a["grouped_siblings"] = []
                a["is_group_representative"] = True
            result.extend(members)
            continue
        members.sort(key=lambda a: (
            (a.get("published") or "").lstrip("~").strip(),
            (a.get("source") or "") or "\uffff",
            a.get("id", 0),
        ))
        pub_vals = [(a.get("published") or "").lstrip("~").strip() for a in members]
        min_pub, max_pub = pub_vals[0], pub_vals[-1]
        try:
            dt_min = datetime.strptime(min_pub[:16], "%Y-%m-%d %H:%M")
            dt_max = datetime.strptime(max_pub[:16], "%Y-%m-%d %H:%M")
            if (dt_max - dt_min) > timedelta(hours=24):
                for a in members:
                    a["group_size"] = 1
                    a["grouped_siblings"] = []
                    a["is_group_representative"] = True
                result.extend(members)
                continue
        except (ValueError, TypeError):
            for a in members:
                a["group_size"] = 1
                a["grouped_siblings"] = []
                a["is_group_representative"] = True
            result.extend(members)
            continue
        rep = members[0]
        siblings = [{"source": a.get("source", ""), "url": a.get("url", ""),
                      "title": a.get("title", "")} for a in members[1:]]
        any_alert = any(a.get("is_alert") or a.get("importance") == "high" for a in members)
        if any_alert:
            rep["is_alert"] = True
        rep["group_size"] = len(members)
        rep["grouped_siblings"] = siblings
        rep["is_group_representative"] = True
        result.append(rep)
    for a in standalone:
        a["group_size"] = 1
        a["grouped_siblings"] = []
        a["is_group_representative"] = True
    result.extend(standalone)
    return result


def test_syndication_grouping_hash():
    titles = [
        "トヨタ、海外で3万8千台減産 中東情勢影響、11月までに | 全国のニュース - 福井新聞社",
        "トヨタ、海外で3万8千台減産 中東情勢影響、11月までに - 西日本新聞me",
        "トヨタ、海外で3万8千台減産 - 中国新聞デジタル",
        "トヨタ、海外で3万8千台減産 中東情勢影響、11月までに｜全国のニュース｜Web東奥 - 東奥日報社",
        "トヨタ、海外で3万8千台減産 中東情勢影響、11月までに（共同通信） - Yahoo!ニュース",
        "トヨタ、海外生産3万8千台減産 5月から約半年間で：ニュース - 中日BIZナビ",
    ]
    hashes = [_normalize_title_hash(t)[0] for t in titles]
    group_hash = hashes[0]
    same = sum(1 for h in [hashes[0], hashes[1], hashes[3], hashes[4]] if h == group_hash)
    assert same >= 4, f"Expected 4+ matching, got {same}. Hashes: {hashes}"


def test_no_body_separator_removal():
    t1, _ = normalize_news_title("A社とB社が業務提携を発表、生産能力を2倍に - 生産ライン再編 - 日経新聞")
    assert "生産能力を2倍に" in t1, f"Body content removed: {t1}"
    t2, _ = normalize_news_title("新製品『X|Y|Z』を発売 | ITmedia")
    assert "X|Y|Z" in t2, f"Body pipe removed: {t2}"


def test_grouping_24h_boundary():
    base = {"title": "テスト記事タイトル - 新聞社A", "url": "http://a.com",
            "source": "A", "keyword": "kw", "id": 1}
    a1 = {**base, "id": 1, "published": "2026-04-20 10:00"}
    a2 = {**base, "id": 2, "published": "2026-04-21 11:00", "source": "B"}
    assert len(_group_syndicated_articles([a1, a2])) == 2

    a3 = {**base, "id": 3, "published": "2026-04-20 10:00", "keyword": "kw1"}
    a4 = {**base, "id": 4, "published": "2026-04-20 10:00", "keyword": "kw2"}
    assert len(_group_syndicated_articles([a3, a4])) == 2

    a5 = {"title": "", "url": "http://x.com", "source": "X", "keyword": "kw",
           "id": 5, "published": "2026-04-20 10:00"}
    a6 = {"title": "", "url": "http://y.com", "source": "Y", "keyword": "kw",
           "id": 6, "published": "2026-04-20 10:00"}
    assert len(_group_syndicated_articles([a5, a6])) == 2

    a7 = {**base, "id": 7, "published": None}
    a8 = {**base, "id": 8, "published": None, "source": "B"}
    assert len(_group_syndicated_articles([a7, a8])) == 2


def test_representative_selection():
    base_title = "テスト記事タイトルですよ - メディアA"
    a1 = {"title": base_title, "url": "http://a.com", "keyword": "kw",
           "id": 1, "published": "2026-04-20 10:00", "source": "B新聞"}
    a2 = {"title": "テスト記事タイトルですよ - メディアB", "url": "http://b.com", "keyword": "kw",
           "id": 2, "published": "2026-04-20 10:00", "source": "A新聞"}
    result = _group_syndicated_articles([a1, a2])
    assert len(result) == 1
    assert result[0]["source"] == "A新聞", f"Got {result[0]['source']}"

    a3 = {"title": base_title, "url": "http://c.com", "keyword": "kw",
           "id": 3, "published": "2026-04-20 10:00", "source": ""}
    a4 = {"title": "テスト記事タイトルですよ - メディアC", "url": "http://d.com", "keyword": "kw",
           "id": 4, "published": "2026-04-20 10:00", "source": "Z新聞"}
    result = _group_syndicated_articles([a3, a4])
    assert len(result) == 1
    assert result[0]["source"] == "Z新聞"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  PASS: {name}")
            except AssertionError as e:
                print(f"  FAIL: {name}: {e}")
    print("Done.")
