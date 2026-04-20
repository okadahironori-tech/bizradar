"""URL自動充足パイプライン"""
import gc
import logging
import os
import re
import time
import unicodedata
from urllib.parse import urlparse, urlencode

import requests
import tldextract
from bs4 import BeautifulSoup

import db

try:
    import pykakasi
    _KKS = pykakasi.kakasi()
except Exception:
    _KKS = None

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_WIKI_UA = "BizRadar/1.0 (https://bizradar-6h9o.onrender.com; bizradarofficial@gmail.com)"

# ── ソースON/OFF ──

_SRC_ENABLED = {
    "jpx":        os.environ.get("URL_ENRICHMENT_JPX_ENABLED", "false").lower() == "true",
    "edinet":     os.environ.get("URL_ENRICHMENT_EDINET_ENABLED", "false").lower() == "true",
    "wikipedia":  os.environ.get("URL_ENRICHMENT_WIKIPEDIA_ENABLED", "true").lower() == "true",
    "google_cse": os.environ.get("URL_ENRICHMENT_GOOGLE_CSE_ENABLED", "false").lower() == "true",
}

# ── 除外ドメイン ──

_EXCLUDE_DOMAINS = {
    "facebook.com", "twitter.com", "x.com", "t.co",
    "linkedin.com", "youtube.com", "instagram.com",
    "note.com", "wantedly.com", "prtimes.jp", "atpress.ne.jp",
    "ja.wikipedia.org", "en.wikipedia.org",
}


def _is_excluded_domain(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        return any(netloc == d or netloc.endswith("." + d) for d in _EXCLUDE_DOMAINS)
    except Exception:
        return False


def _normalize_url_key(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme.lower()}://{p.netloc.lower()}{p.path.rstrip('/')}"
    except Exception:
        return url


def _dedup_candidates(candidates: list) -> list:
    seen = set()
    result = []
    for c in candidates:
        key = _normalize_url_key(c["url"])
        if key not in seen:
            seen.add(key)
            result.append(c)
    return result


# ── 企業名正規化 ──

_COMPANY_STRIP_RE = re.compile(
    r'株式会社|（株）|\(株\)|有限会社|合同会社|ホールディングス|HD|グループ'
    r'|Inc\.|Co\.,?\s*Ltd\.|Corporation',
    re.IGNORECASE
)


def _normalize_company_name(name: str) -> str:
    n = _COMPANY_STRIP_RE.sub('', name)
    n = n.translate(str.maketrans('ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ０１２３４５６７８９',
                                   'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'))
    return re.sub(r'\s+', '', n).lower()


# ── 情報源ごとの候補取得 ──

_WIKI_API = "https://ja.wikipedia.org/w/api.php"


def fetch_candidates_from_wikipedia(company_name: str) -> list:
    try:
        return _wiki_impl(company_name)
    except Exception as e:
        logger.error("[url_enrichment][wikipedia] error: %s", e)
        return []


def _wiki_impl(company_name: str) -> list:
    norm_name = _normalize_company_name(company_name)
    if not norm_name:
        return []

    # Step 1: search
    resp = requests.get(_WIKI_API, params={
        "action": "query", "list": "search", "srsearch": company_name,
        "format": "json", "srlimit": 5,
    }, headers={"User-Agent": _WIKI_UA}, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("query", {}).get("search", [])
    if not results:
        return []
    titles = [r["title"] for r in results]
    time.sleep(1)

    # Step 2: filter disambiguation
    resp = requests.get(_WIKI_API, params={
        "action": "query", "titles": "|".join(titles),
        "prop": "pageprops|categories", "cllimit": 50, "format": "json",
    }, headers={"User-Agent": _WIKI_UA}, timeout=10)
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})
    valid_titles = []
    for pid, page in pages.items():
        if pid == "-1":
            continue
        pp = page.get("pageprops", {})
        if "disambiguation" in pp:
            continue
        cats = [c.get("title", "") for c in page.get("categories", [])]
        if any("曖昧さ回避" in c for c in cats):
            continue
        valid_titles.append(page["title"])
    if not valid_titles:
        return []
    time.sleep(1)

    # Step 3: match company name
    matched = None
    for t in valid_titles:
        if _normalize_company_name(t) == norm_name:
            matched = t
            break
    if not matched:
        for t in valid_titles:
            if norm_name in _normalize_company_name(t) or _normalize_company_name(t) in norm_name:
                matched = t
                break
    if not matched:
        return []

    # Step 4: extlinks (main path)
    resp = requests.get(_WIKI_API, params={
        "action": "query", "titles": matched, "prop": "extlinks",
        "ellimit": 50, "format": "json",
    }, headers={"User-Agent": _WIKI_UA}, timeout=10)
    resp.raise_for_status()
    ext_pages = resp.json().get("query", {}).get("pages", {})
    ext_urls = []
    for pid, page in ext_pages.items():
        for link in page.get("extlinks", []):
            url = link.get("*") or link.get("url", "")
            if url and url.startswith(("http://", "https://")) and not _is_excluded_domain(url):
                ext_urls.append(url)
    time.sleep(1)

    # Step 5: infobox (auxiliary)
    infobox_url = None
    try:
        resp = requests.get(_WIKI_API, params={
            "action": "parse", "page": matched, "prop": "wikitext", "format": "json",
        }, headers={"User-Agent": _WIKI_UA}, timeout=10)
        resp.raise_for_status()
        wt = resp.json().get("parse", {}).get("wikitext", {}).get("*", "")
        for pattern in [
            r'(?:公式ウェブサイト|公式サイト|ホームページ|外部リンク|URL)\s*=\s*\[?(https?://[^\s\]\|<>]+)',
            r'\{\{URL\|(https?://[^\s\}\|<>]+)',
        ]:
            m = re.search(pattern, wt)
            if m:
                candidate = m.group(1).rstrip(']')
                if not _is_excluded_domain(candidate):
                    infobox_url = candidate
                    break
        time.sleep(1)
    except Exception:
        pass

    # Build candidates (infobox first, then extlinks)
    candidates = []
    if infobox_url:
        candidates.append({"source": "wikipedia", "url": infobox_url})

    for url in ext_urls:
        if len(candidates) >= 3:
            break
        if any(c["url"] == url for c in candidates):
            continue
        candidates.append({"source": "wikipedia", "url": url})

    return _dedup_candidates(candidates)[:3]


def fetch_candidates_from_edinet(securities_code: str) -> list:
    return []


def fetch_candidates_from_google_cse(company_name: str) -> list:
    if not _SRC_ENABLED.get("google_cse"):
        return []
    api_key = os.environ.get("GOOGLE_CSE_API_KEY", "").strip()
    cx = os.environ.get("GOOGLE_CSE_CX", "").strip()
    if not api_key or not cx:
        logger.warning("[url_enrichment][google_cse] missing API credentials")
        return []
    try:
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": api_key, "cx": cx,
                    "q": f"{company_name} 公式サイト", "num": 5},
            timeout=10,
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
        candidates = []
        for item in items:
            link = item.get("link", "")
            if not link.startswith(("http://", "https://")):
                continue
            if _is_excluded_domain(link):
                continue
            if "google.com" in link:
                continue
            candidates.append({"source": "google_cse", "url": link})
        return _dedup_candidates(candidates)[:5]
    except Exception as e:
        logger.error("[url_enrichment][google_cse] error: %s", e)
        return []


# ── 到達性チェック ──


def check_url_reachable(url: str) -> dict:
    result = {"reachable": False, "http_status": None, "title": None}
    resp_for_title = None
    # Step 1: HEAD で reachable 判定
    try:
        head_resp = requests.head(url, timeout=10, allow_redirects=True,
                                  headers={"User-Agent": _UA})
        result["http_status"] = head_resp.status_code
        if 200 <= head_resp.status_code < 300:
            result["reachable"] = True
    except Exception:
        pass
    # Step 2: HEAD 失敗時は GET でフォールバック判定
    if not result["reachable"]:
        try:
            resp_for_title = requests.get(url, timeout=10, allow_redirects=True,
                                          headers={"User-Agent": _UA})
            result["http_status"] = resp_for_title.status_code
            if 200 <= resp_for_title.status_code < 300:
                result["reachable"] = True
        except Exception:
            return result
    # Step 3: title 取得用 GET（HEAD 成功時は追加実行）
    if result["reachable"] and resp_for_title is None:
        try:
            resp_for_title = requests.get(url, timeout=10, allow_redirects=True,
                                          headers={"User-Agent": _UA})
        except Exception:
            pass
    # Step 4: title 抽出（GET レスポンスのみ、先頭10KBのみ使用）
    if resp_for_title and hasattr(resp_for_title, "text"):
        try:
            resp_for_title.encoding = resp_for_title.apparent_encoding  # P1
            html_snippet = resp_for_title.text[:10000]
            del resp_for_title
            soup = BeautifulSoup(html_snippet, "html.parser")
            title_tag = soup.find("title")
            if title_tag:
                result["title"] = title_tag.get_text().strip()[:500]
        except Exception:
            pass
    return result


# ── スコアリング ──


_TRUST_SCORES = {
    "jpx": 100,
    "edinet": 90,
    "wikipedia": 70,
    "google_cse": 50,
}


def score_candidate(candidate: dict, company_name: str) -> dict:
    source = candidate.get("source", "")
    url = candidate.get("url", "")
    reachable = candidate.get("reachable")
    title = candidate.get("title") or ""

    source_trust = _TRUST_SCORES.get(source, 30)

    # P3: 企業名正規化を単語単位に変更
    nfkc_name = unicodedata.normalize("NFKC", company_name)
    name_parts = re.sub(
        r'株式会社|（株）|\(株\)|有限会社|合同会社|ホールディングス|グループ|HD|Inc\.|Co\.,\s*Ltd\.|Corporation',
        '', nfkc_name
    )
    name_parts = re.sub(r'[（）()]', '', name_parts).strip()

    domain_match = 0
    try:
        domain = urlparse(url).netloc.lower()
        # P2: tldextract で主要ドメイン部分を抽出
        domain_sld = tldextract.extract(url).domain.lower()
        try:
            if not _KKS:
                raise RuntimeError("pykakasi not available")
            result = _KKS.convert(name_parts)
            romaji = "".join(item.get("hepburn", "") for item in result).lower()
            # P2: 長音正規化 ou→o, uu→u
            normalized_romaji = romaji.replace("ou", "o").replace("uu", "u")
            # P2: 双方向部分一致
            if normalized_romaji and len(normalized_romaji) >= 3 and domain_sld and len(domain_sld) >= 3:
                if normalized_romaji in domain or domain_sld in normalized_romaji:
                    domain_match = 20
        except Exception:
            pass
        if not domain_match:
            ascii_part = re.sub(r'[^a-z0-9]', '', name_parts.lower())
            if ascii_part and len(ascii_part) >= 3 and domain_sld and len(domain_sld) >= 3:
                if ascii_part in domain or domain_sld in ascii_part:
                    domain_match = 20
    except Exception:
        pass

    title_match = 0
    if title and company_name:
        # P3: title比較用にも同じ正規化を適用
        short_name = name_parts.strip()
        if short_name and short_name in title:
            title_match = 30
        elif short_name and len(short_name) >= 2:
            words = [short_name[:len(short_name)//2], short_name[len(short_name)//2:]]
            if any(w in title for w in words if len(w) >= 2):
                title_match = 15

    reachable_pen = 0
    if reachable is True:
        reachable_pen = 20
    elif reachable is False:
        reachable_pen = -50

    total = source_trust + domain_match + title_match + reachable_pen
    return {
        "source_trust_score": source_trust,
        "domain_match_score": domain_match,
        "title_match_score": title_match,
        "reachable_penalty": reachable_pen,
        "total_score": total,
    }


# ── 1社処理 ──


def enrich_company(securities_code: str, company_name: str) -> dict:
    candidates = []

    if _SRC_ENABLED.get("edinet"):
        logger.info("[url_enrichment][edinet] fetching for %s %s", securities_code, company_name)
        c = fetch_candidates_from_edinet(securities_code)
        logger.info("[url_enrichment][edinet] got %d candidates for %s", len(c), securities_code)
        candidates.extend(c)
    else:
        logger.debug("[url_enrichment][edinet] disabled, skipping")

    if _SRC_ENABLED.get("wikipedia"):
        logger.info("[url_enrichment][wikipedia] fetching for %s %s", securities_code, company_name)
        c = fetch_candidates_from_wikipedia(company_name)
        logger.info("[url_enrichment][wikipedia] got %d candidates for %s", len(c), securities_code)
        candidates.extend(c)
    else:
        logger.debug("[url_enrichment][wikipedia] disabled, skipping")

    if _SRC_ENABLED.get("google_cse"):
        logger.info("[url_enrichment][google_cse] fetching for %s %s", securities_code, company_name)
        c = fetch_candidates_from_google_cse(company_name)
        logger.info("[url_enrichment][google_cse] got %d candidates for %s", len(c), securities_code)
        candidates.extend(c)
    else:
        logger.debug("[url_enrichment][google_cse] disabled, skipping")

    candidates = _dedup_candidates(candidates)

    if not candidates:
        logger.info("[url_enrichment] no candidates for %s %s", securities_code, company_name)
        return {"candidates": 0, "result": "no_candidates"}

    for c in candidates:
        check = check_url_reachable(c["url"])
        c["reachable"] = check["reachable"]
        c["http_status"] = check["http_status"]
        c["title"] = check["title"]
        scores = score_candidate(c, company_name)
        c.update(scores)

    with db._conn() as conn:
        with conn.cursor() as cur:
            for c in candidates:
                cur.execute(
                    "INSERT INTO url_enrichment_candidates "
                    "(securities_code, source, candidate_url, reachable, http_status, "
                    "title_text, source_trust_score, domain_match_score, title_match_score, "
                    "reachable_penalty, total_score, status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending') "
                    "ON CONFLICT (securities_code, source, candidate_url) DO UPDATE SET "
                    "reachable=EXCLUDED.reachable, http_status=EXCLUDED.http_status, "
                    "title_text=EXCLUDED.title_text, source_trust_score=EXCLUDED.source_trust_score, "
                    "domain_match_score=EXCLUDED.domain_match_score, "
                    "title_match_score=EXCLUDED.title_match_score, "
                    "reachable_penalty=EXCLUDED.reachable_penalty, "
                    "total_score=EXCLUDED.total_score, "
                    "status=EXCLUDED.status",
                    (securities_code, c["source"], c["url"],
                     c["reachable"], c["http_status"], c.get("title"),
                     c["source_trust_score"], c["domain_match_score"],
                     c["title_match_score"], c["reachable_penalty"], c["total_score"]),
                )

    result = apply_enrichment(securities_code)
    result["candidates"] = len(candidates)
    return result


# ── 自動適用・振り分け ──


def apply_enrichment(securities_code: str) -> dict:
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, candidate_url, total_score FROM url_enrichment_candidates "
                "WHERE securities_code=%s AND status IN ('pending','needs_review') "
                "ORDER BY total_score DESC, id ASC",
                (securities_code,),
            )
            rows = cur.fetchall()
            if not rows:
                return {"result": "no_candidates", "applied_url": None, "top_score": None}

            cur.execute(
                "SELECT website_url FROM listed_companies WHERE securities_code=%s",
                (securities_code,),
            )
            lc = cur.fetchone()
            existing_url = (lc[0] if lc else "").strip() if lc else ""

            top_id, top_url, top_score = rows[0]
            all_ids = [r[0] for r in rows]

            if existing_url:
                cur.execute(
                    "UPDATE url_enrichment_candidates SET status='rejected' "
                    "WHERE id = ANY(%s)",
                    (all_ids,),
                )
                return {"result": "skipped_existing", "applied_url": existing_url, "top_score": top_score}

            if top_score >= 120:
                cur.execute(
                    "UPDATE listed_companies SET website_url=%s WHERE securities_code=%s",
                    (top_url, securities_code),
                )
                cur.execute(
                    "UPDATE url_enrichment_candidates SET status='auto_applied' WHERE id=%s",
                    (top_id,),
                )
                other_ids = [r[0] for r in rows if r[0] != top_id]
                if other_ids:
                    cur.execute(
                        "UPDATE url_enrichment_candidates SET status='rejected' WHERE id = ANY(%s)",
                        (other_ids,),
                    )
                return {"result": "auto_applied", "applied_url": top_url, "top_score": top_score}

            if top_score >= 50:
                cur.execute(
                    "UPDATE url_enrichment_candidates SET status='needs_review' "
                    "WHERE id = ANY(%s)",
                    (all_ids,),
                )
                return {"result": "needs_review", "applied_url": None, "top_score": top_score}

            cur.execute(
                "UPDATE url_enrichment_candidates SET status='rejected' "
                "WHERE id = ANY(%s)",
                (all_ids,),
            )
            return {"result": "rejected", "applied_url": None, "top_score": top_score}


# ── バッチ実行 ──


_CHUNK_SIZE = 20


def _count_unprocessed_targets() -> int:
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM listed_companies lc "
                "WHERE (lc.website_url IS NULL OR lc.website_url = '') "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM url_enrichment_candidates uc "
                "  WHERE uc.securities_code = lc.securities_code"
                ")"
            )
            return cur.fetchone()[0]


def _fetch_next_chunk(chunk_size: int) -> list:
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lc.securities_code, lc.company_name FROM listed_companies lc "
                "WHERE (lc.website_url IS NULL OR lc.website_url = '') "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM url_enrichment_candidates uc "
                "  WHERE uc.securities_code = lc.securities_code"
                ") "
                "ORDER BY lc.securities_code ASC LIMIT %s",
                (chunk_size,),
            )
            return cur.fetchall()


def run_enrichment_batch(limit: int = 100, task_key: str = "batch"):
    logger.info("[url_enrichment] batch start limit=%d key=%s", limit, task_key)
    total = min(_count_unprocessed_targets(), limit) if limit else _count_unprocessed_targets()
    logger.info("[url_enrichment] targets: %d", total)

    stats = {"processed": 0, "auto_applied": 0, "needs_review": 0,
             "rejected": 0, "no_candidates": 0, "failed": 0, "skipped_existing": 0}
    remaining = limit if limit else total

    while remaining > 0:
        chunk_size = min(_CHUNK_SIZE, remaining)
        chunk = _fetch_next_chunk(chunk_size)
        if not chunk:
            break
        for code, name in chunk:
            try:
                result = enrich_company(code, name)
                r = result.get("result", "")
                if r in stats:
                    stats[r] += 1
                stats["processed"] += 1
                logger.info("[url_enrichment] %s %s -> %s score=%s",
                            code, name, r, result.get("top_score"))
            except Exception as e:
                stats["failed"] += 1
                stats["processed"] += 1
                logger.error("[url_enrichment] failed %s %s: %s", code, name, e)
            time.sleep(2)
        remaining -= len(chunk)
        try:
            db.update_enrichment_progress(task_key, stats["processed"], total)
        except Exception:
            pass
        gc.collect()

    try:
        db.update_enrichment_progress(task_key, stats["processed"], total)
    except Exception:
        pass

    logger.info(
        "[url_enrichment] run completed: processed=%d, auto_applied=%d, "
        "needs_review=%d, rejected=%d, no_candidates=%d, failed=%d",
        stats["processed"], stats["auto_applied"], stats["needs_review"],
        stats["rejected"], stats["no_candidates"], stats["failed"],
    )
    return stats
