"""URL自動充足パイプライン"""
import logging
import re
import time

import requests
from bs4 import BeautifulSoup

import db

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ── 情報源ごとの候補取得 ──


def fetch_candidates_from_kabutan(securities_code: str) -> list:
    """Kabutan から公式サイトURLを取得する。"""
    try:
        url = f"https://kabutan.jp/stock/?code={securities_code}"
        resp = requests.get(url, timeout=15, headers={"User-Agent": _UA})
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        for th in soup.find_all("th"):
            if "会社サイト" in th.get_text():
                td = th.find_next_sibling("td")
                if td:
                    a = td.find("a", href=True)
                    if a and a["href"].startswith(("http://", "https://")):
                        return [{"source": "kabutan", "url": a["href"].strip()}]
        return []
    except Exception as e:
        logger.warning("[url_enrichment] kabutan error code=%s: %s", securities_code, e)
        return []


def fetch_candidates_from_edinet(securities_code: str) -> list:
    return []


def fetch_candidates_from_wikipedia(company_name: str) -> list:
    return []


def fetch_candidates_from_google_cse(company_name: str) -> list:
    return []


# ── 到達性チェック ──


def check_url_reachable(url: str) -> dict:
    result = {"reachable": False, "http_status": None, "title": None}
    try:
        resp = requests.head(url, timeout=10, allow_redirects=True,
                             headers={"User-Agent": _UA})
        result["http_status"] = resp.status_code
        if 200 <= resp.status_code < 300:
            result["reachable"] = True
        else:
            resp = requests.get(url, timeout=10, allow_redirects=True,
                                headers={"User-Agent": _UA})
            result["http_status"] = resp.status_code
            if 200 <= resp.status_code < 300:
                result["reachable"] = True
    except Exception:
        try:
            resp = requests.get(url, timeout=10, allow_redirects=True,
                                headers={"User-Agent": _UA})
            result["http_status"] = resp.status_code
            result["reachable"] = 200 <= resp.status_code < 300
        except Exception:
            return result
    if result["reachable"]:
        try:
            if not hasattr(resp, "text"):
                return result
            soup = BeautifulSoup(resp.text[:10000], "html.parser")
            title_tag = soup.find("title")
            if title_tag:
                result["title"] = title_tag.get_text().strip()[:500]
        except Exception:
            pass
    return result


# ── スコアリング ──


_TRUST_SCORES = {
    "kabutan": 80,
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

    domain_match = 0
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower()
        name_parts = re.sub(r'[株式会社グループホールディングス（）\(\)]', '', company_name).strip()
        try:
            import pykakasi
            kks = pykakasi.kakasi()
            result = kks.convert(name_parts)
            romaji = "".join(item.get("hepburn", "") for item in result).lower()
            if romaji and len(romaji) >= 3 and romaji in domain:
                domain_match = 20
        except Exception:
            pass
        if not domain_match:
            ascii_part = re.sub(r'[^a-z0-9]', '', name_parts.lower())
            if ascii_part and len(ascii_part) >= 3 and ascii_part in domain:
                domain_match = 20
    except Exception:
        pass

    title_match = 0
    if title and company_name:
        short_name = re.sub(r'株式会社', '', company_name).strip()
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
    candidates.extend(fetch_candidates_from_kabutan(securities_code))
    candidates.extend(fetch_candidates_from_edinet(securities_code))
    candidates.extend(fetch_candidates_from_wikipedia(company_name))
    candidates.extend(fetch_candidates_from_google_cse(company_name))

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

    import psycopg2
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


def run_enrichment_batch(limit: int = 100):
    logger.info("[url_enrichment] batch start limit=%d", limit)
    with db._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT securities_code, company_name FROM listed_companies "
                "WHERE website_url IS NULL OR website_url = '' "
                "ORDER BY securities_code ASC LIMIT %s",
                (limit,),
            )
            targets = cur.fetchall()

    stats = {"processed": 0, "auto_applied": 0, "needs_review": 0,
             "rejected": 0, "no_candidates": 0, "failed": 0, "skipped_existing": 0}
    for code, name in targets:
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

    logger.info(
        "[url_enrichment] run completed: processed=%d, auto_applied=%d, "
        "needs_review=%d, rejected=%d, no_candidates=%d, failed=%d",
        stats["processed"], stats["auto_applied"], stats["needs_review"],
        stats["rejected"], stats["no_candidates"], stats["failed"],
    )
    return stats
