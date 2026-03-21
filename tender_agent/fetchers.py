from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import feedparser
import requests
from bs4 import BeautifulSoup

from .models import TenderItem

logger = logging.getLogger(__name__)

USER_AGENT = "tender-agent/0.1 (+https://github.com/local/tender-agent)"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "timetuple"):
        try:
            return datetime.fromtimestamp(value.timestamp())
        except Exception:
            return None
    return None


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _parse_publication_date(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _ted_pick_title(ti: Any, lang: str) -> str:
    if not isinstance(ti, dict):
        return (str(ti) if ti else "").strip()
    lang_l = lang.lower()
    for key in (lang_l, lang.upper(), "eng", "ENG"):
        v = ti.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for v in ti.values():
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _ted_notice_url(links: Any, nd: str) -> str:
    if isinstance(links, dict):
        html = links.get("html")
        if isinstance(html, dict):
            for k in ("ENG", "eng"):
                u = html.get(k)
                if isinstance(u, str) and u.startswith("http"):
                    return u
    if nd:
        return f"https://ted.europa.eu/en/notice/-/detail/{nd}"
    return "https://ted.europa.eu"


def fetch_ted_search(
    session: requests.Session,
    source_id: str,
    source_name: str,
    spec: dict[str, Any],
) -> list[TenderItem]:
    base = str(spec.get("api_base") or "https://api.ted.europa.eu").rstrip("/")
    lookback = int(spec.get("lookback_days", 7))
    pd_from = (datetime.now(timezone.utc).date() - timedelta(days=lookback)).strftime("%Y%m%d")
    if spec.get("query"):
        query = str(spec["query"])
    else:
        tmpl = str(spec.get("query_template") or "PD >= {pd_from}")
        query = tmpl.format(pd_from=pd_from)
    fields = spec.get("fields") or ["ND", "TI", "publication-date", "links"]
    limit = int(spec.get("limit", 40))
    max_pages = int(spec.get("max_pages", 1))
    title_lang = str(spec.get("title_lang") or "eng")
    out: list[TenderItem] = []
    for page in range(1, max_pages + 1):
        body: dict[str, Any] = {"query": query, "fields": fields, "page": page, "limit": limit}
        r = session.post(f"{base}/v3/notices/search", json=body, timeout=90)
        r.raise_for_status()
        data = r.json()
        notices = data.get("notices") or []
        if not notices:
            break
        for n in notices:
            nd = str(n.get("ND") or n.get("publication-number") or "").strip()
            title = _ted_pick_title(n.get("TI"), title_lang)
            link = _ted_notice_url(n.get("links"), nd)
            pub = _parse_publication_date(n.get("publication-date"))
            if not title and nd:
                title = nd
            if not title and not link:
                continue
            out.append(
                TenderItem(
                    source_id=source_id,
                    source_name=source_name,
                    title=title or link,
                    link=link,
                    summary="",
                    published=pub,
                )
            )
    return out


def fetch_prozorro_tenders(
    session: requests.Session,
    source_id: str,
    source_name: str,
    spec: dict[str, Any],
) -> list[TenderItem]:
    base = str(spec.get("api_base") or "https://api.prozorro.gov.ua").rstrip("/")
    limit = int(spec.get("limit", 30))
    r = session.get(
        f"{base}/api/2.5/tenders",
        params={"limit": limit, "descending": "1"},
        timeout=90,
    )
    r.raise_for_status()
    rows = r.json().get("data") or []
    out: list[TenderItem] = []
    for row in rows:
        tid = (row.get("id") or "").strip()
        if not tid:
            continue
        dr = session.get(f"{base}/api/2.5/tenders/{tid}", timeout=90)
        dr.raise_for_status()
        t = dr.json().get("data") or {}
        title = (t.get("title") or "").strip()
        desc = (t.get("description") or "").strip()
        tender_id = (t.get("tenderID") or tid).strip()
        link = f"https://prozorro.gov.ua/en/tender/{tender_id}"
        pub = _parse_iso_datetime(t.get("dateModified")) or _parse_iso_datetime(t.get("date"))
        if not title:
            continue
        out.append(
            TenderItem(
                source_id=source_id,
                source_name=source_name,
                title=title,
                link=link,
                summary=desc,
                published=pub,
            )
        )
    return out


def _mtender_release_tender(pkg: dict[str, Any]) -> tuple[str, str, str, datetime | None]:
    ocid = ""
    best_title = ""
    best_desc = ""
    best_pub: datetime | None = None
    for rec in pkg.get("records") or []:
        cr = rec.get("compiledRelease") or {}
        oid = str(cr.get("ocid") or rec.get("ocid") or "").strip()
        if oid:
            ocid = oid
        tender = cr.get("tender") or {}
        title = (tender.get("title") or "").strip()
        desc = (tender.get("description") or "").strip()
        pub = _parse_iso_datetime(cr.get("date")) or _parse_publication_date(cr.get("date"))
        pick = title or desc
        if pick and (not best_title or len(pick) > len(best_title)):
            best_title, best_desc, best_pub = title or desc, desc, pub
    return best_title, best_desc, ocid, best_pub


def fetch_mtender_ocds(
    session: requests.Session,
    source_id: str,
    source_name: str,
    spec: dict[str, Any],
) -> list[TenderItem]:
    list_url = spec.get("list_url") or "https://public.mtender.gov.md/tenders/"
    if not str(list_url).endswith("/"):
        list_url = str(list_url) + "/"
    lookback = int(spec.get("lookback_days", 7))
    limit = int(spec.get("limit", 25))
    max_pages = int(spec.get("max_pages", 5))
    since = datetime.now(timezone.utc) - timedelta(days=lookback)
    offset = spec.get("list_offset") or since.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    out: list[TenderItem] = []
    for _ in range(max_pages):
        r = session.get(
            list_url,
            params={"limit": limit, "offset": offset},
            timeout=90,
        )
        r.raise_for_status()
        j = r.json()
        chunk = j.get("data") or []
        if not chunk:
            break
        for item in chunk:
            ocid = (item.get("ocid") or "").strip()
            if not ocid:
                continue
            detail = session.get(f"{list_url}{ocid}", timeout=90)
            detail.raise_for_status()
            pkg = detail.json()
            title, desc, oid, pub = _mtender_release_tender(pkg)
            if not oid:
                oid = ocid
            link = f"https://mtender.gov.md/en/tenders/{oid}"
            if not title:
                title = oid
            out.append(
                TenderItem(
                    source_id=source_id,
                    source_name=source_name,
                    title=title,
                    link=link,
                    summary=desc,
                    published=pub,
                )
            )
        offset = j.get("offset")
        if not offset:
            break
    return out


def fetch_rss(
    session: requests.Session,
    source_id: str,
    source_name: str,
    url: str,
) -> list[TenderItem]:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    parsed = feedparser.parse(r.content)
    if getattr(parsed, "bozo", False) and not parsed.entries:
        logger.warning("RSS parse issue for %s (%s): %s", source_id, url, getattr(parsed, "bozo_exception", None))
    out: list[TenderItem] = []
    for e in parsed.entries or []:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        summary = (e.get("summary") or e.get("description") or "").strip()
        published = _parse_dt(e.get("published_parsed") or e.get("updated_parsed"))
        if not title and not link:
            continue
        out.append(
            TenderItem(
                source_id=source_id,
                source_name=source_name,
                title=title or link,
                link=link or url,
                summary=summary,
                published=published,
            )
        )
    return out


def fetch_html_links(
    session: requests.Session,
    source_id: str,
    source_name: str,
    url: str,
    link_selector: str,
    item_prefix: str | None = None,
) -> list[TenderItem]:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out: list[TenderItem] = []
    for a in soup.select(link_selector):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(url, href)
        if item_prefix and not full.startswith(item_prefix):
            continue
        title = a.get_text(" ", strip=True) or full
        out.append(
            TenderItem(
                source_id=source_id,
                source_name=source_name,
                title=title,
                link=full,
                summary="",
                published=None,
            )
        )
    return out


def fetch_source(session: requests.Session, spec: dict[str, Any]) -> list[TenderItem]:
    sid = spec["id"]
    name = spec.get("name") or sid
    typ = spec.get("type") or "rss"
    url = spec.get("url") or ""
    if typ == "rss":
        if not url:
            raise ValueError(f"Source {sid}: rss requires url")
        return fetch_rss(session, sid, name, url)
    if typ == "html_links":
        if not url:
            raise ValueError(f"Source {sid}: html_links requires url")
        return fetch_html_links(
            session,
            sid,
            name,
            url,
            link_selector=spec["link_selector"],
            item_prefix=spec.get("item_prefix"),
        )
    if typ == "ted_search":
        return fetch_ted_search(session, sid, name, spec)
    if typ == "prozorro_tenders":
        return fetch_prozorro_tenders(session, sid, name, spec)
    if typ == "mtender_ocds":
        return fetch_mtender_ocds(session, sid, name, spec)
    raise ValueError(f"Unknown source type for {sid}: {typ}")
