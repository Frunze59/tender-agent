from __future__ import annotations

import logging
import re
from typing import Any, Iterable

import requests

from .models import TenderItem
from .translate import translate_item_for_match, translation_should_run

logger = logging.getLogger(__name__)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()


def _haystack(item: TenderItem) -> str:
    return _normalize(f"{item.title} {item.summary} {item.link}")


def matches_keywords(item: TenderItem, keywords: Iterable[str]) -> bool:
    kws = [_normalize(k) for k in keywords if _normalize(k)]
    if not kws:
        return True
    hay = _haystack(item)
    return any(kw in hay for kw in kws)


def matches_pillars(item: TenderItem, pillars: dict[str, Any]) -> bool:
    """
    AND across pillars that apply; OR within each pillar.
    `funding` is skipped when missing or an empty list (optional pillar).
    """
    hay = _haystack(item)
    for name in ("activity", "geography"):
        raw = pillars.get(name) or []
        terms = [_normalize(t) for t in raw if _normalize(str(t))]
        if not terms:
            return False
        if not any(t in hay for t in terms):
            return False

    funding_raw = pillars.get("funding")
    if funding_raw:
        terms = [_normalize(t) for t in funding_raw if _normalize(str(t))]
        if terms and not any(t in hay for t in terms):
            return False

    return True


def _primary_pillar_dict(pillars: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in pillars.items() if k != "additional_locales"}


def matches_pillars_any_locale(item: TenderItem, pillars: dict[str, Any]) -> bool:
    """
    OR across language variants: primary (usually English) pillars, plus each
    `additional_locales` entry merged on top (non-empty lists override that pillar).
    """
    primary = _primary_pillar_dict(pillars)
    variants: list[dict[str, Any]] = [primary]
    extras = pillars.get("additional_locales") or {}
    if isinstance(extras, dict):
        for overlay in extras.values():
            if not isinstance(overlay, dict):
                continue
            merged = dict(primary)
            changed = False
            for key in ("activity", "geography", "funding"):
                olist = overlay.get(key)
                if isinstance(olist, list) and olist:
                    merged[key] = olist
                    changed = True
            if changed:
                variants.append(merged)
    return any(matches_pillars(item, v) for v in variants)


def item_matches(
    item: TenderItem,
    cfg: dict[str, Any],
    *,
    source_spec: dict[str, Any] | None = None,
    http_session: requests.Session | None = None,
) -> bool:
    pillars = cfg.get("keyword_pillars")
    if isinstance(pillars, dict) and pillars.get("activity") and pillars.get("geography"):
        if (
            source_spec
            and http_session
            and translation_should_run(cfg, source_spec)
        ):
            try:
                trans_item = translate_item_for_match(item, cfg, http_session)
                primary = _primary_pillar_dict(pillars)
                return matches_pillars(trans_item, primary)
            except Exception:
                logger.warning(
                    "Translation failed for %s; using locale keyword variants",
                    item.source_id,
                    exc_info=True,
                )
        return matches_pillars_any_locale(item, pillars)
    return matches_keywords(item, cfg.get("keywords") or [])
