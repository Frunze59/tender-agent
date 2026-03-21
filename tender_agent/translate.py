from __future__ import annotations

import os
from typing import Any

import requests

from .models import TenderItem


def translation_should_run(cfg: dict[str, Any], source_spec: dict[str, Any]) -> bool:
    t = cfg.get("translation") or {}
    if (t.get("provider") or "none").lower() != "deepl":
        return False
    loc = (source_spec.get("content_locale") or "").strip().lower()
    allowed = {str(x).lower() for x in (t.get("content_locales") or [])}
    return bool(loc) and loc in allowed


def _deepl_api_url(cfg: dict[str, Any]) -> str:
    t = cfg.get("translation") or {}
    url = t.get("deepl_api_url") or os.environ.get("DEEPL_API_URL")
    if url:
        return str(url).rstrip("/")
    return "https://api-free.deepl.com"


def translate_item_for_match(
    item: TenderItem,
    cfg: dict[str, Any],
    session: requests.Session,
) -> TenderItem:
    key = (os.environ.get("DEEPL_AUTH_KEY") or os.environ.get("DEEPL_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("Set DEEPL_AUTH_KEY or DEEPL_API_KEY for DeepL translation.")

    t = cfg.get("translation") or {}
    target = str(t.get("target_lang") or "EN").upper()
    base = _deepl_api_url(cfg)
    parts = [item.title or "", item.summary or ""]
    if not any(p.strip() for p in parts):
        return item

    form: list[tuple[str, str]] = [("auth_key", key), ("target_lang", target)]
    for p in parts:
        form.append(("text", p))

    r = session.post(f"{base}/v2/translate", data=form, timeout=60)
    r.raise_for_status()
    data = r.json()
    trans = data.get("translations") or []
    if len(trans) < len(parts):
        raise RuntimeError("DeepL returned fewer segments than requested.")

    new_title = (trans[0].get("text") or parts[0]).strip() or item.title
    new_summary = (trans[1].get("text") or parts[1]).strip() if len(trans) > 1 else (parts[1] or "").strip()

    return TenderItem(
        source_id=item.source_id,
        source_name=item.source_name,
        title=new_title,
        link=item.link,
        summary=new_summary,
        published=item.published,
    )
