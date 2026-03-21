from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .config import load_config
from .fetchers import _session, fetch_source
from .matchers import item_matches
from .models import TenderItem
from .notify import send_tender_email
from .storage import SeenStore

logger = logging.getLogger(__name__)


def run_once(config_path: Path, db_path: Path) -> int:
    cfg = load_config(config_path)
    sources = cfg.get("sources") or []
    email_cfg = cfg.get("email") or {}

    session = _session()
    spec_by_id = {str(s.get("id")): s for s in sources if s.get("id") is not None}
    all_items: list[TenderItem] = []
    for spec in sources:
        if spec.get("enabled") is False:
            logger.info(
                "Skipping disabled source %s (%s)",
                spec.get("id"),
                spec.get("url") or "",
            )
            continue
        try:
            items = fetch_source(session, spec)
            all_items.extend(items)
            logger.info("Source %s: %d items", spec.get("id"), len(items))
        except Exception:
            logger.exception("Failed source %s", spec.get("id"))

    matched = [
        it
        for it in all_items
        if item_matches(
            it,
            cfg,
            source_spec=spec_by_id.get(it.source_id),
            http_session=session,
        )
    ]
    by_key: dict[str, TenderItem] = {}
    for it in matched:
        k = it.dedupe_key()
        if k not in by_key:
            by_key[k] = it
    unique = list(by_key.values())
    keys = list(by_key.keys())

    to_send: list[TenderItem] = []
    store = SeenStore(db_path)
    try:
        new_keys = store.filter_new(keys)
        new_set = set(new_keys)
        to_send = [it for it in unique if it.dedupe_key() in new_set]
        if to_send:
            send_tender_email(
                to_send,
                subject_prefix=str(email_cfg.get("subject_prefix") or "[Tenders]"),
                recipients=list(email_cfg.get("to") or []),
            )
            store.mark_seen(new_keys)
            logger.info("Sent %d new tender(s)", len(to_send))
        else:
            logger.info("No new tenders after keyword filter and deduplication.")
    finally:
        store.close()

    return len(to_send)
