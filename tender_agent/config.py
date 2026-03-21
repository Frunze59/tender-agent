from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def _validate_locale_overlay(loc: str, overlay: Any) -> None:
    if not isinstance(overlay, dict):
        raise ValueError(f"keyword_pillars.additional_locales.{loc} must be a mapping.")
    for key, val in overlay.items():
        if key not in ("activity", "geography", "funding"):
            raise ValueError(f"Unknown key in additional_locales.{loc}: {key}")
        if val is not None and not isinstance(val, list):
            raise ValueError(f"additional_locales.{loc}.{key} must be a list.")


def _validate_keyword_pillars(raw: Any) -> None:
    if raw is None:
        return
    if not isinstance(raw, dict):
        raise ValueError("keyword_pillars must be a mapping.")
    act = raw.get("activity")
    geo = raw.get("geography")
    if not isinstance(act, list) or not isinstance(geo, list):
        raise ValueError("keyword_pillars.activity and .geography must be lists.")
    if not act or not geo:
        raise ValueError("keyword_pillars requires non-empty 'activity' and 'geography'.")
    fund = raw.get("funding")
    if fund is not None and not isinstance(fund, list):
        raise ValueError("keyword_pillars.funding must be a list when set.")
    extras = raw.get("additional_locales")
    if extras is not None:
        if not isinstance(extras, dict):
            raise ValueError("keyword_pillars.additional_locales must be a mapping.")
        for loc, overlay in extras.items():
            _validate_locale_overlay(str(loc), overlay)


def _validate_translation(raw: Any) -> None:
    if raw is None:
        return
    if not isinstance(raw, dict):
        raise ValueError("translation must be a mapping.")
    prov = (raw.get("provider") or "none")
    if str(prov).lower() not in ("none", "deepl"):
        raise ValueError("translation.provider must be 'none' or 'deepl'.")
    locs = raw.get("content_locales")
    if locs is not None and not isinstance(locs, list):
        raise ValueError("translation.content_locales must be a list.")


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Config must be a mapping at the top level.")
    data.setdefault("keywords", [])
    _validate_keyword_pillars(data.get("keyword_pillars"))
    _validate_translation(data.get("translation"))
    data.setdefault("sources", [])
    data.setdefault("email", {})
    data["email"].setdefault("subject_prefix", "[Tenders]")
    data["email"].setdefault("to", [])
    data.setdefault("translation", {})
    data.setdefault("schedule", {})
    data["schedule"].setdefault("hour", 8)
    data["schedule"].setdefault("minute", 0)
    data["schedule"].setdefault("timezone", "local")
    return data


def default_paths() -> tuple[Path, Path]:
    root = Path(os.environ.get("TENDER_AGENT_ROOT", ".")).resolve()
    cfg = Path(os.environ.get("TENDER_AGENT_CONFIG", str(root / "config.yaml")))
    db = Path(os.environ.get("TENDER_AGENT_DB", str(root / "data" / "seen.db")))
    return cfg, db
