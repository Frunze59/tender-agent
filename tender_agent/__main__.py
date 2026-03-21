from __future__ import annotations

import argparse
import logging
import sys
from html import escape
from pathlib import Path

from dotenv import load_dotenv

from .config import default_paths, load_config
from .fetchers import _session, fetch_source
from .matchers import item_matches
from .models import TenderItem
from .notify import build_email_html, send_tender_email
from .runner import run_once


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


def cmd_run(args: argparse.Namespace) -> int:
    cfg_path = Path(args.config).resolve()
    db_path = Path(args.db).resolve()
    run_once(cfg_path, db_path)
    return 0


def cmd_dry_run(args: argparse.Namespace) -> int:
    cfg_path = Path(args.config).resolve()
    cfg = load_config(cfg_path)
    session = _session()
    for spec in cfg.get("sources") or []:
        if spec.get("enabled") is False:
            print(
                f"--- {spec.get('id')}: disabled (portal URL: {spec.get('url')}) — set enabled: true and a working rss/html_links config ---"
            )
            continue
        try:
            items = fetch_source(session, spec)
            matched = [
                it
                for it in items
                if item_matches(
                    it,
                    cfg,
                    source_spec=spec,
                    http_session=session,
                )
            ]
            print(f"--- {spec.get('id')} ({spec.get('type')}): {len(items)} items, {len(matched)} pillar matches ---")
            for it in matched[:20]:
                print(f"  {it.title}\n    {it.link}")
            if len(matched) > 20:
                print(f"  ... and {len(matched) - 20} more")
        except Exception as e:
            print(f"ERROR {spec.get('id')}: {e}", file=sys.stderr)
    return 0


def _matched_unique_tenders(cfg_path: Path) -> list[TenderItem]:
    cfg = load_config(cfg_path)
    session = _session()
    sources = cfg.get("sources") or []
    spec_by_id = {str(s.get("id")): s for s in sources if s.get("id") is not None}
    all_items: list[TenderItem] = []
    log = logging.getLogger(__name__)
    for spec in sources:
        if spec.get("enabled") is False:
            continue
        try:
            items = fetch_source(session, spec)
            all_items.extend(items)
        except Exception:
            log.exception("preview-email: failed source %s", spec.get("id"))
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
    return list(by_key.values())


def _demo_tender_items() -> list[TenderItem]:
    """Placeholder rows so you can preview the HTML layout without live pillar matches."""
    return [
        TenderItem(
            "ted_sample",
            "TED",
            "Erasmus+ study visit: capacity building for Baltic-Nordic youth partnerships",
            "https://ted.europa.eu/example-notice",
            "",
            None,
        ),
        TenderItem(
            "prozorro_sample",
            "Prozorro",
            "Training and peer learning — Estonia, mobility programme (Interreg)",
            "https://prozorro.gov.ua/example-tender",
            "",
            None,
        ),
        TenderItem(
            "civic_sample",
            "CIVIC.MD",
            "Cross-border exchange visit: educational trip Moldova–Romania",
            "https://civic.md/example",
            "",
            None,
        ),
    ]


def cmd_preview_email(args: argparse.Namespace) -> int:
    cfg_path = Path(args.config).resolve()
    limit = max(1, int(args.limit))
    if getattr(args, "sample", False):
        items = _demo_tender_items()
    else:
        items = _matched_unique_tenders(cfg_path)
    sample = items[:limit]
    if not sample:
        pillars = load_config(cfg_path).get("keyword_pillars") or {}
        funding = pillars.get("funding")
        funding_hint = ""
        if isinstance(funding, list) and len(funding) > 0:
            funding_hint = (
                " Your config requires a `funding` term in every notice (title/summary/link); "
                "set `funding: []` or remove the `funding` key to only require activity + geography."
            )
        print(
            "No pillar matches to preview — run `dry-run -v` to see per-source counts, "
            "or relax `keyword_pillars`."
            + funding_hint
            + " Or use `--sample` for a fake layout preview.",
            file=sys.stderr,
        )
        return 1
    prefix = (args.subject_prefix or "").strip() or "[Tender agent PREVIEW]"
    cfg = load_config(cfg_path)
    email_cfg = cfg.get("email") or {}
    recipients = list(email_cfg.get("to") or [])

    html_out = (args.html_out or "").strip()
    if html_out:
        subject, body = build_email_html(sample, prefix)
        path = Path(html_out).expanduser().resolve()
        doc = (
            "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
            f"<title>{escape(subject)}</title></head><body>"
            f"<p><strong>Subject:</strong> {escape(subject)}</p>{body}"
            "</body></html>"
        )
        path.write_text(doc, encoding="utf-8")
        print(f"Wrote preview ({len(sample)} tender(s)) to {path}")
        return 0

    send_tender_email(
        sample,
        subject_prefix=prefix,
        recipients=recipients,
    )
    print(f"Sent preview email with {len(sample)} tender(s) to configured recipients.")
    return 0


def cmd_daemon(args: argparse.Namespace) -> int:
    from zoneinfo import ZoneInfo

    from apscheduler.schedulers.blocking import BlockingScheduler

    cfg_path = Path(args.config).resolve()
    db_path = Path(args.db).resolve()
    cfg = load_config(cfg_path)
    sched = cfg.get("schedule") or {}
    hour = int(sched.get("hour", 8))
    minute = int(sched.get("minute", 0))
    tz_name = str(sched.get("timezone") or "local")
    tz = None if tz_name == "local" else ZoneInfo(tz_name)

    def job() -> None:
        run_once(cfg_path, db_path)

    scheduler = BlockingScheduler(timezone=tz)
    scheduler.add_job(job, "cron", hour=hour, minute=minute, id="tender_run")
    logging.getLogger(__name__).info(
        "Scheduler started: daily at %02d:%02d (%s)", hour, minute, tz_name
    )
    scheduler.start()
    return 0


def main() -> int:
    load_dotenv()
    d_cfg, d_db = default_paths()
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--config", default=str(d_cfg), help="Path to config.yaml")
    parent.add_argument("--db", default=str(d_db), help="Path to SQLite dedupe store")
    parent.add_argument("-v", "--verbose", action="store_true")

    parser = argparse.ArgumentParser(description="Tender monitoring agent")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", parents=[parent], help="Fetch, dedupe, email once (use with cron)")
    p_run.set_defaults(func=cmd_run)

    p_dry = sub.add_parser("dry-run", parents=[parent], help="Print matches without email or DB writes")
    p_dry.set_defaults(func=cmd_dry_run)

    p_prev = sub.add_parser(
        "preview-email",
        parents=[parent],
        help="Live matched tenders as a real email or HTML file (does not update dedupe DB)",
    )
    p_prev.add_argument(
        "--limit",
        type=int,
        default=10,
        metavar="N",
        help="Max tenders in the preview (default: 10)",
    )
    p_prev.add_argument(
        "--html-out",
        metavar="FILE",
        default="",
        help="Write preview HTML to this file instead of sending email",
    )
    p_prev.add_argument(
        "--subject-prefix",
        default="",
        help='Email subject prefix (default: "[Tender agent PREVIEW]")',
    )
    p_prev.add_argument(
        "--sample",
        action="store_true",
        help="Skip fetching: use a few fake tenders to preview the email/HTML layout",
    )
    p_prev.set_defaults(func=cmd_preview_email)

    p_daemon = sub.add_parser("daemon", parents=[parent], help="Run daily at schedule.hour/minute (see config)")
    p_daemon.set_defaults(func=cmd_daemon)

    args = parser.parse_args()
    _setup_logging(args.verbose)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
