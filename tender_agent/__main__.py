from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from .config import default_paths, load_config
from .fetchers import _session, fetch_source
from .matchers import item_matches
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

    p_daemon = sub.add_parser("daemon", parents=[parent], help="Run daily at schedule.hour/minute (see config)")
    p_daemon.set_defaults(func=cmd_daemon)

    args = parser.parse_args()
    _setup_logging(args.verbose)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
