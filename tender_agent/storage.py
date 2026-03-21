from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Iterable


def _digest(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


class SeenStore:
    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen (
                digest TEXT PRIMARY KEY,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def filter_new(self, keys: Iterable[str]) -> list[str]:
        keys = list(keys)
        if not keys:
            return []
        digests = [_digest(k) for k in keys]
        placeholders = ",".join("?" * len(digests))
        cur = self._conn.execute(
            f"SELECT digest FROM seen WHERE digest IN ({placeholders})",
            digests,
        )
        known = {row[0] for row in cur.fetchall()}
        return [k for k, d in zip(keys, digests) if d not in known]

    def mark_seen(self, keys: Iterable[str]) -> None:
        for k in keys:
            self._conn.execute(
                "INSERT OR IGNORE INTO seen (digest) VALUES (?)",
                (_digest(k),),
            )
        self._conn.commit()
