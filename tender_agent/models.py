from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class TenderItem:
    source_id: str
    source_name: str
    title: str
    link: str
    summary: str
    published: Optional[datetime]

    def dedupe_key(self) -> str:
        link = (self.link or "").strip()
        if link:
            return link
        return f"{self.source_id}\0{self.title.strip()}"
