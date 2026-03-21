from __future__ import annotations

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape

from .models import TenderItem


def _smtp_settings() -> dict[str, str | int | bool]:
    host = os.environ.get("SMTP_HOST", "").strip()
    port_raw = (os.environ.get("SMTP_PORT") or "587").strip()
    port = int(port_raw or "587")
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "")
    use_raw = (os.environ.get("SMTP_USE_TLS") or "true").strip()
    use_tls = use_raw.lower() in ("1", "true", "yes")
    from_addr = os.environ.get("SMTP_FROM", "").strip()
    if not host or not from_addr:
        raise RuntimeError("SMTP_HOST and SMTP_FROM must be set in the environment.")
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "use_tls": use_tls,
        "from_addr": from_addr,
    }


def _recipients(config_to: list[str]) -> list[str]:
    raw = os.environ.get("EMAIL_TO", "").strip()
    env_list = [x.strip() for x in raw.split(",") if x.strip()]
    merged = [x.strip() for x in config_to if x.strip()] + env_list
    seen: set[str] = set()
    out: list[str] = []
    for e in merged:
        if e not in seen:
            seen.add(e)
            out.append(e)
    if not out:
        raise RuntimeError("No email recipients: set email.to in config or EMAIL_TO in the environment.")
    return out


def build_email_html(items: list[TenderItem], subject_prefix: str) -> tuple[str, str]:
    lines = []
    for it in items:
        safe_title = escape(it.title)
        safe_link = escape(it.link, quote=True)
        safe_src = escape(it.source_name)
        lines.append(
            f"<li><strong>{safe_src}</strong>: <a href=\"{safe_link}\">{safe_title}</a></li>"
        )
    body = "<ul>" + "".join(lines) + "</ul>"
    subject = f"{subject_prefix} {len(items)} new tender(s)"
    return subject, body


def send_tender_email(
    items: list[TenderItem],
    *,
    subject_prefix: str,
    recipients: list[str],
) -> None:
    if not items:
        return
    cfg = _smtp_settings()
    to_list = _recipients(recipients)
    subject, html = build_email_html(items, subject_prefix)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = str(cfg["from_addr"])
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(cfg["host"], int(cfg["port"]), timeout=60) as smtp:
        if cfg["use_tls"]:
            smtp.starttls()
        if cfg["user"]:
            smtp.login(str(cfg["user"]), str(cfg["password"]))
        smtp.sendmail(str(cfg["from_addr"]), to_list, msg.as_string())
