"""Email extraction and validation utilities."""

from __future__ import annotations

import re

# Email regex — matches most valid addresses
EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

# Patterns to exclude
EXCLUDE_PATTERNS = {
    "noreply@", "no-reply@", "mailer-daemon@",
    "example.com", "example.org",
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js",
    "wixpress.com", "sentry.io",
}

# Generic prefixes (lower priority)
GENERIC_PREFIXES = {
    "info", "office", "kontakt", "contact", "hello", "mail",
    "support", "service", "admin", "webmaster", "postmaster",
}


def extract_emails(text: str) -> list[str]:
    """Extract valid email addresses from text, sorted by priority."""
    raw = EMAIL_RE.findall(text)
    valid = []
    seen = set()

    for email in raw:
        email_lower = email.lower()
        if email_lower in seen:
            continue
        seen.add(email_lower)

        # Skip excluded patterns
        if any(pat in email_lower for pat in EXCLUDE_PATTERNS):
            continue

        # Basic length check
        if len(email) > 254:
            continue

        valid.append(email)

    # Sort: personal emails first, generic last
    def priority(e: str) -> int:
        prefix = e.split("@")[0].lower()
        return 1 if prefix in GENERIC_PREFIXES else 0

    valid.sort(key=priority)
    return valid
