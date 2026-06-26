"""F1 TV authentication module — token lifecycle management."""

import json
import os
import base64
import logging
import time

logger = logging.getLogger(__name__)

F1_AUTH_URL = "https://api.formula1.com/v2/account/subscriber/authenticate/by-password"
F1_LOGIN_PAGE = "https://account.formula1.com/#/en/login"


def is_token_valid(token: str) -> bool:
    """Check if a F1 JWT token is still valid (not expired)."""
    if not token:
        return False
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return False
        padding = "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + padding))
        exp = payload.get("exp", 0)
        return exp > time.time()
    except Exception:
        return False


def get_token_from_env() -> str:
    """Read F1_TOKEN from the environment."""
    return os.environ.get("F1_TOKEN", "")


def login_f1(email: str, password: str):
    """Attempt HTTP login to F1 TV.

    Note: F1's auth API is behind CloudFront WAF with bot control.
    Python HTTP clients (httpx, requests, cloudscraper, curl_cffi) are
    typically blocked (HTTP 403).  When that happens this function logs
    a warning and returns None.

    The canonical way to obtain a fresh token is:
        python scripts/auto_login.py

    Returns the subscriptionToken on success, None otherwise.
    """
    if not email or not password:
        logger.warning("F1_EMAIL or F1_PASSWORD not set — skipping")
        return None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Origin": "https://account.formula1.com",
        "Referer": "https://account.formula1.com/",
        "Accept": "application/json, text/plain, */*",
    }
    payload = {"Login": email, "Password": password}

    try:
        import httpx

        with httpx.Client(verify=True, timeout=15) as client:
            client.get(F1_LOGIN_PAGE, headers=headers)
            resp = client.post(F1_AUTH_URL, json=payload, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            t = data.get("data", {}).get("subscriptionToken", "")
            if t:
                logger.info("F1 auto-login succeeded (%d chars)", len(t))
                return t
            logger.warning("Login response missing subscriptionToken")
        elif resp.status_code == 403:
            logger.warning(
                "F1 auth blocked by CloudFront WAF (HTTP 403). "
                "Run python scripts/auto_login.py locally."
            )
        elif resp.status_code == 401:
            logger.warning("F1 login failed — bad credentials (401)")
        else:
            logger.warning("F1 login HTTP %d", resp.status_code)
    except ImportError:
        logger.error("httpx not installed")
    except Exception as exc:
        logger.warning("F1 login error: %s", exc)

    return None


def ensure_valid_token() -> str:
    """Return a valid F1 token, auto-refreshing if possible.

    1. Read F1_TOKEN from env.
    2. If valid → return it.
    3. If expired and F1_EMAIL + F1_PASSWORD are set → call login_f1().
       On success update os.environ so the caller sees the new token.
    4. Return whatever we have (may be empty).
    """
    token = get_token_from_env()

    if is_token_valid(token):
        return token

    email = os.environ.get("F1_EMAIL")
    password = os.environ.get("F1_PASSWORD")

    if not email or not password:
        logger.info(
            "F1_TOKEN expired and F1_EMAIL/PASSWORD not set. "
            "Update F1_TOKEN manually in Render dashboard."
        )
        return token

    logger.info("F1_TOKEN expired — attempting auto-login ...")
    new_token = login_f1(email, password)

    if new_token:
        token = new_token
        os.environ["F1_TOKEN"] = token
        logger.info("Auto-login succeeded, token updated in memory")
    else:
        logger.warning(
            "Auto-login failed (CloudFront WAF blocks non-browser clients). "
            "Refresh manually: run scripts/auto_login.py locally."
        )

    return token
