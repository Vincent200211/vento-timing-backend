"""F1 topic data decoding (base64 + zlib or plain JSON)."""
from __future__ import annotations
import base64
import json
import logging
import zlib

logger = logging.getLogger(__name__)


def decode_topic_data(raw: str) -> dict:
    """Decode a topic data payload.

    Tries in order:
      1. Plain JSON
      2. Stripped quotes + JSON
      3. Base64 (auto-fix padding, urlsafe) → JSON (no compression)
      4. Base64 → raw deflate / zlib / gzip
      5. Returns {} on complete failure.
    """
    raw = raw.strip()

    # 1. Plain JSON
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        pass

    # 2. Strip surrounding quotes (if any) and parse as JSON
    if raw.startswith('"') and raw.endswith('"'):
        try:
            return json.loads(raw[1:-1])
        except (json.JSONDecodeError, ValueError):
            pass

    # 3. Base64 decode (standard, padding-fixed, then urlsafe)
    decoded = None
    for b64_fn, label in [
        (lambda s: base64.b64decode(s), "standard"),
        (lambda s: base64.b64decode(s + "=" * ((4 - len(s) % 4) % 4)), "padded"),
        (lambda s: base64.urlsafe_b64decode(s), "urlsafe"),
    ]:
        try:
            decoded = b64_fn(raw)
            break
        except Exception:
            continue

    if decoded is None:
        logger.warning("Base64 decode failed: all variants exhausted")
        return {}

    # 4. Base64 → plain JSON (no compression)
    try:
        return json.loads(decoded.decode("utf-8", errors="replace"))
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        pass

    # 5. Base64 → various decompression formats
    for wbits in [-zlib.MAX_WBITS, zlib.MAX_WBITS, 16 + zlib.MAX_WBITS, 15 + 32]:
        try:
            decompressed = zlib.decompress(decoded, wbits)
            return json.loads(decompressed.decode("utf-8-sig"))
        except Exception:
            continue

    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"Failed to decode topic data: all formats failed — raw[:200]={repr(raw[:200])}")
    return {}
