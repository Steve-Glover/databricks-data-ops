"""Table prefix utilities for developer isolation in shared schemas.

In dev environments, silver/gold tables are prefixed with the developer's
last name to avoid collisions. Bronze tables are shared and never prefixed.

All sources (CLI argument, env var, Spark auto-detect) are expected to
provide a raw identifier such as an email (``first.last@company.org``) or
username.  The last name is extracted and sanitized into a trailing-underscore
prefix like ``"glover_"``.

Resolution order:
  1. Explicit argument (from CLI --table-prefix, passed by DAB)
  2. DATA_OPS_TABLE_PREFIX environment variable (for notebooks/interactive)
  3. Auto-detect from Spark session current_user() (fallback for interactive)
"""

import os
import re
from typing import Optional

from pyspark.sql import SparkSession


def resolve_table_prefix(
    prefix: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> str:
    """Resolve the developer table prefix from available sources.

    Each source value is run through :func:`_extract_last_name` and
    :func:`_sanitize_prefix` so callers can pass a raw email, username,
    or pre-sanitized value and get a consistent result.

    An empty string (from any source) means *no prefix* (sit/prod).

    Args:
        prefix: Raw prefix string (email, username, or pre-sanitized).
                Pass ``""`` to force no prefix.
                Pass ``None`` to fall through to env var / auto-detect.
        spark: SparkSession, required only for the auto-detect fallback.

    Returns:
        Sanitized last-name prefix (e.g. ``"glover_"``) or ``""`` for no prefix.

    Raises:
        RuntimeError: If auto-detect is needed but *spark* is ``None`` and
            no environment variable is set.
    """
    raw: Optional[str] = None

    # 1. Explicit argument
    if prefix is not None:
        raw = prefix
    else:
        # 2. Environment variable
        env_prefix = os.environ.get("DATA_OPS_TABLE_PREFIX")
        if env_prefix is not None:
            raw = env_prefix
        else:
            # 3. Auto-detect from Spark session
            if spark is None:
                raise RuntimeError(
                    "Cannot auto-detect table prefix: no explicit prefix, "
                    "no DATA_OPS_TABLE_PREFIX env var, and no SparkSession provided."
                )
            raw = _get_email_from_spark(spark)

    if raw == "":
        return ""
    return _sanitize_prefix(_extract_last_name(raw))


def prefixed_table_name(
    catalog: str,
    schema: str,
    table: str,
    prefix: str = "",
) -> str:
    """Build a fully-qualified table name with an optional developer prefix.

    Args:
        catalog: Unity Catalog name (e.g. ``"dev"``).
        schema: Schema name (e.g. ``"silver"``).
        table: Base table name (e.g. ``"member_clean"``).
        prefix: Developer prefix (e.g. ``"glover_"``).  Empty string
                means no prefix.

    Returns:
        Fully qualified name, e.g. ``"dev.silver.glover_member_clean"``.
    """
    return f"{catalog}.{schema}.{prefix}{table}"


def _get_email_from_spark(spark: SparkSession) -> str:
    """Retrieve the current user's email from Spark.

    Args:
        spark: Active SparkSession.

    Returns:
        Email string as returned by ``SELECT current_user()``.

    Raises:
        RuntimeError: If ``current_user()`` cannot be determined.
    """
    try:
        return spark.sql("SELECT current_user()").collect()[0][0]
    except Exception as e:
        raise RuntimeError(f"Failed to detect current user from Spark: {e}") from e


def _extract_last_name(raw: str) -> str:
    """Extract the last name from an email or ``first.last`` username.

    ``"steve.glover@company.org"`` -> ``"glover"``
    ``"steve.glover"``            -> ``"glover"``
    ``"glover"``                  -> ``"glover"``

    Args:
        raw: Email address, dotted username, or plain name.

    Returns:
        Last name portion, or the full input if there is no dot separator.
    """
    local_part = raw.split("@")[0] if "@" in raw else raw
    parts = local_part.split(".")
    return parts[-1] if len(parts) > 1 else local_part


def _sanitize_prefix(raw: str) -> str:
    """Sanitize a raw string into a safe table-name prefix.

    Replaces non-alphanumeric/underscore characters with underscores,
    collapses runs, and ensures a trailing underscore separator.

    Args:
        raw: Raw prefix (e.g. ``"glover"``).

    Returns:
        Sanitized prefix with trailing underscore (e.g. ``"glover_"``),
        or ``""`` if the input contains no usable characters.
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", raw)
    sanitized = re.sub(r"_+", "_", sanitized)
    sanitized = sanitized.strip("_")
    if not sanitized:
        return ""
    return f"{sanitized}_"
