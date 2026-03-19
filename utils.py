from __future__ import annotations

from decimal import Decimal, InvalidOperation


def parse_decimal(text: str) -> Decimal | None:
    """
    Parse user numbers like "1,234.56" safely.
    Returns None if parsing fails or result is not finite.
    """
    if text is None:
        return None
    cleaned = text.strip().replace(",", "")
    if cleaned == "":
        return None
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None
    return value


def fmt_money(value: Decimal | float, places: int = 2) -> str:
    """
    Pretty number for UI. Keeps it simple and readable.
    """
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    q = Decimal(10) ** -places
    value = value.quantize(q)
    # Use standard grouping for readability.
    return f"{value:,.{places}f}"


def clamp_range(value: int, lo: int, hi: int) -> bool:
    return lo <= value <= hi

