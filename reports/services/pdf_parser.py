from __future__ import annotations

import re


_RE_NUM = re.compile(r"-?\d[\d\s\u00A0]*[.,]?\d*")
# Money tokens with 2 decimals.
# Important guards:
# - (?<!\d) : token cannot start inside another long digit sequence
# - (?!\d)  : token cannot continue into extra digits
_RE_MONEY = re.compile(r"(?<!\d)-?(?:\d{1,3}(?:[ \u00A0]\d{3})+|\d+)[.,]\d{2}(?!\d)")


def _to_float(text: str) -> float | None:
    if not text:
        return None
    m = _RE_NUM.search(text)
    if not m:
        return None
    token = m.group(0).replace("\u00A0", "").replace(" ", "").replace(",", ".")
    try:
        return float(token)
    except Exception:
        return None


def _extract_numbers(text: str) -> list[float]:
    values: list[float] = []
    for m in _RE_NUM.finditer(text):
        token = m.group(0).replace("\u00A0", "").replace(" ", "").replace(",", ".")
        try:
            values.append(float(token))
        except Exception:
            continue
    return values


def _extract_money_values(text: str) -> list[float]:
    """
    Extract money-like values only (e.g. 470 255,91), excluding plain integers
    like dates/report numbers that often appear in the same line.
    """
    values: list[float] = []
    for m in _RE_MONEY.finditer(text):
        token = m.group(0).replace("\u00A0", "").replace(" ", "").replace(",", ".")
        try:
            values.append(float(token))
        except Exception:
            continue
    return values


def _find_amount_after_label(content: str, label: str, prefer_last: bool = True) -> float | None:
    for line in content.splitlines():
        if label.lower() in line.lower():
            # Prefer money-formatted numbers to avoid grabbing date/doc numbers.
            values = _extract_money_values(line)
            if not values:
                # Fallback for uncommon formatting.
                values = _extract_numbers(line)
            if not values:
                return None
            return values[-1] if prefer_last else values[0]
    return None


def parse_pdf_file(pdf_file) -> dict:
    """
    Extract values from PDF text (no OCR):
    - "Итого стоимость реализованного товара и услуг"
    - row 2.1
    - row 4.5
    """
    try:
        import pdfplumber
    except Exception as exc:
        raise RuntimeError("Не установлена библиотека pdfplumber для разбора PDF.") from exc

    with pdfplumber.open(pdf_file) as pdf:
        text = "\n".join((page.extract_text() or "") for page in pdf.pages)

    total_realized = _find_amount_after_label(
        text, "Итого стоимость реализованного товара и услуг", prefer_last=True
    )
    row_21 = _find_amount_after_label(text, "2.1", prefer_last=True)
    row_45 = _find_amount_after_label(text, "4.5", prefer_last=True)

    return {
        "total_realized_amount": total_realized,
        "row_2_1": row_21,
        "row_4_5": row_45,
    }
