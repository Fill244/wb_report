from __future__ import annotations

import re
from typing import Callable

import pandas as pd


PARTNERS = ("a", "b", "c", "d")
_PARTNER_SET = frozenset(PARTNERS)
_RE_NORM_CHARS = re.compile(r"[^0-9a-zа-я ]+")
_RE_NORM_SPACES = re.compile(r"\s+")
_RE_NUM_CLEAN = re.compile(r"[^0-9.\-]+")


def _normalize_name(name: str) -> str:
    s = str(name).strip().lower()
    s = s.replace("\u00A0", " ").replace("ё", "е")
    s = _RE_NORM_CHARS.sub(" ", s)
    return _RE_NORM_SPACES.sub(" ", s).strip()


def _to_float(value) -> float:
    if value is None:
        return 0.0
    s = str(value).strip().replace(",", ".")
    s = _RE_NUM_CLEAN.sub("", s)
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _extract_partner(value) -> str | None:
    if value is None:
        return None
    s = str(value)
    if not s:
        return None
    for ch in reversed(s):
        o = ord(ch)
        if 65 <= o <= 68:
            return chr(o | 32)
        if 97 <= o <= 100:
            return ch
    return None


def _find_column(df: pd.DataFrame, keyword_groups: list[list[str]]) -> str | None:
    normalized_groups = [[_normalize_name(k) for k in group] for group in keyword_groups]
    for col in df.columns:
        norm_col = _normalize_name(col)
        for group in normalized_groups:
            if any(k in norm_col for k in group):
                return col
    return None


def _find_article_like_column(df: pd.DataFrame) -> str | None:
    """
    Fallback detector for article column when headers are noisy/missing.
    Picks a column where values most often contain partner marker a/b/c/d.
    """
    best_col = None
    best_score = 0
    sample_size = min(len(df), 300)
    if sample_size == 0:
        return None

    for col in df.columns:
        vals = df[col].tolist()[:sample_size]
        score = 0
        for v in vals:
            if _extract_partner(v) is not None:
                score += 1
        if score > best_score:
            best_score = score
            best_col = col

    # Require at least a few hits to avoid false positives.
    return best_col if best_score >= 3 else None


def process_report(
    file_obj,
    mode: int,
    prepare_df: Callable,
    build_result: Callable,
) -> dict:
    """
    Wrapper over existing hot-path logic.
    mode=1 -> standard
    mode=2 -> exclude USN + wb_sales_amount + wb_returns_amount
    """
    df = prepare_df(file_obj)
    result = build_result(df)

    if mode == 2:
        partners = result.get("partners", {})
        for p in PARTNERS:
            row = partners.get(p)
            if not row:
                continue
            # File 2 rule: don't include these metrics in merged totals.
            row["commission"] = 0.0
            row["wb_sales_amount"] = 0.0
            row["wb_returns_amount"] = 0.0
    return result


def merge_reports(base: dict, extra: dict) -> dict:
    merged = dict(base)

    merged["overall_storage_amount"] = round(
        float(base.get("overall_storage_amount", 0)) + float(extra.get("overall_storage_amount", 0)),
        2,
    )
    merged["overall_receiving_amount"] = round(
        float(base.get("overall_receiving_amount", 0)) + float(extra.get("overall_receiving_amount", 0)),
        2,
    )
    merged["product_cost_sales"] = round(
        float(base.get("product_cost_sales", 0)) + float(extra.get("product_cost_sales", 0)),
        2,
    )
    merged["product_cost_returns"] = round(
        float(base.get("product_cost_returns", 0)) + float(extra.get("product_cost_returns", 0)),
        2,
    )

    base_missing = set(base.get("missing_products", []) or [])
    extra_missing = set(extra.get("missing_products", []) or [])
    merged["missing_products"] = sorted(base_missing | extra_missing)

    merged["details"] = list(base.get("details", []) or []) + list(extra.get("details", []) or [])

    merged_partners = {}
    for p in PARTNERS:
        lrow = (base.get("partners", {}) or {}).get(p, {})
        rrow = (extra.get("partners", {}) or {}).get(p, {})
        merged_partners[p] = {
            "sales_amount": round(float(lrow.get("sales_amount", 0)) + float(rrow.get("sales_amount", 0)), 2),
            "returns_amount": round(float(lrow.get("returns_amount", 0)) + float(rrow.get("returns_amount", 0)), 2),
            "net_amount": round(float(lrow.get("net_amount", 0)) + float(rrow.get("net_amount", 0)), 2),
            "commission": round(float(lrow.get("commission", 0)) + float(rrow.get("commission", 0)), 2),
            "total_amount": round(float(lrow.get("total_amount", 0)) + float(rrow.get("total_amount", 0)), 2),
            "delivery_amount": round(float(lrow.get("delivery_amount", 0)) + float(rrow.get("delivery_amount", 0)), 2),
            "wb_realized_amount": round(
                float(lrow.get("wb_realized_amount", 0)) + float(rrow.get("wb_realized_amount", 0)), 2
            ),
            "wb_sales_amount": round(float(lrow.get("wb_sales_amount", 0)) + float(rrow.get("wb_sales_amount", 0)), 2),
            "wb_returns_amount": round(
                float(lrow.get("wb_returns_amount", 0)) + float(rrow.get("wb_returns_amount", 0)), 2
            ),
            "fines_amount": round(float(lrow.get("fines_amount", 0)) + float(rrow.get("fines_amount", 0)), 2),
            "withholdings_amount": round(
                float(lrow.get("withholdings_amount", 0)) + float(rrow.get("withholdings_amount", 0)), 2
            ),
        }
    merged["partners"] = merged_partners

    merged_costs = {}
    for p in PARTNERS:
        lcost = (base.get("partner_costs", {}) or {}).get(p, {})
        rcost = (extra.get("partner_costs", {}) or {}).get(p, {})
        merged_costs[p] = {
            "sales": round(float(lcost.get("sales", 0)) + float(rcost.get("sales", 0)), 2),
            "returns": round(float(lcost.get("returns", 0)) + float(rcost.get("returns", 0)), 2),
        }
    merged["partner_costs"] = merged_costs
    return merged


def parse_usn_file(file_obj, read_excel_fast: Callable) -> dict[str, float]:
    """
    Файл 3 (доп. УСН к сумме из отчёта 1+2):
    - колонка «Сумма выкупа… (вкл. НДС)» (или колонка E)
    - партнёр из артикула (как в отчётах 1–2)
    - возвращает по каждому партнёру величину 7% от суммы выкупа (это добавка к УСН, не дублирует commission из файла 1)
    """
    df = read_excel_fast(file_obj)
    norm_cols = {_normalize_name(col): col for col in df.columns}

    buyout_col = None
    for norm_name, original in norm_cols.items():
        # robust name match (normalized lower-case)
        if "сумма выкупа" in norm_name and "ндс" in norm_name:
            buyout_col = original
            break
    # Fallback requested by user: in USN file this metric is always column E.
    # Works even when header is broken/shifted/noisy in Excel export.
    if buyout_col is None and len(df.columns) >= 5:
        buyout_col = df.columns[4]
    if buyout_col is None:
        raise ValueError(
            "В файле УСН не найдена колонка 'Сумма выкупа, руб. (вкл. НДС)' и отсутствует колонка E."
        )

    # Partner in USN file should be resolved from article (same idea as reports 1/2).
    supplier_col = _find_column(df, [["артикул", "поставщика"], ["артикул продавца"], ["артикул"]])
    if supplier_col is None:
        supplier_col = _find_article_like_column(df)
    partner_col = _find_column(df, [["партнер"], ["партнер контрагента"], ["партн"]])
    if supplier_col is None and partner_col is None:
        raise ValueError("В файле УСН не удалось определить колонку с артикулом для расчета партнёра.")

    totals = {p: 0.0 for p in PARTNERS}
    buyout_values = df[buyout_col].tolist()
    partner_values = df[partner_col].tolist() if partner_col else None
    supplier_values = df[supplier_col].tolist() if supplier_col else None

    for i, raw_buyout in enumerate(buyout_values):
        partner = None
        if supplier_values is not None:
            partner = _extract_partner(supplier_values[i])
        if partner is None and partner_values is not None:
            partner = _extract_partner(partner_values[i])
        if partner not in _PARTNER_SET:
            continue
        totals[partner] += _to_float(raw_buyout)

    return {p: round(totals[p] * 0.07, 2) for p in PARTNERS}


def apply_usn_adjustments(report: dict, usn_adjustments: dict[str, float]) -> None:
    """
    Добавляет УСН из файла 3 к уже посчитанному УСН в partners[*].commission.

    Во всём проекте УСН хранится в поле commission (историческое имя JSON):
    в _build_result это 7% от wb_net; здесь прибавляем 7% от суммы выкупа по файлу 3.
    total_amount уменьшаем на ту же величину, чтобы итог WB оставался согласованным.
    """
    partners = report.get("partners", {})
    for p in PARTNERS:
        row = partners.get(p)
        if not row:
            continue
        extra = float(usn_adjustments.get(p, 0.0))
        if not extra:
            continue
        row["commission"] = round(float(row.get("commission", 0.0)) + extra, 2)
        row["total_amount"] = round(float(row.get("total_amount", 0.0)) - extra, 2)


def total_partner_usn(report: dict) -> float:
    """
    Суммарный УСН по всем партнёрам = сумма partners[*].commission (commission == УСН, руб.).
    """
    return round(
        sum(float(((report.get("partners") or {}).get(p) or {}).get("commission", 0.0)) for p in PARTNERS),
        2,
    )


def total_usn(report: dict) -> float:
    """Алиас для совместимости; см. total_partner_usn."""
    return total_partner_usn(report)


def sync_partner_usn_alias(report: dict) -> None:
    """Дублирует commission в usn для явной семантики «УСН = commission»."""
    partners = report.get("partners") or {}
    for p in PARTNERS:
        row = partners.get(p)
        if not row:
            continue
        row["usn"] = round(float(row.get("commission", 0.0)), 2)


def sum_file3_usn(usn_by_partner: dict[str, float]) -> float:
    return round(sum(float(usn_by_partner.get(p, 0.0)) for p in PARTNERS), 2)
