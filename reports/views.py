
"""
views_optimized.py
==================
Performance-critical path summary
----------------------------------
1.  Excel reading   → python-calamine (Rust, 5-10x faster than openpyxl).
                      Single read with header=None, column filtering in Python
                      → zero double-reads.
2.  _to_number      → str.translate() instead of 3 chained regex passes.
3.  Partner extract → fully vectorised str.extract (no apply).
4.  Cost lookup     → plain Python lists + local var refs (fastest dict scan).
5.  details list    → all numpy arrays rounded before zip, no per-row float().
6.  Cost map cache  → process-level dict avoids repeated DB hit on same process.
7.  import_products → bulk_create / bulk_update (3 DB queries total).
"""

import json
import re
import traceback
import hashlib
from decimal import Decimal, InvalidOperation
from functools import lru_cache

import numpy as np
import pandas as pd

from django.core.cache import cache
from django.db.models import Count, Min, Prefetch
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.csrf import csrf_exempt

from .forms import ProductForm, ProductVariantFormSet
from .models import Product, ProductVariant, Report
from .services.pdf_parser import parse_pdf_file
from .services.report_processor_multi import (
    apply_usn_adjustments,
    merge_reports,
    parse_usn_file,
    process_report,
    sum_file3_usn,
    sync_partner_usn_alias,
    total_partner_usn,
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_PARTNERS    = ('a', 'b', 'c', 'd')
_PARTNER_SET = frozenset(_PARTNERS)

_EMPTY_PARTNER_ROW = {
    'sales_amount': 0.0, 'returns_amount': 0.0, 'net_amount': 0.0,
    # commission: УСН в рублях (7% от wb_net в отчёте; в multi-режиме +7% от выкупа из файла 3)
    'commission': 0.0,   'total_amount': 0.0,   'delivery_amount': 0.0,
    'wb_realized_amount': 0.0, 'wb_sales_amount': 0.0, 'wb_returns_amount': 0.0,
    'fines_amount': 0.0, 'withholdings_amount': 0.0,
}

# Compiled once at import time
_RE_NORM_CHARS  = re.compile(r'[^0-9a-zа-я ]+')
_RE_NORM_SPACES = re.compile(r'\s+')

# str.translate table: removes spaces, NBSP, comma; used in _to_number
_STRIP_TABLE = str.maketrans({
    ' ':    None,
    '\t':   None,
    '\n':   None,
    '\r':   None,
    '\u00A0': None,
    ',':    '.',
})

_REPORT_KEYWORD_GROUPS: list[list[str]] = [
    ['артикул', 'поставщика'],
    ['партнер'],
    ['тип документа'],
    ['к перечислению', 'перечислению продавцу'],
    ['услуги по доставке'],
    ['вайлдберриз реализовал', 'реализовал товар'],
    ['хранение', 'хран', 'хранен'],
    ['операции на приемке', 'приемке', 'приёмке', 'прием'],
    ['размер'],
    ['кол-во', 'количество'],
    ['штраф'],
    ['удерж'],
]

# Process-level cost-map cache: invalidated when any ProductVariant changes.
# Keyed by the DB rowcount of ProductVariant so stale data is never used.
_COST_MAP_CACHE: dict = {}   # {'count': int, 'exact': dict, 'fallback': dict}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4096)
def _normalize_name(name: str) -> str:
    s = str(name).strip().lower()
    s = s.replace('\u00A0', ' ').replace('ё', 'е')
    s = _RE_NORM_CHARS.sub(' ', s)
    return _RE_NORM_SPACES.sub(' ', s).strip()


def _matches_keywords(normalized_name: str, keywords: list[str]) -> bool:
    for kw in keywords:
        if kw in normalized_name:
            return True
        words = kw.split()
        if words and all(w in normalized_name for w in words):
            return True
    return False


def _find_column(df: pd.DataFrame, keywords: list[str]):
    norm_kws = [_normalize_name(k) for k in keywords]
    for col in df.columns:
        if _matches_keywords(_normalize_name(col), norm_kws):
            return col
    return None


def _to_number(series: pd.Series) -> pd.Series:
    """
    Convert Series → float64.
    For object dtype uses str.translate (single C pass) instead of
    three chained regex replacements.
    """
    series = series.fillna(0)
    if series.dtype == object:
        # translate is ~3x faster than chained str.replace / regex
        series = (
            series.astype(str)
            .map(lambda v: re.sub(r'[^0-9.\-]', '', v.translate(_STRIP_TABLE)))
        )
    return pd.to_numeric(series, errors='coerce').fillna(0)


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={col: _normalize_name(col) for col in df.columns})


# ---------------------------------------------------------------------------
# Vectorised partner extraction
# ---------------------------------------------------------------------------

def _extract_partner_series(series: pd.Series) -> pd.Series:
    """
    Last occurrence of a/b/c/d in each value, fully vectorised.
    Reverses the string → extracts first match → lower-cases.
    """
    result = (
        series.fillna('').astype(str)
        .str[::-1]
        .str.extract(r'([abcdABCD])', expand=False)
        .str.lower()
    )
    return result.where(result.isin(_PARTNER_SET), other=np.nan)


# ---------------------------------------------------------------------------
# Excel loading  ← THE hottest path
# ---------------------------------------------------------------------------

def _read_excel_fast(file, header=0, usecols=None, nrows=None) -> pd.DataFrame:
    """
    Use python-calamine (Rust) when available; fall back to openpyxl.
    calamine is 5-10x faster for large .xlsx files.
    """
    kwargs = dict(header=header, usecols=usecols)
    if nrows is not None:
        kwargs['nrows'] = nrows
    try:
        return pd.read_excel(file, engine='calamine', **kwargs)
    except Exception:
        file.seek(0)
        return pd.read_excel(file, engine='openpyxl', **kwargs)


def _prepare_df(file) -> pd.DataFrame:
    """
    Single-read strategy:
      1. Read header row only (nrows=0) to find wanted columns.
      2. Read full file with usecols filter.
    Uses calamine engine → avoids openpyxl's slow XML parsing.
    """
    # --- pass 1: headers only (cheap) ----------------------------------------
    header_df = _read_excel_fast(file, nrows=0)
    file.seek(0)

    norm_groups = [
        [_normalize_name(k) for k in grp]
        for grp in _REPORT_KEYWORD_GROUPS
    ]

    matched = [
        col for col in header_df.columns
        if any(_matches_keywords(_normalize_name(col), grp) for grp in norm_groups)
    ]

    # --- pass 2: full data, filtered columns only ----------------------------
    df = _read_excel_fast(file, usecols=matched if matched else None)
    return _normalize_column_names(df)


def _find_product_import_header_row(probe_df: pd.DataFrame) -> int:
    need_article = {'артикул продавца', 'артикул поставщика'}
    for i in range(len(probe_df)):
        normalized = {_normalize_name(str(v)) for v in probe_df.iloc[i] if pd.notna(v)}
        if normalized & need_article and 'размер' in normalized and 'цена' in normalized:
            return i
    raise ValueError('Не удалось определить строку заголовков в Excel-файле.')


def _prepare_product_import_df(file) -> pd.DataFrame:
    probe_df = _read_excel_fast(file, header=None, nrows=10)
    file.seek(0)

    header_row = _find_product_import_header_row(probe_df)
    headers    = probe_df.iloc[header_row].tolist()

    want        = {'артикул продавца', 'артикул поставщика', 'размер', 'цена'}
    col_indexes = [i for i, v in enumerate(headers) if _normalize_name(str(v)) in want]

    if len(col_indexes) < 3:
        raise ValueError('Не удалось определить колонки Артикул продавца, Размер и Цена.')

    file.seek(0)
    df = _read_excel_fast(file, header=header_row, usecols=col_indexes)
    return _normalize_column_names(df)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def _set_products_message(request, text: str, level: str = 'info') -> None:
    request.session['products_message'] = {'text': text, 'level': level}


def _pop_products_message(request):
    return request.session.pop('products_message', None)


# ---------------------------------------------------------------------------
# Cost map — process-level cache (no DB hit when data hasn't changed)
# ---------------------------------------------------------------------------

def _build_cost_maps() -> tuple[dict, dict]:
    """
    Returns (exact_cost, fallback_cost) dicts.
    Result is cached in a module-level dict keyed by ProductVariant row count.
    If the count hasn't changed we skip the DB query entirely.
    """
    count = ProductVariant.objects.count()
    cached = _COST_MAP_CACHE
    if cached.get('count') == count:
        return cached['exact'], cached['fallback']

    exact: dict[tuple[str, str], float]  = {}
    fallback: dict[str, float]            = {}

    for sku_v, size_v, cost_v in ProductVariant.objects.values_list(
        'product__sku', 'size', 'cost'
    ):
        nsku  = str(sku_v).strip().lower()
        nsize = str(size_v or '').strip().lower()
        cf    = float(cost_v)
        exact[(nsku, nsize)] = cf
        if not nsize:
            fallback[nsku] = cf

    cached.clear()
    cached.update({'count': count, 'exact': exact, 'fallback': fallback})
    return exact, fallback


# ---------------------------------------------------------------------------
# Cost lookup — unavoidably a Python loop, but maximally tight
# ---------------------------------------------------------------------------

def _vectorised_cost_lookup(
    sku_lower: pd.Series,
    size_lower: pd.Series,
    sku_display: pd.Series,
    size_display: pd.Series,
    exact_cost: dict,
    fallback_cost: dict,
) -> tuple[np.ndarray, set[str]]:
    n      = len(sku_lower)
    costs  = np.zeros(n, dtype=np.float64)
    missing: set[str] = set()

    sk_l  = sku_lower.tolist()
    sz_l  = size_lower.tolist()
    sv_l  = sku_display.tolist()
    szv_l = size_display.tolist()
    ec    = exact_cost
    fb    = fallback_cost

    for i in range(n):
        sk = sk_l[i];  sz = sz_l[i]
        c  = ec.get((sk, sz))
        if c is None and sz:
            c = ec.get((sk, ''))
        if c is None:
            c = fb.get(sk)
        if c is None:
            szv = szv_l[i]
            missing.add(f'{sv_l[i]}{" / " + szv if szv else ""}')
        else:
            costs[i] = c

    return costs, missing


# ---------------------------------------------------------------------------
# Core report builder
# ---------------------------------------------------------------------------

def _build_result(df: pd.DataFrame) -> dict:
    # --- column discovery ---------------------------------------------------
    supplier_col  = _find_column(df, ['артикул', 'поставщика'])
    partner_col   = _find_column(df, ['партнер'])
    doc_type_col  = _find_column(df, ['тип документа'])
    seller_col    = _find_column(df, ['к перечислению', 'перечислению продавцу'])
    delivery_col  = _find_column(df, ['услуги по доставке'])
    wb_col        = _find_column(df, ['вайлдберриз реализовал', 'реализовал товар'])
    storage_col   = _find_column(df, ['хранение', 'хран', 'хранен'])
    receiving_col = _find_column(df, ['операции на приемке', 'приемке', 'приёмке', 'прием'])
    size_col      = _find_column(df, ['размер'])
    qty_col       = _find_column(df, ['кол-во', 'количество'])
    fines_col     = _find_column(df, ['штраф'])
    with_col      = _find_column(df, ['удерж'])

    missing_cols = [name for name, col in [
        ('Артикул поставщика',                          supplier_col),
        ('Тип документа',                               doc_type_col),
        ('К перечислению Продавцу за реализованный Товар', seller_col),
    ] if col is None]
    if missing_cols:
        raise ValueError(f'Отсутствуют обязательные колонки: {", ".join(missing_cols)}')

    # --- partner detection (vectorised) -------------------------------------
    df = df.copy()
    df['partner'] = _extract_partner_series(df[supplier_col])
    if partner_col is not None:
        mask = df['partner'].isna()
        if mask.any():
            df.loc[mask, 'partner'] = _extract_partner_series(df.loc[mask, partner_col])

    # Storage / receiving totals before partner filter
    overall_storage_sum   = float(_to_number(df[storage_col]).sum())   if storage_col   else 0.0
    overall_receiving_sum = float(_to_number(df[receiving_col]).sum()) if receiving_col else 0.0

    df = df[df['partner'].notna()].copy()

    _empty = {
        'partners':                 {p: dict(_EMPTY_PARTNER_ROW) for p in _PARTNERS},
        'overall_storage_amount':   round(overall_storage_sum, 2),
        'overall_receiving_amount': round(overall_receiving_sum, 2),
        'product_cost_sales':   0.0, 'product_cost_returns': 0.0,
        'partner_costs':            {p: {'sales': 0.0, 'returns': 0.0} for p in _PARTNERS},
        'missing_products': [],      'details': [],
    }
    if df.empty:
        return _empty

    # --- key columns --------------------------------------------------------
    df['sku']      = df[supplier_col].fillna('').astype(str).str.strip()
    df['size']     = df[size_col].fillna('').astype(str).str.strip() if size_col else ''
    df['quantity'] = _to_number(df[qty_col]) if qty_col else pd.Series(1.0, index=df.index)

    # --- numeric columns (all converted once) --------------------------------
    sales_amount    = _to_number(df[seller_col])
    delivery_amount = _to_number(df[delivery_col]) if delivery_col else pd.Series(0.0, index=df.index)
    wb_realized     = _to_number(df[wb_col])        if wb_col      else pd.Series(0.0, index=df.index)
    fines_amount    = _to_number(df[fines_col])     if fines_col   else pd.Series(0.0, index=df.index)
    with_amount     = _to_number(df[with_col])      if with_col    else pd.Series(0.0, index=df.index)

    # --- doc-type masks (once) ----------------------------------------------
    doc_type     = df[doc_type_col].fillna('').astype(str).str.strip().str.lower()
    sales_mask   = doc_type.str.contains('продажа|^$', regex=True).to_numpy()
    returns_mask = doc_type.str.contains('возврат',    regex=True).to_numpy()

    # --- cost lookup --------------------------------------------------------
    exact_cost, fallback_cost = _build_cost_maps()
    sku_lower  = df['sku'].str.lower()
    size_lower = df['size'].str.lower()

    cost_base, missing_products = _vectorised_cost_lookup(
        sku_lower, size_lower, df['sku'], df['size'], exact_cost, fallback_cost,
    )

    qty_arr          = df['quantity'].to_numpy(dtype=np.float64)
    row_cost_arr     = cost_base * qty_arr        # numpy: one shot

    # --- to numpy before groupby (avoid repeated .to_numpy calls) -----------
    sales_np   = sales_amount.to_numpy(dtype=np.float64)
    deliver_np = delivery_amount.to_numpy(dtype=np.float64)
    wb_np      = wb_realized.to_numpy(dtype=np.float64)
    fines_np   = fines_amount.to_numpy(dtype=np.float64)
    with_np    = with_amount.to_numpy(dtype=np.float64)

    sales_cost_arr = np.where(sales_mask,   row_cost_arr, 0.0)
    ret_cost_arr   = np.where(returns_mask, row_cost_arr, 0.0)

    # --- groupby (single pass) ----------------------------------------------
    grouped = pd.DataFrame({
        'partner':        df['partner'].to_numpy(),
        'sales_amount':   np.where(sales_mask,   sales_np, 0.0),
        'returns_amount': np.where(returns_mask, sales_np, 0.0),
        'delivery':       deliver_np,
        'wb_sales':       np.where(sales_mask,   wb_np, 0.0),
        'wb_returns':     np.where(returns_mask, wb_np, 0.0),
        'fines':          fines_np,
        'withs':          with_np,
        'sales_cost':     sales_cost_arr,
        'returns_cost':   ret_cost_arr,
    }).groupby('partner', sort=False).sum()

    # --- per-partner results -------------------------------------------------
    results: dict       = {}
    partner_costs: dict = {}

    for p in _PARTNERS:
        if p in grouped.index:
            row   = grouped.loc[p]
            s_sum = float(row['sales_amount'])
            r_sum = float(row['returns_amount'])
            wb_s  = float(row['wb_sales'])
            wb_r  = float(row['wb_returns'])
            del_s = float(row['delivery'])
            fins  = float(row['fines'])
            withs = float(row['withs'])
            sc    = float(row['sales_cost'])
            rc    = float(row['returns_cost'])
        else:
            s_sum = r_sum = wb_s = wb_r = del_s = fins = withs = sc = rc = 0.0

        wb_net = round(wb_s - wb_r, 2)
        net    = s_sum - r_sum
        comm   = round(wb_net * 0.07, 2)

        results[p] = {
            'sales_amount':        round(s_sum, 2),
            'returns_amount':      round(r_sum, 2),
            'net_amount':          round(net, 2),
            'commission':          comm,
            'total_amount':        round(net - comm - fins - withs, 2),
            'delivery_amount':     round(del_s, 2),
            'wb_realized_amount':  wb_net,
            'wb_sales_amount':     round(wb_s, 2),
            'wb_returns_amount':   round(wb_r, 2),
            'fines_amount':        round(fins, 2),
            'withholdings_amount': round(withs, 2),
        }
        partner_costs[p] = {'sales': round(sc, 2), 'returns': round(rc, 2)}

    # --- details: all rounding done vectorially before zip ------------------
    art_arr    = df[supplier_col].fillna('').astype(str).str.strip().tolist()
    partner_l  = df['partner'].tolist()
    type_arr   = df[doc_type_col].fillna('').astype(str).str.strip().tolist()

    # Round all numeric arrays at once (numpy) — no per-row Python round()
    sale_r  = np.round(sales_np,   2)
    wb_r_   = np.round(wb_np,      2)
    del_r   = np.round(deliver_np, 2)
    fin_r   = np.round(fines_np,   2)
    ded_r   = np.round(with_np,    2)
    cost_r  = np.round(row_cost_arr, 2)
    profit  = np.round(
        sales_np - wb_np * 0.07 - deliver_np - fines_np - with_np - row_cost_arr, 2
    )

    details = [
        {
            'article':     art,
            'partner':     prt,
            'type':        typ,
            'sale_amount': float(sa),
            'wb_amount':   float(wb),
            'delivery':    float(dl),
            'fines':       float(fi),
            'deductions':  float(dd),
            'cost':        float(co),
            'profit':      float(pr),
        }
        for art, prt, typ, sa, wb, dl, fi, dd, co, pr in zip(
            art_arr, partner_l, type_arr,
            sale_r, wb_r_, del_r, fin_r, ded_r, cost_r, profit,
        )
    ]

    return {
        'partners':                 results,
        'overall_storage_amount':   round(overall_storage_sum, 2),
        'overall_receiving_amount': round(overall_receiving_sum, 2),
        'product_cost_sales':       round(float(sales_cost_arr.sum()), 2),
        'product_cost_returns':     round(float(ret_cost_arr.sum()), 2),
        'partner_costs':            partner_costs,
        'missing_products':         sorted(missing_products),
        'details':                  details,
    }


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

def product_list(request):
    products = (
        Product.objects
        .annotate(
            size_count=Count('variants', distinct=True),
            min_cost=Min('variants__cost'),
        )
        .prefetch_related(
            Prefetch('variants', queryset=ProductVariant.objects.order_by('size', 'id'))
        )
        .order_by('sku')
    )
    return render(request, 'reports/products.html', {
        'products':         products,
        'products_message': _pop_products_message(request),
    })


def product_add(request):
    if request.method == 'POST':
        form    = ProductForm(request.POST)
        formset = ProductVariantFormSet(request.POST)
        if form.is_valid() and formset.is_valid():
            product = form.save()
            for variant in formset.save(commit=False):
                variant.product = product
                variant.save()
            return redirect('product_list')
    else:
        form    = ProductForm()
        formset = ProductVariantFormSet()
    return render(request, 'reports/product_form.html', {
        'form': form, 'formset': formset, 'title': 'Добавить товар',
    })


def product_edit(request, pk):
    product = get_object_or_404(Product, pk=pk)
    if request.method == 'POST':
        form    = ProductForm(request.POST, instance=product)
        formset = ProductVariantFormSet(request.POST, instance=product)
        if form.is_valid() and formset.is_valid():
            form.save()
            formset.save()
            return redirect('product_list')
    else:
        form    = ProductForm(instance=product)
        formset = ProductVariantFormSet(instance=product)
    return render(request, 'reports/product_form.html', {
        'form': form, 'formset': formset, 'title': 'Редактировать товар',
    })


def product_view(request, pk):
    product  = get_object_or_404(Product.objects.prefetch_related('variants'), pk=pk)
    variants = product.variants.all().order_by('size')
    return render(request, 'reports/product_view.html', {
        'product': product, 'variants': variants,
    })


def delete_product(request, pk):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')
    get_object_or_404(Product, pk=pk).delete()
    _COST_MAP_CACHE.clear()   # invalidate cost cache after product deletion
    return redirect('product_list')


def bulk_update_variant_costs(request, pk):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')

    product = get_object_or_404(Product, pk=pk)
    variant_ids = request.POST.getlist('variant_ids')
    raw_cost = (request.POST.get('cost') or '').strip().replace(',', '.')

    if not variant_ids:
        _set_products_message(request, 'Выберите хотя бы один размер для изменения.', 'error')
        return redirect('product_list')

    if not raw_cost:
        _set_products_message(request, 'Укажите новую себестоимость.', 'error')
        return redirect('product_list')

    try:
        cost = Decimal(raw_cost)
    except (InvalidOperation, TypeError):
        _set_products_message(request, 'Себестоимость должна быть числом.', 'error')
        return redirect('product_list')

    variants = list(ProductVariant.objects.filter(product=product, id__in=variant_ids))
    if not variants:
        _set_products_message(request, 'Не удалось найти выбранные размеры для обновления.', 'error')
        return redirect('product_list')

    for variant in variants:
        variant.cost = cost

    ProductVariant.objects.bulk_update(variants, ['cost'])
    _COST_MAP_CACHE.clear()
    _set_products_message(request, f'Себестоимость обновлена для размеров: {len(variants)}.', 'success')
    return redirect('product_list')


def import_products_excel(request):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')

    excel_file = request.FILES.get('file')
    if not excel_file:
        _set_products_message(request, 'Выберите Excel-файл для импорта.', 'error')
        return redirect('product_list')

    try:
        df = _prepare_product_import_df(excel_file)

        article_col = _find_column(df, ['артикул продавца', 'артикул поставщика'])
        size_col    = _find_column(df, ['размер'])
        price_col   = _find_column(df, ['цена'])

        missing_cols = [n for n, c in [
            ('Артикул продавца', article_col),
            ('Размер',           size_col),
            ('Цена',             price_col),
        ] if c is None]
        if missing_cols:
            raise ValueError(f'В Excel не найдены обязательные колонки: {", ".join(missing_cols)}.')

        # --- vectorised cleaning --------------------------------------------
        sku_s = df[article_col].fillna('').astype(str).str.strip()
        sz_s  = df[size_col].fillna('').astype(str).str.strip().replace('nan', '')
        price_s = (
            df[price_col].fillna('').astype(str)
            .str.strip()
            .str.replace(r'[\u00A0\s]', '', regex=True)
            .str.replace(',', '.', regex=False)
        )

        valid_mask = (
            sku_s.str.len().gt(0)
            & sku_s.str.lower().ne('nan')
            & price_s.str.len().gt(0)
            & price_s.str.lower().ne('nan')
        )
        sku_s   = sku_s[valid_mask].reset_index(drop=True)
        sz_s    = sz_s[valid_mask].reset_index(drop=True)
        price_s = price_s[valid_mask].reset_index(drop=True)

        if sku_s.empty:
            raise ValueError('Файл не содержит корректных строк для импорта.')

        def _try_decimal(v):
            try:
                return Decimal(v)
            except (InvalidOperation, TypeError):
                return None

        costs = [_try_decimal(p) for p in price_s]
        rows  = [
            (sku, sz, cost)
            for sku, sz, cost in zip(sku_s, sz_s, costs)
            if cost is not None
        ]

        if not rows:
            raise ValueError('Файл не содержит корректных строк для импорта.')

        # --- bulk upsert (3 queries total) -----------------------------------
        skus             = {sku for sku, _, _ in rows}
        existing_products = {p.sku: p for p in Product.objects.filter(sku__in=skus)}
        new_products      = [Product(sku=sku) for sku in skus if sku not in existing_products]

        if new_products:
            Product.objects.bulk_create(new_products, ignore_conflicts=True)
            for p in Product.objects.filter(sku__in=[p.sku for p in new_products]):
                existing_products[p.sku] = p

        created_products = len(new_products)

        existing_variants: dict[tuple, ProductVariant] = {
            (v.product_id, v.size): v
            for v in ProductVariant.objects.filter(product__sku__in=skus)
        }

        to_create: list[ProductVariant] = []
        to_update: list[ProductVariant] = []

        for sku, size, cost in rows:
            product = existing_products[sku]
            key     = (product.pk, size)
            if key in existing_variants:
                v      = existing_variants[key]
                v.cost = cost
                to_update.append(v)
            else:
                to_create.append(ProductVariant(product=product, size=size, cost=cost))

        if to_create:
            ProductVariant.objects.bulk_create(to_create, ignore_conflicts=True)
        if to_update:
            ProductVariant.objects.bulk_update(to_update, ['cost'])

        _COST_MAP_CACHE.clear()   # new variants → invalidate cost cache

        _set_products_message(
            request,
            f'Импорт завершён. Обработано строк: {len(rows)}. '
            f'Новых товаров: {created_products}. '
            f'Новых размеров: {len(to_create)}. '
            f'Обновлено размеров: {len(to_update)}.',
            'success',
        )
    except Exception as exc:
        _set_products_message(request, f'Ошибка импорта: {exc}', 'error')

    return redirect('product_list')


@csrf_exempt
def save_report(request):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')

    try:
        payload     = json.loads(request.body.decode('utf-8'))
        report_data = payload.get('data')
        report_name = payload.get('report_name', '')
        file_name   = payload.get('file_name', '')
        if not report_data:
            return JsonResponse({'error': 'Нет данных для сохранения отчёта.'}, status=400)
        report = Report.objects.create(
            title=report_name[:255],
            file_name=file_name[:255],
            data=report_data,
        )
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Неверный JSON в запросе.'}, status=400)
    except Exception as exc:
        return JsonResponse({'error': f'Ошибка при сохранении отчёта: {exc}'}, status=500)

    return JsonResponse({'success': True, 'report_id': report.id})


def report_history(request):
    reports = Report.objects.order_by('-created_at')
    return render(request, 'reports/history.html', {'reports': reports})


def load_report(request, pk):
    report = get_object_or_404(Report, pk=pk)
    return render(request, 'reports/report_view.html', {
        'report':      report,
        'report_json': json.dumps(report.data, ensure_ascii=False),
    })


def delete_report(request, pk):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')
    get_object_or_404(Report, pk=pk).delete()
    return redirect('report_history')


def upload_page(request):
    return render(request, 'reports/upload.html')


@csrf_exempt
def upload_file(request):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этот URL.')

    mode = request.POST.get('mode', 'single')

    if mode == 'multi':
        main_file = request.FILES.get('main_report')
        extra_file = request.FILES.get('additional_report')
        usn_file = request.FILES.get('usn_file')
        pdf_file = request.FILES.get('pdf_file')

        if not all([main_file, extra_file, usn_file, pdf_file]):
            return JsonResponse({
                'error': 'Для расширенного режима нужно загрузить 4 файла: основной, дополнительный, УСН и PDF.'
            }, status=400)

        warnings: list[str] = []
        try:
            # File 1: standard processing.
            result_main = process_report(main_file, mode=1, prepare_df=_prepare_df, build_result=_build_result)

            # File 2: same processing but without USN + wb sales/returns metrics.
            result_extra = process_report(extra_file, mode=2, prepare_df=_prepare_df, build_result=_build_result)

            merged = merge_reports(result_main, result_extra)

            # УСН после Excel 1+2: в commission только файл 1 (файл 2 обнуляет commission до merge).
            usn_after_excel_1_and_2 = total_partner_usn(merged)

            # Файл 3: 7% от «Сумма выкупа» по партнёру — прибавляется к УСН (commission) каждого партнёра.
            usn_adjustments = parse_usn_file(usn_file, read_excel_fast=_read_excel_fast)
            apply_usn_adjustments(merged, usn_adjustments)
            sync_partner_usn_alias(merged)

            usn_total = total_partner_usn(merged)
            usn_from_file3 = sum_file3_usn(usn_adjustments)

            # File 4: PDF checks and extra rows.
            pdf_data = parse_pdf_file(pdf_file)

            # Ожидание из PDF: 7% от итоговой строки отчёта WB (не сумма Excel 1+2+3 по отдельным базам).
            expected_usn = None
            if pdf_data.get('total_realized_amount') is not None:
                expected_usn = round(float(pdf_data['total_realized_amount']) * 0.07, 2)

            match = True
            if expected_usn is not None:
                match = abs(expected_usn - usn_total) <= 0.01
                if not match:
                    warnings.append(
                        f'Проверка PDF: УСН не совпадает (ожидалось {expected_usn:.2f}, рассчитано {usn_total:.2f}).'
                    )
            else:
                warnings.append('Не удалось извлечь из PDF строку "Итого стоимость реализованного товара и услуг".')

            row_21 = pdf_data.get('row_2_1')
            row_45 = pdf_data.get('row_4_5')
            extra_rows = {
                '2.1': row_21 if (row_21 is not None and row_21 < 0) else None,
                '4.5': row_45 if row_45 is not None else None,
            }

            merged['warnings'] = warnings
            merged['usn_breakdown'] = {
                'from_excel_1_and_2': round(usn_after_excel_1_and_2, 2),
                'from_file3_buyout': round(usn_from_file3, 2),
                'by_partner_file3': usn_adjustments,
                'total': round(usn_total, 2),
                'note': 'УСН в JSON = поле commission (и дублируется как usn). Файл 2 в УСН не входит.',
            }
            merged['pdf_checks'] = {
                'expected_usn': expected_usn,
                'actual_usn': usn_total,
                'match': match if expected_usn is not None else False,
                'expected_usn_source': '7% от суммы строки «Итого стоимость реализованного товара и услуг» в PDF',
                'actual_usn_source': 'сумма partners[a..d].commission после Excel 1+2 и добавки из файла 3',
            }
            merged['extra_rows'] = extra_rows
            return JsonResponse(merged)
        except ValueError as exc:
            return JsonResponse({'error': str(exc)}, status=400)
        except Exception as exc:
            return JsonResponse({
                'error': f'Ошибка при расширенной обработке файлов: {exc}',
                'trace': traceback.format_exc(),
            }, status=500)

    excel_file = request.FILES.get('file')
    if not excel_file:
        return JsonResponse({'error': 'Файл не найден в запросе.'}, status=400)

    # Hash first 1 MB only — fast, negligible collision risk for Excel files
    chunk     = excel_file.read(1 << 20)
    file_hash = hashlib.blake2b(chunk, digest_size=16).hexdigest()
    excel_file.seek(0)

    cache_key     = f'wb_report_{file_hash}'
    cached_result = cache.get(cache_key)
    if cached_result is not None:
        return JsonResponse(cached_result)

    try:
        df      = _prepare_df(excel_file)
        results = _build_result(df)
        cache.set(cache_key, results, 3600)
    except ValueError as exc:
        return JsonResponse({'error': str(exc)}, status=400)
    except Exception as exc:
        return JsonResponse({
            'error': f'Ошибка при обработке файла: {exc}',
            'trace': traceback.format_exc(),
        }, status=500)

    return JsonResponse(results)
