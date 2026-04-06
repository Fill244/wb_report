import json
import pandas as pd
import re
import traceback
import hashlib
from django.core.cache import cache
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.csrf import csrf_exempt

from .forms import ProductForm, ProductVariantFormSet
from .models import Product, ProductVariant, Report


def _normalize_name(name):
    s = str(name).strip().lower()
    s = s.replace('\u00A0', ' ')
    s = s.replace('ё', 'е')
    s = re.sub(r'[^0-9a-zа-я ]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _find_column(df, keywords):
    normalized_keywords = [_normalize_name(keyword) for keyword in keywords]
    for col in df.columns:
        name = _normalize_name(col)
        if any(keyword in name for keyword in normalized_keywords):
            return col
        for keyword in normalized_keywords:
            keyword_words = [word for word in keyword.split(' ') if word]
            if keyword_words and all(word in name for word in keyword_words):
                return col
    return None


def _extract_partner(value):
    if pd.isna(value):
        return None
    s = str(value).strip().lower()
    for ch in reversed(s):
        if ch in {'a', 'b', 'c', 'd'}:
            return ch
    return None


def _to_number(series):
    series = series.fillna(0)
    if series.dtype == object:
        series = series.astype(str)
        series = series.str.replace(r'[\s\u00A0]', '', regex=True)
        series = series.str.replace(',', '.', regex=False)
        series = series.str.replace(r'[^0-9.\-]', '', regex=True)
    return pd.to_numeric(series, errors='coerce').fillna(0)


def _normalize_column_names(df):
    cleaned = {col: _normalize_name(col) for col in df.columns}
    return df.rename(columns=cleaned)


def _prepare_df(file):
    df = pd.read_excel(file, engine='openpyxl')
    df = _normalize_column_names(df)
    return df


def _build_result(df):
    supplier_col = _find_column(df, ['артикул', 'поставщика'])
    partner_col = _find_column(df, ['партнер'])
    doc_type_col = _find_column(df, ['тип документа'])
    seller_amount_col = _find_column(df, ['к перечислению', 'перечислению продавцу'])
    delivery_col = _find_column(df, ['услуги по доставке'])
    wb_realized_col = _find_column(df, ['вайлдберриз реализовал', 'реализовал товар'])
    storage_col = _find_column(df, ['хранение', 'хран', 'хранен'])
    receiving_col = _find_column(df, ['операции на приемке', 'приемке', 'приёмке', 'прием'])
    sku_col = supplier_col
    size_col = _find_column(df, ['размер'])
    qty_col = _find_column(df, ['кол-во', 'количество'])
    fines_col = _find_column(df, ['штраф'])
    withholdings_col = _find_column(df, ['удерж'])

    missing = []
    for name, col in [
        ('Артикул поставщика', supplier_col),
        ('Тип документа', doc_type_col),
        ('К перечислению Продавцу за реализованный Товар', seller_amount_col),
    ]:
        if col is None:
            missing.append(name)
    if missing:
        raise ValueError(f'Отсутствуют обязательные колонки: {", ".join(missing)}')

    df = df.copy()
    df['partner'] = df[supplier_col].apply(_extract_partner)
    if partner_col is not None:
        partner_from_partner_col = df[partner_col].apply(_extract_partner)
        df['partner'] = df['partner'].fillna(partner_from_partner_col)

    storage_amount = _to_number(df[storage_col]) if storage_col is not None else pd.Series([0] * len(df), index=df.index)
    receiving_amount = _to_number(df[receiving_col]) if receiving_col is not None else pd.Series([0] * len(df), index=df.index)
    overall_storage_sum = float(storage_amount.sum())
    overall_receiving_sum = float(receiving_amount.sum())

    df['sku'] = df[sku_col].fillna('').astype(str).str.strip()
    df['size'] = df[size_col].fillna('').astype(str).str.strip() if size_col is not None else ''
    df['quantity'] = _to_number(df[qty_col]) if qty_col is not None else pd.Series([1] * len(df), index=df.index)

    product_map = {}
    for variant in ProductVariant.objects.select_related('product').all():
        key = (variant.product.sku.strip().lower(), variant.size.strip().lower())
        product_map[key] = variant

    doc_type = df[doc_type_col].fillna('').astype(str).str.strip().str.lower()
    sales_mask = doc_type.str.contains('продажа|^$')
    returns_mask = doc_type.str.contains('возврат')

    sales_amount = _to_number(df[seller_amount_col])
    delivery_amount = _to_number(df[delivery_col]) if delivery_col is not None else pd.Series([0] * len(df), index=df.index)
    wb_realized_amount = _to_number(df[wb_realized_col]) if wb_realized_col is not None else pd.Series([0] * len(df), index=df.index)
    fines_amount = _to_number(df[fines_col]) if fines_col is not None else pd.Series([0] * len(df), index=df.index)
    withholdings_amount = _to_number(df[withholdings_col]) if withholdings_col is not None else pd.Series([0] * len(df), index=df.index)

    df = df[df['partner'].notna()]

    product_cost_sales = 0.0
    product_cost_returns = 0.0
    partner_costs = {partner: {'sales': 0.0, 'returns': 0.0} for partner in ['a', 'b', 'c', 'd']}
    missing_products = set()
    for idx, row in df.iterrows():
        sku = str(row['sku']).strip().lower()
        size = str(row['size']).strip().lower()
        qty = float(row['quantity'])
        partner = row['partner']
        product = product_map.get((sku, size))
        if product is None and size:
            product = product_map.get((sku, ''))
        if product is None:
            sku_value = str(row[sku_col]).strip()
            size_value = ''
            if size_col is not None:
                size_value = str(row[size_col]).strip()
            missing_products.add(f'{sku_value}{" / " + size_value if size_value else ""}')
            continue
        cost_amount = float(product.cost) * qty
        if sales_mask.loc[idx]:
            product_cost_sales += cost_amount
            if partner in partner_costs:
                partner_costs[partner]['sales'] += cost_amount
        if returns_mask.loc[idx]:
            product_cost_returns += cost_amount
            if partner in partner_costs:
                partner_costs[partner]['returns'] += cost_amount

    results = {}
    for partner in ['a', 'b', 'c', 'd']:
        partner_rows = df[df['partner'] == partner]
        sales_sum = float(sales_amount[sales_mask & (df['partner'] == partner)].sum())
        returns_sum = float(sales_amount[returns_mask & (df['partner'] == partner)].sum())
        delivery_sum = float(delivery_amount[partner_rows.index].sum())
        wb_sales_sum = float(wb_realized_amount[sales_mask & (df['partner'] == partner)].sum())
        wb_returns_sum = float(wb_realized_amount[returns_mask & (df['partner'] == partner)].sum())
        wb_sum = round(wb_sales_sum - wb_returns_sum, 2)
        fines_sum = float(fines_amount[partner_rows.index].sum())
        withholdings_sum = float(withholdings_amount[partner_rows.index].sum())
        net = sales_sum - returns_sum
        commission = round(wb_sum * 0.07, 2)
        total = round(net - commission - fines_sum - withholdings_sum, 2)

        results[partner] = {
            'sales_amount': round(sales_sum, 2),
            'returns_amount': round(returns_sum, 2),
            'net_amount': round(net, 2),
            'commission': commission,
            'total_amount': total,
            'delivery_amount': round(delivery_sum, 2),
            'wb_realized_amount': wb_sum,
            'wb_sales_amount': round(wb_sales_sum, 2),
            'wb_returns_amount': round(wb_returns_sum, 2),
            'fines_amount': round(fines_sum, 2),
            'withholdings_amount': round(withholdings_sum, 2),
        }

    return {
        'partners': results,
        'overall_storage_amount': round(overall_storage_sum, 2),
        'overall_receiving_amount': round(overall_receiving_sum, 2),
        'product_cost_sales': round(product_cost_sales, 2),
        'product_cost_returns': round(product_cost_returns, 2),
        'partner_costs': {partner: {'sales': round(costs['sales'], 2), 'returns': round(costs['returns'], 2)} for partner, costs in partner_costs.items()},
        'missing_products': sorted(missing_products),
        'details': [
            {
                'article': str(row[supplier_col]).strip(),
                'partner': row['partner'],
                'type': str(row[doc_type_col]).strip(),
                'sale_amount': round(float(sales_amount.loc[idx]), 2),
                'wb_amount': round(float(wb_realized_amount.loc[idx]), 2),
                'delivery': round(float(delivery_amount.loc[idx]), 2),
                'fines': round(float(fines_amount.loc[idx]), 2),
                'deductions': round(float(withholdings_amount.loc[idx]), 2),
                'cost': round(float(product.cost if product else 0) * float(row['quantity']), 2),
                'profit': round(float(sales_amount.loc[idx]) - float(wb_realized_amount.loc[idx]) * 0.07 - float(delivery_amount.loc[idx]) - float(fines_amount.loc[idx]) - float(withholdings_amount.loc[idx]) - (float(product.cost if product else 0) * float(row['quantity'])), 2),
            }
            for idx, row in df.iterrows()
        ],
    }


def product_list(request):
    products = Product.objects.prefetch_related('variants').order_by('sku')
    return render(request, 'reports/products.html', {'products': products})


def product_add(request):
    if request.method == 'POST':
        form = ProductForm(request.POST)
        formset = ProductVariantFormSet(request.POST)
        if form.is_valid() and formset.is_valid():
            product = form.save()
            variants = formset.save(commit=False)
            for variant in variants:
                variant.product = product
                variant.save()
            return redirect('product_list')
    else:
        form = ProductForm()
        formset = ProductVariantFormSet()
    return render(request, 'reports/product_form.html', {'form': form, 'formset': formset, 'title': 'Добавить товар'})


def product_edit(request, pk):
    product = get_object_or_404(Product, pk=pk)
    if request.method == 'POST':
        form = ProductForm(request.POST, instance=product)
        formset = ProductVariantFormSet(request.POST, instance=product)
        if form.is_valid() and formset.is_valid():
            form.save()
            formset.save()
            return redirect('product_list')
    else:
        form = ProductForm(instance=product)
        formset = ProductVariantFormSet(instance=product)
    return render(request, 'reports/product_form.html', {'form': form, 'formset': formset, 'title': 'Редактировать товар'})


def product_view(request, pk):
    product = get_object_or_404(Product.objects.prefetch_related('variants'), pk=pk)
    variants = product.variants.all().order_by('size')
    return render(request, 'reports/product_view.html', {'product': product, 'variants': variants})


def delete_product(request, pk):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')
    product = get_object_or_404(Product, pk=pk)
    product.delete()
    return redirect('product_list')


@csrf_exempt
def save_report(request):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')

    try:
        payload = json.loads(request.body.decode('utf-8'))
        report_data = payload.get('data')
        report_name = payload.get('report_name', '')
        file_name = payload.get('file_name', '')
        if not report_data:
            return JsonResponse({'error': 'Нет данных для сохранения отчёта.'}, status=400)

        report = Report.objects.create(
            title=report_name[:255] if report_name else '',
            file_name=file_name[:255] if file_name else '',
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
    report_json = json.dumps(report.data, ensure_ascii=False)
    return render(request, 'reports/report_view.html', {
        'report': report,
        'report_json': report_json,
    })


def delete_report(request, pk):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этом URL.')
    report = get_object_or_404(Report, pk=pk)
    report.delete()
    return redirect('report_history')


def upload_page(request):
    return render(request, 'reports/upload.html')


@csrf_exempt
def upload_file(request):
    if request.method != 'POST':
        return HttpResponseBadRequest('Только POST-запрос принимается на этот URL.')

    excel_file = request.FILES.get('file')
    if not excel_file:
        return JsonResponse({'error': 'Файл не найден в запросе.'}, status=400)

    # Calculate file hash for caching
    file_hash = hashlib.sha256(excel_file.read()).hexdigest()
    excel_file.seek(0)  # Reset file pointer

    # Check cache first
    cache_key = f'wb_report_{file_hash}'
    cached_result = cache.get(cache_key)
    if cached_result:
        return JsonResponse(cached_result)

    try:
        df = _prepare_df(excel_file)
        results = _build_result(df)
        # Cache the result for 1 hour
        cache.set(cache_key, results, 3600)
    except ValueError as exc:
        return JsonResponse({'error': str(exc)}, status=400)
    except Exception as exc:
        return JsonResponse({
            'error': f'Ошибка при обработке файла: {exc}',
            'trace': traceback.format_exc(),
        }, status=500)

    return JsonResponse(results)
