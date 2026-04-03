from django.contrib import admin

from .models import Product, ProductVariant, Report


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ('sku',)
    search_fields = ('sku',)


@admin.register(ProductVariant)
class ProductVariantAdmin(admin.ModelAdmin):
    list_display = ('product', 'size', 'cost')
    list_filter = ('product',)
    search_fields = ('product__sku', 'size')


@admin.register(Report)
class ReportAdmin(admin.ModelAdmin):
    list_display = ('title', 'file_name', 'created_at')
    search_fields = ('title', 'file_name')
    readonly_fields = ('created_at',)
