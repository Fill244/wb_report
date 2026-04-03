from django.db import models


class Product(models.Model):
    sku = models.CharField('Артикул', max_length=100, unique=True)

    class Meta:
        verbose_name = 'Товар'
        verbose_name_plural = 'Товары'
        ordering = ['sku']

    def __str__(self):
        return self.sku


class ProductVariant(models.Model):
    product = models.ForeignKey(Product, related_name='variants', on_delete=models.CASCADE)
    size = models.CharField('Размер', max_length=100, blank=True)
    cost = models.DecimalField('Себестоимость', max_digits=12, decimal_places=2, default=0)

    class Meta:
        verbose_name = 'Вариант товара'
        verbose_name_plural = 'Варианты товара'
        unique_together = ('product', 'size')
        ordering = ['product__sku', 'size']

    def __str__(self):
        if self.size:
            return f'{self.product.sku} / {self.size}'
        return f'{self.product.sku} / Без размера'


class Report(models.Model):
    title = models.CharField('Название отчёта', max_length=255, blank=True)
    file_name = models.CharField('Имя файла', max_length=255, blank=True)
    data = models.JSONField('Данные отчёта')
    created_at = models.DateTimeField('Дата сохранения', auto_now_add=True)

    class Meta:
        verbose_name = 'Отчёт'
        verbose_name_plural = 'Отчёты'
        ordering = ['-created_at']

    def __str__(self):
        return self.title or self.file_name or f'Отчёт от {self.created_at:%Y-%m-%d %H:%M}'
