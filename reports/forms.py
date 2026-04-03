from django import forms
from django.forms import inlineformset_factory
from .models import Product, ProductVariant


class ProductForm(forms.ModelForm):
    class Meta:
        model = Product
        fields = ['sku']
        widgets = {
            'sku': forms.TextInput(attrs={'class': 'rounded-2xl border border-black bg-white px-4 py-2 text-slate-900 w-full'}),
        }


class ProductVariantForm(forms.ModelForm):
    class Meta:
        model = ProductVariant
        fields = ['size', 'cost']
        widgets = {
            'size': forms.TextInput(attrs={'class': 'rounded-2xl border border-slate-300 bg-slate-100 px-4 py-2 text-slate-900 w-full'}),
            'cost': forms.NumberInput(attrs={'class': 'rounded-2xl border border-slate-300 bg-slate-100 px-4 py-2 text-slate-900 w-full', 'step': '0.01'}),
        }


ProductVariantFormSet = inlineformset_factory(
    Product,
    ProductVariant,
    form=ProductVariantForm,
    extra=1,
    can_delete=True,
)
