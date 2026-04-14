from django.urls import path
from . import views

urlpatterns = [
    path('', views.upload_page, name='upload_page'),
    path('upload/', views.upload_file, name='upload_file'),
    path('report/save/', views.save_report, name='save_report'),
    path('report/load/<int:pk>/', views.load_report, name='load_report'),
    path('report/delete/<int:pk>/', views.delete_report, name='delete_report'),
    path('report/history/', views.report_history, name='report_history'),
    path('products/', views.product_list, name='product_list'),
    path('products/add/', views.product_add, name='product_add'),
    path('products/import/', views.import_products_excel, name='import_products_excel'),
    path('products/<int:pk>/edit/', views.product_edit, name='product_edit'),
    path('products/<int:pk>/view/', views.product_view, name='product_view'),
    path('products/<int:pk>/delete/', views.delete_product, name='delete_product'),
]
