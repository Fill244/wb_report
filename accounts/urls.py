from django.urls import path

from .views import RegisterView, UserLoginView, UserLogoutView


urlpatterns = [
    path('login/', UserLoginView.as_view(), name='login'),
    path('register/', RegisterView.as_view(), name='register'),
    path('logout/', UserLogoutView.as_view(), name='logout'),
]

