from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('pib-mundial/', views.pib_mundial, name='pib_mundial'),
]
