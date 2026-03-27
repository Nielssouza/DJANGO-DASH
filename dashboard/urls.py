from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('idh-mundial/', views.idh_mundial, name='idh_mundial'),
    path('pib-mundial/', views.pib_mundial, name='pib_mundial'),
]
