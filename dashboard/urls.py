from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('acoes-mercado-mundial/', views.acoes_mercado_mundial, name='acoes_mercado_mundial'),
    path('cotacao-moedas/', views.cotacao_moedas, name='cotacao_moedas'),
    path('empresarial/', views.empresarial, name='empresarial'),
    path('idh-mundial/', views.idh_mundial, name='idh_mundial'),
    path('impostometro/', views.impostometro, name='impostometro'),
    path('panorama-macroeconomico/', views.panorama_macroeconomico, name='panorama_macroeconomico'),
    path('pib-mundial/', views.pib_mundial, name='pib_mundial'),
]
