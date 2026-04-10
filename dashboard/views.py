import json
import os
import re
import unicodedata
from io import BytesIO

import pandas as pd
import requests
from django.shortcuts import render

CACHE_FILE = "gdp_data_cache.csv"
PER_CAPITA_CACHE_FILE = "gdp_per_capita_cache.csv"
GDP_REAL_GROWTH_CACHE_FILE = "gdp_growth_real_cache.csv"
INFLATION_CACHE_FILE = "inflation_data_cache.csv"
UNEMPLOYMENT_CACHE_FILE = "unemployment_data_cache.csv"
CURRENT_ACCOUNT_CACHE_FILE = "current_account_data_cache.csv"
EXPORTS_CACHE_FILE = "exports_data_cache.csv"
REAL_INTEREST_RATE_CACHE_FILE = "real_interest_rate_cache.csv"
IMF_WEO_CACHE_FILE = "imf_weo_cache.csv"
IMF_WEO_DATA_URL = "https://data.imf.org/-/media/iData/External%20Storage/Documents/5661B7CB2FCC4A56866765D4281AEF01/en/WEOOct2025all"
WORLD_BANK_COUNTRY_CACHE_FILE = "world_bank_countries_cache.csv"
COUNTRY_NAME_CACHE_FILE = "country_names_locale_cache.csv"
CURRENCY_CATALOG_CACHE_FILE = "currency_catalog_cache.csv"
CRYPTO_MARKETS_CACHE_FILE = "crypto_markets_cache.csv"
GLOBAL_STOCKS_CACHE_FILE = "global_stocks_cache.csv"
HDI_CACHE_FILE = "hdi_data_cache.csv"
IMPOSTOMETRO_API_URL = "https://impostometro.com.br/Contador/Brasil"
IMPOSTOMETRO_SOURCE_URL = "https://impostometro.com.br/"
IMPOSTOMETRO_METHODOLOGY_URL = "https://impostometro.com.br/home/metodologiaimpostometro"
IMPOSTOMETRO_LIVE_CACHE_TTL_SECONDS = 300
IMPOSTOMETRO_HISTORY_CACHE_TTL_SECONDS = 86400
IMPOSTOMETRO_HISTORY_YEARS = 10
IMPOSTOMETRO_PERSISTED_CACHE_FILE = "impostometro_cache.json"
IMPOSTOMETRO_ANNUAL_SEED_BRL = {
    2016: 2_004_536_531_089,
    2017: 2_172_053_819_243,
    2018: 2_388_541_448_792,
    2019: 2_504_853_948_529,
    2020: 2_057_746_503_833,
    2021: 2_592_601_562_926,
    2022: 2_890_489_835_290,
    2023: 3_059_131_305_527,
    2024: 3_632_250_571_342,
    2025: 3_985_305_326_877,
}
# Annual federal arrecadação managed by RFB (federal + INSS, in BRL)
# Source: Receita Federal do Brasil – Análise da Arrecadação das Receitas Federais
FEDERAL_ARRECADACAO_ANUAL_BRL = {
    2005: 548_700_000_000,
    2006: 622_300_000_000,
    2007: 733_800_000_000,
    2008: 846_300_000_000,
    2009: 841_300_000_000,
    2010: 849_700_000_000,
    2011: 998_300_000_000,
    2012: 1_097_300_000_000,
    2013: 1_168_200_000_000,
    2014: 1_221_200_000_000,
    2015: 1_198_900_000_000,
    2016: 1_267_800_000_000,
    2017: 1_312_600_000_000,
    2018: 1_421_300_000_000,
    2019: 1_510_200_000_000,
    2020: 1_483_800_000_000,
    2021: 1_876_300_000_000,
    2022: 2_069_300_000_000,
    2023: 2_241_100_000_000,
    2024: 2_655_500_000_000,
    2025: 2_920_000_000_000,
}
HDI_SOURCE_PAGE_URL = "https://hdr.undp.org/data-center/human-development-index"
HDI_DATA_FALLBACK_URL = "https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_HDI_Table.xlsx"
HDI_TIME_SERIES_CACHE_FILE = "hdi_time_series_cache.csv"
HDI_TIME_SERIES_FALLBACK_URL = "https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv"
HDI_REPORT_PAGE_URL_TEMPLATE = "https://hdr.undp.org/content/human-development-report-{edition}"
CURRENCY_API_URL = "https://api.frankfurter.dev/v2"
CURRENCY_DEFAULT_BASE = "BRL"
CURRENCY_QUICK_QUOTES = ["USD", "EUR", "GBP", "JPY", "ARS", "CNY"]
CRYPTO_API_URL = "https://api.coingecko.com/api/v3"
GLOBAL_STOCKS_API_URL = "https://query1.finance.yahoo.com/v7/finance/spark"
CRYPTO_DEFAULT_IDS = [
    "bitcoin",
    "ethereum",
    "tether",
    "binancecoin",
    "solana",
    "ripple",
    "cardano",
    "dogecoin",
]
GLOBAL_STOCK_DEFAULT_BASE = CURRENCY_DEFAULT_BASE
GLOBAL_STOCK_DEFAULT_CAPITAL = 10000.0
GLOBAL_STOCK_MARKET_FILTERS = {
    "all": {"label": "Todos os mercados"},
    "us": {"label": "Estados Unidos"},
    "europe": {"label": "Europa"},
    "asia": {"label": "Asia-Pacifico"},
    "latam": {"label": "America Latina"},
}
GLOBAL_STOCK_WATCHLIST = [
    {"symbol": "AAPL", "company": "Apple Inc.", "market_key": "us", "market_label": "Estados Unidos", "market_short": "EUA", "sector": "Tecnologia"},
    {"symbol": "MSFT", "company": "Microsoft Corporation", "market_key": "us", "market_label": "Estados Unidos", "market_short": "EUA", "sector": "Software e nuvem"},
    {"symbol": "NVDA", "company": "NVIDIA Corporation", "market_key": "us", "market_label": "Estados Unidos", "market_short": "EUA", "sector": "Semicondutores"},
    {"symbol": "AMZN", "company": "Amazon.com, Inc.", "market_key": "us", "market_label": "Estados Unidos", "market_short": "EUA", "sector": "Consumo e cloud"},
    {"symbol": "SAP.DE", "company": "SAP SE", "market_key": "europe", "market_label": "Europa", "market_short": "Europa", "sector": "Software empresarial"},
    {"symbol": "ASML.AS", "company": "ASML Holding N.V.", "market_key": "europe", "market_label": "Europa", "market_short": "Europa", "sector": "Equipamentos para chips"},
    {"symbol": "MC.PA", "company": "LVMH", "market_key": "europe", "market_label": "Europa", "market_short": "Europa", "sector": "Luxo"},
    {"symbol": "7203.T", "company": "Toyota Motor Corporation", "market_key": "asia", "market_label": "Asia-Pacifico", "market_short": "Asia", "sector": "Automotivo"},
    {"symbol": "6758.T", "company": "Sony Group Corporation", "market_key": "asia", "market_label": "Asia-Pacifico", "market_short": "Asia", "sector": "Tecnologia e entretenimento"},
    {"symbol": "005930.KS", "company": "Samsung Electronics Co., Ltd.", "market_key": "asia", "market_label": "Asia-Pacifico", "market_short": "Asia", "sector": "Eletronicos e chips"},
    {"symbol": "9988.HK", "company": "Alibaba Group Holding Limited", "market_key": "asia", "market_label": "Asia-Pacifico", "market_short": "Asia", "sector": "E-commerce e cloud"},
    {"symbol": "VALE3.SA", "company": "Vale S.A.", "market_key": "latam", "market_label": "America Latina", "market_short": "LatAm", "sector": "Mineracao"},
    {"symbol": "PETR4.SA", "company": "Petrobras PN", "market_key": "latam", "market_label": "America Latina", "market_short": "LatAm", "sector": "Petroleo e gas"},
    {"symbol": "ITUB4.SA", "company": "Itau Unibanco PN", "market_key": "latam", "market_label": "America Latina", "market_short": "LatAm", "sector": "Bancos"},
]
BUSINESS_DRE_SERIES = [
    {
        "year": 2023,
        "gross_revenue": 820_000_000,
        "deductions": 94_000_000,
        "cost_of_sales": 430_000_000,
        "selling_expenses": 92_000_000,
        "administrative_expenses": 58_000_000,
        "research_expenses": 22_000_000,
        "other_operating_income": 8_000_000,
        "depreciation_amortization": 38_000_000,
        "financial_expenses": 21_000_000,
        "income_tax": 24_000_000,
    },
    {
        "year": 2024,
        "gross_revenue": 930_000_000,
        "deductions": 105_000_000,
        "cost_of_sales": 476_000_000,
        "selling_expenses": 101_000_000,
        "administrative_expenses": 61_000_000,
        "research_expenses": 24_000_000,
        "other_operating_income": 11_000_000,
        "depreciation_amortization": 42_000_000,
        "financial_expenses": 18_000_000,
        "income_tax": 36_000_000,
    },
    {
        "year": 2025,
        "gross_revenue": 1_080_000_000,
        "deductions": 121_000_000,
        "cost_of_sales": 542_000_000,
        "selling_expenses": 110_000_000,
        "administrative_expenses": 67_000_000,
        "research_expenses": 26_000_000,
        "other_operating_income": 13_000_000,
        "depreciation_amortization": 45_000_000,
        "financial_expenses": 15_000_000,
        "income_tax": 54_000_000,
    },
]
BUSINESS_DRE_ROW_DEFINITIONS = [
    {"key": "gross_revenue", "label": "Receita bruta", "kind": "positive", "row_class": "is-headline"},
    {"key": "deductions", "label": "Deducoes e impostos sobre vendas", "kind": "negative", "row_class": "is-detail"},
    {"key": "net_revenue", "label": "Receita liquida", "kind": "positive", "row_class": "is-total"},
    {"key": "cost_of_sales", "label": "Custo dos produtos vendidos (CPV)", "kind": "negative", "row_class": "is-detail"},
    {"key": "gross_profit", "label": "Lucro bruto", "kind": "positive", "row_class": "is-subtotal"},
    {"key": "selling_expenses", "label": "Despesas comerciais", "kind": "negative", "row_class": "is-detail"},
    {"key": "administrative_expenses", "label": "Despesas administrativas", "kind": "negative", "row_class": "is-detail"},
    {"key": "research_expenses", "label": "Pesquisa e desenvolvimento", "kind": "negative", "row_class": "is-detail"},
    {"key": "other_operating_income", "label": "Outras receitas operacionais", "kind": "positive", "row_class": "is-detail"},
    {"key": "ebitda", "label": "EBITDA", "kind": "positive", "row_class": "is-highlight"},
    {"key": "depreciation_amortization", "label": "Depreciacao e amortizacao", "kind": "negative", "row_class": "is-detail"},
    {"key": "ebit", "label": "EBIT", "kind": "positive", "row_class": "is-subtotal"},
    {"key": "financial_expenses", "label": "Resultado financeiro", "kind": "negative", "row_class": "is-detail"},
    {"key": "pretax_income", "label": "Lucro antes do IR/CS", "kind": "positive", "row_class": "is-subtotal"},
    {"key": "income_tax", "label": "IR e CSLL", "kind": "negative", "row_class": "is-detail"},
    {"key": "net_income", "label": "Lucro liquido", "kind": "positive", "row_class": "is-highlight"},
]
BUSINESS_BALANCE_SHEET_SERIES = [
    {
        "year": 2023,
        "cash": 96_000_000,
        "accounts_receivable": 118_000_000,
        "inventory": 84_000_000,
        "other_current_assets": 32_000_000,
        "fixed_assets": 285_000_000,
        "intangible_assets": 148_000_000,
        "other_non_current_assets": 37_000_000,
        "suppliers": 92_000_000,
        "short_term_debt": 58_000_000,
        "taxes_payable": 24_000_000,
        "other_current_liabilities": 46_000_000,
        "long_term_debt": 205_000_000,
        "provisions": 35_000_000,
        "share_capital": 180_000_000,
        "reserves": 55_000_000,
        "retained_earnings": 105_000_000,
    },
    {
        "year": 2024,
        "cash": 112_000_000,
        "accounts_receivable": 136_000_000,
        "inventory": 92_000_000,
        "other_current_assets": 36_000_000,
        "fixed_assets": 302_000_000,
        "intangible_assets": 155_000_000,
        "other_non_current_assets": 42_000_000,
        "suppliers": 101_000_000,
        "short_term_debt": 62_000_000,
        "taxes_payable": 26_000_000,
        "other_current_liabilities": 50_000_000,
        "long_term_debt": 214_000_000,
        "provisions": 37_000_000,
        "share_capital": 180_000_000,
        "reserves": 67_000_000,
        "retained_earnings": 138_000_000,
    },
    {
        "year": 2025,
        "cash": 144_000_000,
        "accounts_receivable": 152_000_000,
        "inventory": 101_000_000,
        "other_current_assets": 41_000_000,
        "fixed_assets": 327_000_000,
        "intangible_assets": 162_000_000,
        "other_non_current_assets": 48_000_000,
        "suppliers": 110_000_000,
        "short_term_debt": 57_000_000,
        "taxes_payable": 29_000_000,
        "other_current_liabilities": 54_000_000,
        "long_term_debt": 220_000_000,
        "provisions": 39_000_000,
        "share_capital": 180_000_000,
        "reserves": 81_000_000,
        "retained_earnings": 205_000_000,
    },
]
BUSINESS_BALANCE_SHEET_ROW_DEFINITIONS = [
    {"key": "cash", "label": "Ativo circulante - Caixa e equivalentes", "row_class": "is-detail"},
    {"key": "accounts_receivable", "label": "Ativo circulante - Contas a receber", "row_class": "is-detail"},
    {"key": "inventory", "label": "Ativo circulante - Estoques", "row_class": "is-detail"},
    {"key": "other_current_assets", "label": "Ativo circulante - Outros ativos", "row_class": "is-detail"},
    {"key": "total_current_assets", "label": "Ativo circulante total", "row_class": "is-total"},
    {"key": "fixed_assets", "label": "Ativo nao circulante - Imobilizado", "row_class": "is-detail"},
    {"key": "intangible_assets", "label": "Ativo nao circulante - Intangivel", "row_class": "is-detail"},
    {"key": "other_non_current_assets", "label": "Ativo nao circulante - Outros ativos", "row_class": "is-detail"},
    {"key": "total_non_current_assets", "label": "Ativo nao circulante total", "row_class": "is-total"},
    {"key": "total_assets", "label": "Total do ativo", "row_class": "is-highlight"},
    {"key": "suppliers", "label": "Passivo circulante - Fornecedores", "row_class": "is-detail"},
    {"key": "short_term_debt", "label": "Passivo circulante - Divida de curto prazo", "row_class": "is-detail"},
    {"key": "taxes_payable", "label": "Passivo circulante - Tributos a recolher", "row_class": "is-detail"},
    {"key": "other_current_liabilities", "label": "Passivo circulante - Outros passivos", "row_class": "is-detail"},
    {"key": "total_current_liabilities", "label": "Passivo circulante total", "row_class": "is-total"},
    {"key": "long_term_debt", "label": "Passivo nao circulante - Divida de longo prazo", "row_class": "is-detail"},
    {"key": "provisions", "label": "Passivo nao circulante - Provisoes", "row_class": "is-detail"},
    {"key": "total_non_current_liabilities", "label": "Passivo nao circulante total", "row_class": "is-total"},
    {"key": "total_liabilities", "label": "Passivo total", "row_class": "is-subtotal"},
    {"key": "share_capital", "label": "Patrimonio liquido - Capital social", "row_class": "is-detail"},
    {"key": "reserves", "label": "Patrimonio liquido - Reservas", "row_class": "is-detail"},
    {"key": "retained_earnings", "label": "Patrimonio liquido - Lucros acumulados", "row_class": "is-detail"},
    {"key": "total_equity", "label": "Patrimonio liquido", "row_class": "is-highlight"},
    {"key": "total_liabilities_equity", "label": "Passivo + patrimonio liquido", "row_class": "is-total"},
]
BUSINESS_CASH_FLOW_SERIES = [
    {
        "year": 2023,
        "opening_cash": 82_000_000,
        "net_income": 49_000_000,
        "depreciation_amortization": 38_000_000,
        "other_operational_adjustments": 54_000_000,
        "working_capital_variation": 11_000_000,
        "interest_and_taxes_paid": 12_000_000,
        "capex": 58_000_000,
        "intangibles_investments": 14_000_000,
        "debt_raised": 40_000_000,
        "debt_repayment": 46_000_000,
        "dividends": 26_000_000,
    },
    {
        "year": 2024,
        "opening_cash": 96_000_000,
        "net_income": 78_000_000,
        "depreciation_amortization": 42_000_000,
        "other_operational_adjustments": 59_000_000,
        "working_capital_variation": 14_000_000,
        "interest_and_taxes_paid": 19_000_000,
        "capex": 71_000_000,
        "intangibles_investments": 17_000_000,
        "debt_raised": 35_000_000,
        "debt_repayment": 44_000_000,
        "dividends": 33_000_000,
    },
    {
        "year": 2025,
        "opening_cash": 112_000_000,
        "net_income": 113_000_000,
        "depreciation_amortization": 45_000_000,
        "other_operational_adjustments": 69_000_000,
        "working_capital_variation": 12_000_000,
        "interest_and_taxes_paid": 22_000_000,
        "capex": 83_000_000,
        "intangibles_investments": 26_000_000,
        "debt_raised": 24_000_000,
        "debt_repayment": 51_000_000,
        "dividends": 25_000_000,
    },
]
BUSINESS_CASH_FLOW_ROW_DEFINITIONS = [
    {"key": "opening_cash", "label": "Caixa inicial", "kind": "positive", "row_class": "is-headline"},
    {"key": "net_income", "label": "Lucro liquido", "kind": "positive", "row_class": "is-detail"},
    {"key": "depreciation_amortization", "label": "Depreciacao e amortizacao", "kind": "positive", "row_class": "is-detail"},
    {"key": "other_operational_adjustments", "label": "Outros ajustes operacionais", "kind": "positive", "row_class": "is-detail"},
    {"key": "working_capital_variation", "label": "Consumo de capital de giro", "kind": "negative", "row_class": "is-detail"},
    {"key": "interest_and_taxes_paid", "label": "Juros e tributos pagos", "kind": "negative", "row_class": "is-detail"},
    {"key": "operating_cash_flow", "label": "Fluxo de caixa operacional", "kind": "positive", "row_class": "is-highlight"},
    {"key": "capex", "label": "Capex", "kind": "negative", "row_class": "is-detail"},
    {"key": "intangibles_investments", "label": "Investimentos em intangivel e software", "kind": "negative", "row_class": "is-detail"},
    {"key": "investing_cash_flow", "label": "Fluxo de caixa de investimentos", "kind": "negative", "row_class": "is-subtotal"},
    {"key": "debt_raised", "label": "Captacao de divida", "kind": "positive", "row_class": "is-detail"},
    {"key": "debt_repayment", "label": "Amortizacao de divida", "kind": "negative", "row_class": "is-detail"},
    {"key": "dividends", "label": "Dividendos e JCP", "kind": "negative", "row_class": "is-detail"},
    {"key": "financing_cash_flow", "label": "Fluxo de caixa de financiamento", "kind": "negative", "row_class": "is-subtotal"},
    {"key": "free_cash_flow", "label": "Fluxo de caixa livre", "kind": "positive", "row_class": "is-total"},
    {"key": "net_change_in_cash", "label": "Variacao liquida de caixa", "kind": "positive", "row_class": "is-highlight"},
    {"key": "ending_cash", "label": "Caixa final", "kind": "positive", "row_class": "is-total"},
]
BUSINESS_CAPEX_SERIES = [
    {
        "year": 2023,
        "maintenance_capex": 24_000_000,
        "expansion_capex": 22_000_000,
        "technology_capex": 12_000_000,
        "intangibles_investments": 14_000_000,
    },
    {
        "year": 2024,
        "maintenance_capex": 28_000_000,
        "expansion_capex": 27_000_000,
        "technology_capex": 16_000_000,
        "intangibles_investments": 17_000_000,
    },
    {
        "year": 2025,
        "maintenance_capex": 31_000_000,
        "expansion_capex": 34_000_000,
        "technology_capex": 18_000_000,
        "intangibles_investments": 26_000_000,
    },
]
BUSINESS_CAPEX_ROW_DEFINITIONS = [
    {"key": "maintenance_capex", "label": "Capex de manutencao", "kind": "negative", "row_class": "is-detail"},
    {"key": "expansion_capex", "label": "Capex de expansao", "kind": "negative", "row_class": "is-detail"},
    {"key": "technology_capex", "label": "Capex de tecnologia e automacao", "kind": "negative", "row_class": "is-detail"},
    {"key": "total_physical_capex", "label": "Capex fisico total", "kind": "negative", "row_class": "is-subtotal"},
    {"key": "intangibles_investments", "label": "Investimentos em intangivel e software", "kind": "negative", "row_class": "is-detail"},
    {"key": "total_investments", "label": "Investimento total", "kind": "negative", "row_class": "is-highlight"},
    {"key": "depreciation_amortization", "label": "Depreciacao e amortizacao", "kind": "positive", "row_class": "is-detail"},
    {"key": "free_cash_after_investments", "label": "Caixa livre apos investimentos", "kind": "positive", "row_class": "is-total"},
]
BUSINESS_OPEX_ROW_DEFINITIONS = [
    {"key": "gross_profit", "label": "Lucro bruto", "kind": "positive", "row_class": "is-headline"},
    {"key": "selling_expenses", "label": "Despesas comerciais", "kind": "negative", "row_class": "is-detail"},
    {"key": "administrative_expenses", "label": "Despesas administrativas", "kind": "negative", "row_class": "is-detail"},
    {"key": "research_expenses", "label": "Pesquisa e desenvolvimento", "kind": "negative", "row_class": "is-detail"},
    {"key": "sgna", "label": "SG&A", "kind": "negative", "row_class": "is-subtotal"},
    {"key": "total_opex", "label": "Opex total", "kind": "negative", "row_class": "is-highlight"},
    {"key": "other_operating_income", "label": "Outras receitas operacionais", "kind": "positive", "row_class": "is-detail"},
    {"key": "net_opex", "label": "Opex liquido", "kind": "negative", "row_class": "is-total"},
    {"key": "ebitda", "label": "EBITDA", "kind": "positive", "row_class": "is-highlight"},
]
BUSINESS_CAPEX_PROJECTS = [
    {
        "name": "Automacao da linha industrial",
        "focus": "Produtividade e capacidade",
        "year": 2025,
        "investment": 28_000_000,
        "annual_cash_gain": 8_400_000,
        "annual_ebit_gain": 6_900_000,
    },
    {
        "name": "Hub de analytics comercial",
        "focus": "Receita e eficiencia comercial",
        "year": 2025,
        "investment": 16_000_000,
        "annual_cash_gain": 5_600_000,
        "annual_ebit_gain": 4_800_000,
    },
    {
        "name": "Expansao do centro de distribuicao",
        "focus": "Escala logistica",
        "year": 2025,
        "investment": 44_000_000,
        "annual_cash_gain": 11_000_000,
        "annual_ebit_gain": 9_200_000,
    },
]
BUSINESS_INDICATOR_ROW_DEFINITIONS = [
    {"key": "roe", "label": "ROE", "unit": "percent", "row_class": "is-highlight"},
    {"key": "roa", "label": "ROA", "unit": "percent", "row_class": "is-detail"},
    {"key": "roic", "label": "ROIC", "unit": "percent", "row_class": "is-highlight"},
    {"key": "wacc", "label": "WACC", "unit": "percent", "row_class": "is-detail"},
    {"key": "roic_spread", "label": "Spread ROIC - WACC", "unit": "pp", "row_class": "is-total"},
    {"key": "eva", "label": "EVA", "unit": "currency", "row_class": "is-total"},
    {"key": "contribution_margin", "label": "Margem de contribuicao", "unit": "percent", "row_class": "is-highlight"},
    {"key": "interest_coverage", "label": "Cobertura de juros", "unit": "multiple", "row_class": "is-detail"},
    {"key": "cash_conversion_ebitda_pct", "label": "Conversao de caixa / EBITDA", "unit": "percent", "row_class": "is-detail"},
    {"key": "fcf_yield_revenue", "label": "FCF yield interno / receita", "unit": "percent", "row_class": "is-detail"},
    {"key": "fcf_yield_ebitda", "label": "FCF yield interno / EBITDA", "unit": "percent", "row_class": "is-detail"},
    {"key": "pmr", "label": "PMR", "unit": "days", "row_class": "is-detail"},
    {"key": "pmp", "label": "PMP", "unit": "days", "row_class": "is-detail"},
    {"key": "pme", "label": "PME", "unit": "days", "row_class": "is-detail"},
    {"key": "financial_cycle", "label": "Ciclo financeiro", "unit": "days", "row_class": "is-total"},
    {"key": "working_capital_need", "label": "Necessidade de capital de giro", "unit": "currency", "row_class": "is-total"},
    {"key": "net_debt_to_ebitda", "label": "Divida liquida / EBITDA", "unit": "multiple", "row_class": "is-detail"},
    {"key": "debt_to_equity_pct", "label": "Divida bruta / patrimonio liquido", "unit": "percent", "row_class": "is-detail"},
    {"key": "short_term_debt_ratio", "label": "Divida CP / divida total", "unit": "percent", "row_class": "is-detail"},
    {"key": "interest_bearing_liabilities_assets", "label": "Passivo oneroso / ativo total", "unit": "percent", "row_class": "is-detail"},
    {"key": "dscr", "label": "DSCR", "unit": "multiple", "row_class": "is-total"},
    {"key": "opex_ratio", "label": "Opex / receita", "unit": "percent", "row_class": "is-detail"},
    {"key": "sgna_ratio", "label": "SG&A / receita", "unit": "percent", "row_class": "is-detail"},
    {"key": "research_ratio", "label": "P&D / receita", "unit": "percent", "row_class": "is-detail"},
    {"key": "asset_productivity", "label": "Produtividade do ativo", "unit": "multiple", "row_class": "is-detail"},
    {"key": "invested_capital_turnover", "label": "Giro do capital investido", "unit": "multiple", "row_class": "is-detail"},
    {"key": "maintenance_ratio", "label": "Capex manutencao / receita", "unit": "percent", "row_class": "is-detail"},
    {"key": "expansion_ratio", "label": "Capex expansao / receita", "unit": "percent", "row_class": "is-detail"},
    {"key": "reinvestment_multiple", "label": "Capex / depreciacao", "unit": "multiple", "row_class": "is-detail"},
    {"key": "capex_intensity", "label": "Capex / receita", "unit": "percent", "row_class": "is-detail"},
    {"key": "operating_leverage", "label": "Alavancagem operacional", "unit": "multiple", "row_class": "is-total"},
    {"key": "break_even_revenue", "label": "Ponto de equilibrio", "unit": "currency", "row_class": "is-total"},
    {"key": "health_score", "label": "Score de saude financeira", "unit": "score", "row_class": "is-highlight"},
]
GDP_CACHE = None
GDP_PER_CAPITA_CACHE = None
GDP_REAL_GROWTH_CACHE = None
INFLATION_CACHE = None
UNEMPLOYMENT_CACHE = None
CURRENT_ACCOUNT_CACHE = None
EXPORTS_CACHE = None
REAL_INTEREST_RATE_CACHE = None
IMF_WEO_CACHE = None
WORLD_BANK_COUNTRY_CACHE = None
COUNTRY_NAME_CACHE = None
CURRENCY_CATALOG_CACHE = None
CRYPTO_MARKETS_CACHE = None
GLOBAL_STOCKS_CACHE = None
HDI_CACHE = None
HDI_TIME_SERIES_CACHE = None
HDI_DATA_URL_CACHE = HDI_DATA_FALLBACK_URL
HDI_TIME_SERIES_URL_CACHE = HDI_TIME_SERIES_FALLBACK_URL
IMPOSTOMETRO_RANGE_CACHE = {}
IMPOSTOMETRO_RANGE_CACHE_TS = {}
IMPOSTOMETRO_PERSISTED_CACHE = None
LIMIT_OPTIONS = [10, 25, 50, 100, 0]
METRIC_DEFINITIONS = {
    "gdp": {"label": "PIB Nominal (US$)", "short_label": "PIB", "field": "gdp"},
    "growth": {"label": "Crescimento do PIB (%)", "short_label": "Crescimento do PIB", "field": "growth_pct"},
    "per_capita": {"label": "PIB per capita (US$)", "short_label": "PIB per capita", "field": "gdp_per_capita"},
}
MACRO_METRIC_DEFINITIONS = {
    "gdp": {"label": "PIB Nominal (US$)", "short_label": "PIB nominal", "field": "gdp"},
    "growth_real": {"label": "Crescimento real do PIB (%)", "short_label": "Crescimento real", "field": "growth_real_pct"},
    "per_capita": {"label": "PIB per capita (US$)", "short_label": "PIB per capita", "field": "gdp_per_capita"},
    "inflation": {"label": "Inflacao ao consumidor (%)", "short_label": "Inflacao", "field": "inflation_pct"},
    "unemployment": {
        "label": "Desemprego (% da forca de trabalho)",
        "short_label": "Desemprego",
        "field": "unemployment_pct",
    },
    "current_account": {
        "label": "Conta corrente (US$)",
        "short_label": "Conta corrente",
        "field": "current_account_usd",
    },
    "exports": {"label": "Exportacoes de bens e servicos (US$)", "short_label": "Exportacoes", "field": "exports_usd"},
    "real_interest": {"label": "Juros reais (%)", "short_label": "Juros reais", "field": "real_interest_pct"},
}
HDI_GROUP_FILTERS = {
    "all": {"label": "Todas as faixas", "group": None},
    "very-high": {"label": "Muito alto desenvolvimento humano", "group": "Very high human development"},
    "high": {"label": "Alto desenvolvimento humano", "group": "High human development"},
    "medium": {"label": "Medio desenvolvimento humano", "group": "Medium human development"},
    "low": {"label": "Baixo desenvolvimento humano", "group": "Low human development"},
}
HDI_GROUP_LABELS = {
    config["group"]: config["label"]
    for config in HDI_GROUP_FILTERS.values()
    if config["group"]
}
COUNTRY_NAME_ALIASES = {
    "Bahamas, The": "Bahamas",
    "Bolivia": "Bolivia",
    "bolivia (plurinational state of)": "Bolivia",
    "Brunei Darussalam": "Brunei",
    "brunei darussalam": "Brunei",
    "Cabo Verde": "Cabo Verde",
    "cabo verde": "Cabo Verde",
    "China, People's Republic of": "China",
    "congo": "Republica do Congo",
    "Congo, Dem. Rep.": "Republica Democratica do Congo",
    "congo (democratic republic of the)": "Republica Democratica do Congo",
    "Congo, Rep.": "Republica do Congo",
    "cote d'ivoire": "Costa do Marfim",
    "Czechia": "Chequia",
    "Egypt, Arab Rep.": "Egito",
    "eswatini (kingdom of)": "Essuatini",
    "Gambia, The": "Gambia",
    "Hong Kong SAR, China": "Hong Kong",
    "hong kong, china (sar)": "Hong Kong",
    "Iran, Islamic Rep.": "Ira",
    "iran (islamic republic of)": "Ira",
    "korea (republic of)": "Coreia do Sul",
    "Korea, Dem. People's Rep.": "Coreia do Norte",
    "Korea, Rep.": "Coreia do Sul",
    "Kyrgyz Republic": "Quirguistao",
    "Lao PDR": "Laos",
    "lao people's democratic republic": "Laos",
    "Macao SAR, China": "Macau",
    "micronesia (federated states of)": "Micronesia",
    "moldova (republic of)": "Moldavia",
    "palestine, state of": "Palestina",
    "Russian Federation": "Russia",
    "russian federation": "Russia",
    "sao tome and principe": "Sao Tome e Principe",
    "Slovak Republic": "Eslovaquia",
    "St. Kitts and Nevis": "Sao Cristovao e Nevis",
    "St. Lucia": "Santa Lucia",
    "St. Martin (French part)": "Sao Martinho",
    "St. Vincent and the Grenadines": "Sao Vicente e Granadinas",
    "Syrian Arab Republic": "Siria",
    "syrian arab republic": "Siria",
    "tanzania (united republic of)": "Tanzania",
    "Turkiye, Republic of": "Turquia",
    "Türkiye, Republic of": "Turquia",
    "Turkiye": "Turquia",
    "turkiye": "Turquia",
    "venezuela (bolivarian republic of)": "Venezuela",
    "Venezuela, RB": "Venezuela",
    "viet nam": "Vietna",
    "Yemen, Rep.": "Iemen",
}


def normalize_columns(dataframe):
    if dataframe.empty:
        return dataframe

    return dataframe.rename(
        columns={
            "PaÃƒÂ­s": "Pais",
            "PaÃ­s": "Pais",
            "País": "Pais",
            "Country": "Pais",
            "Year": "Ano",
            "GDP": "gdp",
            "GDP per capita": "gdp_per_capita",
        }
    )


def normalize_currency_catalog(dataframe):
    if dataframe.empty:
        return pd.DataFrame(columns=["code", "name", "symbol"])

    normalized = dataframe.rename(columns={"iso_code": "code"}).copy()
    for column in ["code", "name", "symbol"]:
        if column not in normalized.columns:
            normalized[column] = ""
    normalized["code"] = normalized["code"].astype(str).str.upper()
    normalized["name"] = normalized["name"].fillna("")
    normalized["symbol"] = normalized["symbol"].fillna("")
    return normalized[["code", "name", "symbol"]].drop_duplicates(subset=["code"]).sort_values("code")


def normalize_global_stock_quotes(dataframe):
    columns = [
        "symbol",
        "company",
        "market_key",
        "market_label",
        "market_short",
        "sector",
        "currency",
        "exchange",
        "price_local",
        "previous_close",
        "day_high",
        "day_low",
        "volume",
        "regular_market_time",
        "market_state",
    ]
    if dataframe.empty:
        return pd.DataFrame(columns=columns)

    normalized = dataframe.copy()
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = ""

    for column in ["symbol", "company", "market_key", "market_label", "market_short", "sector", "currency", "exchange", "market_state"]:
        normalized[column] = normalized[column].fillna("")

    for column in ["price_local", "previous_close", "day_high", "day_low", "volume", "regular_market_time"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["symbol"] = normalized["symbol"].astype(str).str.upper()
    return normalized[columns].drop_duplicates(subset=["symbol"]).sort_values(["market_key", "symbol"])


def load_indicator_data(cache_name, cache_file, indicator_code, value_column, force=False):
    global GDP_CACHE, GDP_PER_CAPITA_CACHE

    if force:
        globals()[cache_name] = None

    cached = globals().get(cache_name)
    if cached is not None and not cached.empty:
        return cached

    if os.path.exists(cache_file):
        cached = normalize_columns(pd.read_csv(cache_file))
        if not cached.empty:
            if "Ano" in cached.columns:
                cached["Ano"] = cached["Ano"].astype(int)
            globals()[cache_name] = cached
            return cached

    url = f"https://api.worldbank.org/v2/country/all/indicator/{indicator_code}?format=json&per_page=20000"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
        rows = [
            {"Pais": item["country"]["value"], "Ano": int(item["date"]), value_column: item["value"]}
            for item in payload[1]
            if item["value"] is not None
        ]
        cached = pd.DataFrame(rows).sort_values(["Pais", "Ano"])
        cached.to_csv(cache_file, index=False)
        globals()[cache_name] = cached
        return cached
    except Exception:
        return pd.DataFrame(columns=["Pais", "Ano", value_column])


def load_gdp_data(force=False):
    return load_indicator_data("GDP_CACHE", CACHE_FILE, "NY.GDP.MKTP.CD", "gdp", force=force)


def load_gdp_per_capita_data(force=False):
    return load_indicator_data(
        "GDP_PER_CAPITA_CACHE",
        PER_CAPITA_CACHE_FILE,
        "NY.GDP.PCAP.CD",
        "gdp_per_capita",
        force=force,
    )


def load_gdp_growth_real_data(force=False):
    return load_indicator_data(
        "GDP_REAL_GROWTH_CACHE",
        GDP_REAL_GROWTH_CACHE_FILE,
        "NY.GDP.MKTP.KD.ZG",
        "growth_real_pct",
        force=force,
    )


def load_inflation_data(force=False):
    return load_indicator_data(
        "INFLATION_CACHE",
        INFLATION_CACHE_FILE,
        "FP.CPI.TOTL.ZG",
        "inflation_pct",
        force=force,
    )


def load_unemployment_data(force=False):
    return load_indicator_data(
        "UNEMPLOYMENT_CACHE",
        UNEMPLOYMENT_CACHE_FILE,
        "SL.UEM.TOTL.ZS",
        "unemployment_pct",
        force=force,
    )


def build_currency_rates_cache_file(base_currency):
    return f"currency_rates_{str(base_currency or CURRENCY_DEFAULT_BASE).strip().lower()}_cache.csv"


def load_currency_catalog(force=False):
    global CURRENCY_CATALOG_CACHE

    if force:
        CURRENCY_CATALOG_CACHE = None

    if CURRENCY_CATALOG_CACHE is not None and not CURRENCY_CATALOG_CACHE.empty:
        return CURRENCY_CATALOG_CACHE

    if os.path.exists(CURRENCY_CATALOG_CACHE_FILE):
        CURRENCY_CATALOG_CACHE = normalize_currency_catalog(pd.read_csv(CURRENCY_CATALOG_CACHE_FILE))
        if not CURRENCY_CATALOG_CACHE.empty:
            return CURRENCY_CATALOG_CACHE

    try:
        response = requests.get(f"{CURRENCY_API_URL}/currencies", timeout=30)
        response.raise_for_status()
        payload = response.json()
        rows = [
            {
                "code": item.get("iso_code"),
                "name": item.get("name"),
                "symbol": item.get("symbol"),
            }
            for item in payload
        ]
        CURRENCY_CATALOG_CACHE = normalize_currency_catalog(pd.DataFrame(rows))
        if not CURRENCY_CATALOG_CACHE.empty:
            CURRENCY_CATALOG_CACHE.to_csv(CURRENCY_CATALOG_CACHE_FILE, index=False)
        return CURRENCY_CATALOG_CACHE
    except Exception:
        return normalize_currency_catalog(pd.DataFrame())


def load_currency_rates(base_currency, quotes=None, force=False):
    base_code = str(base_currency or CURRENCY_DEFAULT_BASE).strip().upper()
    cache_file = build_currency_rates_cache_file(base_code)
    quote_codes = []
    if quotes:
        quote_codes = [
            str(code).strip().upper()
            for code in quotes
            if str(code).strip() and str(code).strip().upper() != base_code
        ]

    if not force and os.path.exists(cache_file):
        cached = pd.read_csv(cache_file)
        if not cached.empty:
            cached["quote"] = cached["quote"].astype(str).str.upper()
            if quote_codes:
                cached = cached[cached["quote"].isin(quote_codes)].copy()
            if not cached.empty:
                cached["rate"] = pd.to_numeric(cached["rate"], errors="coerce")
                return cached

    params = {"base": base_code}
    if quote_codes:
        params["quotes"] = ",".join(quote_codes)

    try:
        response = requests.get(f"{CURRENCY_API_URL}/rates", params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        rows = [
            {
                "date": item.get("date"),
                "base": item.get("base"),
                "quote": item.get("quote"),
                "rate": item.get("rate"),
            }
            for item in payload
        ]
        dataframe = pd.DataFrame(rows)
        if not dataframe.empty:
            dataframe["quote"] = dataframe["quote"].astype(str).str.upper()
            dataframe["rate"] = pd.to_numeric(dataframe["rate"], errors="coerce")
            dataframe.to_csv(cache_file, index=False)
        return dataframe
    except Exception:
        if os.path.exists(cache_file):
            cached = pd.read_csv(cache_file)
            if not cached.empty:
                cached["quote"] = cached["quote"].astype(str).str.upper()
                if quote_codes:
                    cached = cached[cached["quote"].isin(quote_codes)].copy()
                cached["rate"] = pd.to_numeric(cached["rate"], errors="coerce")
                return cached
        return pd.DataFrame(columns=["date", "base", "quote", "rate"])


def load_crypto_markets(force=False):
    global CRYPTO_MARKETS_CACHE

    if force:
        CRYPTO_MARKETS_CACHE = None

    if CRYPTO_MARKETS_CACHE is not None and not CRYPTO_MARKETS_CACHE.empty:
        return CRYPTO_MARKETS_CACHE

    if os.path.exists(CRYPTO_MARKETS_CACHE_FILE):
        CRYPTO_MARKETS_CACHE = pd.read_csv(CRYPTO_MARKETS_CACHE_FILE)
        if not CRYPTO_MARKETS_CACHE.empty:
            for column in ["current_price_usd", "price_change_pct_24h", "market_cap_rank"]:
                if column in CRYPTO_MARKETS_CACHE.columns:
                    CRYPTO_MARKETS_CACHE[column] = pd.to_numeric(CRYPTO_MARKETS_CACHE[column], errors="coerce")
            return CRYPTO_MARKETS_CACHE

    params = {
        "vs_currency": "usd",
        "ids": ",".join(CRYPTO_DEFAULT_IDS),
        "price_change_percentage": "24h",
        "sparkline": "false",
    }
    try:
        response = requests.get(f"{CRYPTO_API_URL}/coins/markets", params=params, timeout=30, headers={"accept": "application/json"})
        response.raise_for_status()
        payload = response.json()
        rows = [
            {
                "id": item.get("id"),
                "symbol": str(item.get("symbol") or "").upper(),
                "name": item.get("name"),
                "current_price_usd": item.get("current_price"),
                "price_change_pct_24h": item.get("price_change_percentage_24h"),
                "market_cap_rank": item.get("market_cap_rank"),
            }
            for item in payload
        ]
        CRYPTO_MARKETS_CACHE = pd.DataFrame(rows)
        if not CRYPTO_MARKETS_CACHE.empty:
            CRYPTO_MARKETS_CACHE.to_csv(CRYPTO_MARKETS_CACHE_FILE, index=False)
        return CRYPTO_MARKETS_CACHE
    except Exception:
        if os.path.exists(CRYPTO_MARKETS_CACHE_FILE):
            cached = pd.read_csv(CRYPTO_MARKETS_CACHE_FILE)
            if not cached.empty:
                for column in ["current_price_usd", "price_change_pct_24h", "market_cap_rank"]:
                    if column in cached.columns:
                        cached[column] = pd.to_numeric(cached[column], errors="coerce")
                return cached
        return pd.DataFrame(columns=["id", "symbol", "name", "current_price_usd", "price_change_pct_24h", "market_cap_rank"])


def load_global_stock_quotes(force=False):
    global GLOBAL_STOCKS_CACHE

    if force:
        GLOBAL_STOCKS_CACHE = None

    if GLOBAL_STOCKS_CACHE is not None and not GLOBAL_STOCKS_CACHE.empty:
        return GLOBAL_STOCKS_CACHE

    if os.path.exists(GLOBAL_STOCKS_CACHE_FILE):
        GLOBAL_STOCKS_CACHE = normalize_global_stock_quotes(pd.read_csv(GLOBAL_STOCKS_CACHE_FILE))
        if not GLOBAL_STOCKS_CACHE.empty:
            return GLOBAL_STOCKS_CACHE

    watchlist_map = {item["symbol"]: item for item in GLOBAL_STOCK_WATCHLIST}
    params = {
        "symbols": ",".join(watchlist_map.keys()),
        "range": "1d",
        "interval": "1d",
    }
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
    }

    try:
        response = requests.get(GLOBAL_STOCKS_API_URL, params=params, timeout=30, headers=headers)
        response.raise_for_status()
        payload = response.json()
        rows = []
        for item in payload.get("spark", {}).get("result", []):
            symbol = str(item.get("symbol") or "").upper()
            metadata = watchlist_map.get(symbol, {})
            response_payload = item.get("response") or []
            if not response_payload:
                continue

            stock_payload = response_payload[0]
            meta = stock_payload.get("meta") or {}
            quote_block = (((stock_payload.get("indicators") or {}).get("quote") or [{}])[0])
            close_prices = [value for value in (quote_block.get("close") or []) if value is not None]
            price_local = meta.get("regularMarketPrice")
            if price_local is None and close_prices:
                price_local = close_prices[-1]

            rows.append(
                {
                    "symbol": symbol,
                    "company": meta.get("longName") or meta.get("shortName") or metadata.get("company") or symbol,
                    "market_key": metadata.get("market_key", ""),
                    "market_label": metadata.get("market_label", ""),
                    "market_short": metadata.get("market_short", ""),
                    "sector": metadata.get("sector", ""),
                    "currency": meta.get("currency", ""),
                    "exchange": meta.get("fullExchangeName") or meta.get("exchangeName") or "",
                    "price_local": price_local,
                    "previous_close": meta.get("chartPreviousClose") or meta.get("previousClose"),
                    "day_high": meta.get("regularMarketDayHigh"),
                    "day_low": meta.get("regularMarketDayLow"),
                    "volume": meta.get("regularMarketVolume"),
                    "regular_market_time": meta.get("regularMarketTime"),
                    "market_state": meta.get("marketState", ""),
                }
            )

        GLOBAL_STOCKS_CACHE = normalize_global_stock_quotes(pd.DataFrame(rows))
        if not GLOBAL_STOCKS_CACHE.empty:
            GLOBAL_STOCKS_CACHE.to_csv(GLOBAL_STOCKS_CACHE_FILE, index=False)
        return GLOBAL_STOCKS_CACHE
    except Exception:
        if os.path.exists(GLOBAL_STOCKS_CACHE_FILE):
            cached = normalize_global_stock_quotes(pd.read_csv(GLOBAL_STOCKS_CACHE_FILE))
            if not cached.empty:
                return cached
        return normalize_global_stock_quotes(pd.DataFrame())


def load_current_account_data(force=False):
    return load_indicator_data(
        "CURRENT_ACCOUNT_CACHE",
        CURRENT_ACCOUNT_CACHE_FILE,
        "BN.CAB.XOKA.CD",
        "current_account_usd",
        force=force,
    )


def load_exports_data(force=False):
    return load_indicator_data(
        "EXPORTS_CACHE",
        EXPORTS_CACHE_FILE,
        "NE.EXP.GNFS.CD",
        "exports_usd",
        force=force,
    )


def load_real_interest_rate_data(force=False):
    return load_indicator_data(
        "REAL_INTEREST_RATE_CACHE",
        REAL_INTEREST_RATE_CACHE_FILE,
        "FR.INR.RINR",
        "real_interest_pct",
        force=force,
    )


def load_imf_weo_data(force=False):
    global IMF_WEO_CACHE

    if force:
        IMF_WEO_CACHE = None

    if IMF_WEO_CACHE is not None and not IMF_WEO_CACHE.empty:
        return IMF_WEO_CACHE

    if os.path.exists(IMF_WEO_CACHE_FILE):
        IMF_WEO_CACHE = normalize_columns(pd.read_csv(IMF_WEO_CACHE_FILE))
        if not IMF_WEO_CACHE.empty:
            IMF_WEO_CACHE["Ano"] = IMF_WEO_CACHE["Ano"].astype(int)
            return IMF_WEO_CACHE

    try:
        raw = pd.read_excel(IMF_WEO_DATA_URL, sheet_name="Countries")
        year_columns = [column for column in raw.columns if isinstance(column, int)]
        indicators = {
            "NGDPD": "gdp",
            "NGDP_RPCH": "growth_pct",
            "NGDPDPC": "gdp_per_capita",
        }
        filtered = raw[raw["INDICATOR.ID"].isin(indicators)].copy()
        melted = filtered.melt(
            id_vars=["COUNTRY.ID", "COUNTRY", "INDICATOR.ID"],
            value_vars=year_columns,
            var_name="Ano",
            value_name="value",
        )
        melted = melted[melted["value"].notna()].copy()
        melted["metric"] = melted["INDICATOR.ID"].map(indicators)

        IMF_WEO_CACHE = (
            melted.pivot_table(
                index=["COUNTRY.ID", "COUNTRY", "Ano"],
                columns="metric",
                values="value",
                aggfunc="first",
            )
            .reset_index()
            .rename(columns={"COUNTRY": "Pais"})
        )
        IMF_WEO_CACHE["Ano"] = IMF_WEO_CACHE["Ano"].astype(int)
        if "gdp" in IMF_WEO_CACHE.columns:
            IMF_WEO_CACHE["gdp"] = IMF_WEO_CACHE["gdp"] * 1_000_000_000
        IMF_WEO_CACHE.to_csv(IMF_WEO_CACHE_FILE, index=False)
        return IMF_WEO_CACHE
    except Exception:
        return pd.DataFrame(columns=["COUNTRY.ID", "Pais", "Ano", "gdp", "growth_pct", "gdp_per_capita"])


def load_world_bank_country_metadata(force=False):
    global WORLD_BANK_COUNTRY_CACHE

    if force:
        WORLD_BANK_COUNTRY_CACHE = None

    if WORLD_BANK_COUNTRY_CACHE is not None and not WORLD_BANK_COUNTRY_CACHE.empty:
        return WORLD_BANK_COUNTRY_CACHE

    if os.path.exists(WORLD_BANK_COUNTRY_CACHE_FILE):
        WORLD_BANK_COUNTRY_CACHE = pd.read_csv(WORLD_BANK_COUNTRY_CACHE_FILE)
        if not WORLD_BANK_COUNTRY_CACHE.empty:
            WORLD_BANK_COUNTRY_CACHE["is_aggregate"] = (
                WORLD_BANK_COUNTRY_CACHE["is_aggregate"].astype(str).str.lower().eq("true")
            )
            return WORLD_BANK_COUNTRY_CACHE

    url = "https://api.worldbank.org/v2/country?format=json&per_page=400"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
        rows = [
            {
                "country_code": item["id"],
                "Pais": item["name"],
                "region": item["region"]["value"],
                "is_aggregate": item["region"]["value"] == "Aggregates",
            }
            for item in payload[1]
        ]
        WORLD_BANK_COUNTRY_CACHE = pd.DataFrame(rows).sort_values("Pais")
        WORLD_BANK_COUNTRY_CACHE.to_csv(WORLD_BANK_COUNTRY_CACHE_FILE, index=False)
        return WORLD_BANK_COUNTRY_CACHE
    except Exception:
        return pd.DataFrame(columns=["country_code", "Pais", "region", "is_aggregate"])


def fmt_currency(value):
    if value is None or pd.isna(value):
        return "--"
    abs_value = abs(value)
    if abs_value >= 1e12:
        return f"US$ {value / 1e12:.2f} tri".replace(".", ",")
    if abs_value >= 1e9:
        return f"US$ {value / 1e9:.2f} bi".replace(".", ",")
    return f"US$ {value / 1e6:.2f} mi".replace(".", ",")


def fmt_rate(value):
    if value is None or pd.isna(value):
        return "--"
    abs_value = abs(value)
    if abs_value >= 100:
        return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if abs_value >= 1:
        return f"{value:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{value:,.6f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_amount_value(value):
    if value is None or pd.isna(value):
        return "--"
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_compact_number(value):
    if value is None or pd.isna(value):
        return "--"
    abs_value = abs(float(value))
    if abs_value >= 1e9:
        return f"{value / 1e9:.2f} bi".replace(".", ",")
    if abs_value >= 1e6:
        return f"{value / 1e6:.2f} mi".replace(".", ",")
    if abs_value >= 1e3:
        return f"{value / 1e3:.2f} mil".replace(".", ",")
    return f"{value:,.0f}".replace(",", ".")


def fmt_currency_full(value):
    if value is None or pd.isna(value):
        return "--"
    return f"US$ {int(round(value)):,}".replace(",", ".")


def split_currency_parts(formatted_value):
    if not formatted_value or formatted_value == "--":
        return "--", ""
    if " " not in formatted_value:
        return formatted_value, ""
    return formatted_value.split(" ", 1)


def build_full_currency_display(value):
    formatted = fmt_currency_full(value)
    prefix, number = split_currency_parts(formatted)
    return {
        "formatted": formatted,
        "prefix": prefix,
        "number": number,
    }


def fmt_currency_unit(value):
    if value is None or pd.isna(value):
        return "--"
    return f"US$ {value:,.0f}".replace(",", ".")


def fmt_brl_compact(value, digits=1):
    if value is None or pd.isna(value):
        return "--"

    prefix = "-" if value < 0 else ""
    absolute_value = abs(float(value))
    if absolute_value >= 1e12:
        scaled_value = absolute_value / 1e12
        suffix = " tri"
    elif absolute_value >= 1e9:
        scaled_value = absolute_value / 1e9
        suffix = " bi"
    elif absolute_value >= 1e6:
        scaled_value = absolute_value / 1e6
        suffix = " mi"
    elif absolute_value >= 1e3:
        scaled_value = absolute_value / 1e3
        suffix = " mil"
    else:
        return f"{prefix}R$ {absolute_value:,.0f}".replace(",", ".")

    return f"{prefix}R$ {scaled_value:.{digits}f}{suffix}".replace(".", ",")


def fmt_brl_full(value):
    if value is None or pd.isna(value):
        return "--"
    prefix = "-" if value < 0 else ""
    return f"{prefix}R$ {abs(float(value)):,.0f}".replace(",", ".")


def fmt_percent(value, signed=False):
    if value is None or pd.isna(value):
        return "--"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value:.1f}%".replace(".", ",")


def fmt_decimal(value, digits=1):
    if value is None or pd.isna(value):
        return "--"
    return f"{value:.{digits}f}".replace(".", ",")


def fmt_pp(value, signed=True):
    if value is None or pd.isna(value):
        return "--"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value:.1f} p.p.".replace(".", ",")


def fmt_multiple(value, digits=2, signed=False):
    if value is None or pd.isna(value):
        return "--"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value:.{digits}f}x".replace(".", ",")


def fmt_days(value):
    if value is None or pd.isna(value):
        return "--"
    return f"{value:.0f} dias".replace(".", ",")


def fmt_signed_days(value):
    if value is None or pd.isna(value):
        return "--"
    prefix = "+" if value > 0 else ""
    return f"{prefix}{value:.0f} dias".replace(".", ",")


def fmt_score(value):
    if value is None or pd.isna(value):
        return "--"
    return f"{value:.0f}/100"


def fmt_points(value):
    if value is None or pd.isna(value):
        return "--"
    prefix = "+" if value > 0 else ""
    return f"{prefix}{value:.0f} pts"


def format_business_indicator_value(unit, value):
    if unit == "percent":
        return fmt_percent(value)
    if unit == "pp":
        return fmt_pp(value)
    if unit == "multiple":
        return fmt_multiple(value, digits=2)
    if unit == "currency":
        return fmt_brl_compact(value)
    if unit == "days":
        return fmt_days(value)
    if unit == "score":
        return fmt_score(value)
    return fmt_decimal(value, digits=2)


def clamp_percent(value, minimum=0.0, maximum=100.0):
    if value is None or pd.isna(value):
        return minimum
    return max(minimum, min(float(value), maximum))


def build_business_line_chart(years, series_items, formatter=fmt_brl_compact, width=420, height=240):
    left_padding, right_padding, top_padding, bottom_padding = 52, 18, 20, 36
    plot_width = width - left_padding - right_padding
    plot_height = height - top_padding - bottom_padding

    all_values = [
        float(value)
        for item in series_items
        for value in item.get("values", [])
        if value is not None and not pd.isna(value)
    ]
    if not all_values:
        all_values = [0.0, 1.0]

    min_value = min(0.0, min(all_values))
    max_value = max(all_values)
    if max_value <= min_value:
        max_value = min_value + 1.0

    def x_pos(index):
        if len(years) <= 1:
            return left_padding + (plot_width / 2)
        return left_padding + ((plot_width / (len(years) - 1)) * index)

    def y_pos(value):
        ratio = (float(value) - min_value) / (max_value - min_value)
        return top_padding + plot_height - (ratio * plot_height)

    grid_lines = []
    for step in range(4):
        tick_value = min_value + ((max_value - min_value) * step / 3)
        grid_lines.append(
            {
                "y": round(y_pos(tick_value), 1),
                "label": formatter(tick_value),
            }
        )
    grid_lines.sort(key=lambda item: item["y"])

    x_labels = [{"x": round(x_pos(index), 1), "label": str(year)} for index, year in enumerate(years)]
    chart_series = []
    for item in series_items:
        points = []
        dots = []
        for index, value in enumerate(item.get("values", [])):
            x = x_pos(index)
            y = y_pos(value)
            points.append(f"{x:.1f},{y:.1f}")
            dots.append(
                {
                    "x": round(x, 1),
                    "y": round(y, 1),
                    "year": years[index],
                    "value_display": formatter(value),
                }
            )
        chart_series.append(
            {
                "label": item["label"],
                "slug": item["slug"],
                "color": item["color"],
                "points": " ".join(points),
                "dots": dots,
            }
        )

    return {
        "width": width,
        "height": height,
        "grid_lines": grid_lines,
        "x_labels": x_labels,
        "series": chart_series,
    }


def build_business_grouped_bar_chart(groups, series_items, formatter=fmt_brl_compact, width=520, height=310):
    tone_fill_map = {
        "bright": "rgba(255, 255, 255, 0.92)",
        "mid": "rgba(255, 255, 255, 0.58)",
        "soft": "rgba(255, 255, 255, 0.32)",
        "neutral": "rgba(255, 255, 255, 0.42)",
    }
    left_padding, right_padding, top_padding, bottom_padding = 44, 18, 28, 72
    plot_width = width - left_padding - right_padding
    plot_height = height - top_padding - bottom_padding
    max_value = max(
        [
            abs(float(group.get(item["key"]) or 0))
            for group in groups
            for item in series_items
        ]
        or [1.0]
    )
    if max_value <= 0:
        max_value = 1.0

    group_slot_width = plot_width / max(len(groups), 1)
    inner_padding = max(18.0, group_slot_width * 0.12)
    bar_gap = 10.0
    total_bar_gap = bar_gap * max(len(series_items) - 1, 0)
    bar_width = min(34.0, (group_slot_width - (inner_padding * 2) - total_bar_gap) / max(len(series_items), 1))
    bar_width = max(bar_width, 14.0)
    total_bar_width = (bar_width * len(series_items)) + total_bar_gap
    baseline_y = top_padding + plot_height

    grid_lines = []
    for step in range(4):
        tick_value = max_value * step / 3
        y = baseline_y - ((tick_value / max_value) * plot_height if max_value else 0)
        grid_lines.append({"y": round(y, 1), "label": formatter(tick_value)})
    grid_lines.sort(key=lambda item: item["y"])

    bars = []
    group_labels = []
    group_dividers = []
    for group_index, group in enumerate(groups):
        group_start = left_padding + (group_slot_width * group_index)
        bars_start = group_start + ((group_slot_width - total_bar_width) / 2)
        group_labels.append({"x": round(group_start + (group_slot_width / 2), 1), "label": group["label"]})
        if group_index < len(groups) - 1:
            group_dividers.append(round(group_start + group_slot_width, 1))

        for item_index, item in enumerate(series_items):
            value = abs(float(group.get(item["key"]) or 0))
            bar_height = (value / max_value) * plot_height if max_value else 0.0
            x = bars_start + (item_index * (bar_width + bar_gap))
            y = baseline_y - bar_height
            center_x = x + (bar_width / 2)
            bars.append(
                {
                    "group_label": group["label"],
                    "label": item["label"],
                    "short_label": item.get("short_label", item["label"]),
                    "tone": item.get("tone", "neutral"),
                    "fill": tone_fill_map.get(item.get("tone", "neutral"), tone_fill_map["neutral"]),
                    "x": round(x, 1),
                    "y": round(y, 1),
                    "width": round(bar_width, 1),
                    "height": round(bar_height, 1),
                    "center_x": round(center_x, 1),
                    "value_display": formatter(value),
                    "value_y": round(max(top_padding + 12, y - 8), 1),
                    "series_y": round(height - 32, 1),
                }
            )

    return {
        "width": width,
        "height": height,
        "baseline_y": round(baseline_y, 1),
        "grid_lines": grid_lines,
        "bars": bars,
        "group_labels": group_labels,
        "group_dividers": group_dividers,
        "legend": [{"label": item["label"], "tone": item.get("tone", "neutral")} for item in series_items],
        "max_value_label": formatter(max_value),
    }


def build_business_pareto_chart(items, formatter=fmt_brl_compact, width=420, height=260):
    sorted_items = sorted(
        [item for item in items if item.get("value") not in (None, 0)],
        key=lambda item: item["value"],
        reverse=True,
    )
    if not sorted_items:
        sorted_items = [{"label": "Base", "short_label": "Base", "value": 1.0}]

    left_padding, right_padding, top_padding, bottom_padding = 38, 42, 18, 54
    plot_width = width - left_padding - right_padding
    plot_height = height - top_padding - bottom_padding
    max_value = max(float(item["value"]) for item in sorted_items) or 1.0
    total_value = sum(float(item["value"]) for item in sorted_items) or 1.0
    slot_width = plot_width / len(sorted_items)
    bar_width = slot_width * 0.56

    cumulative = 0.0
    bars = []
    line_points = []
    line_dots = []
    for index, item in enumerate(sorted_items):
        value = float(item["value"])
        share_pct = (value / total_value) * 100
        cumulative += share_pct
        x = left_padding + (slot_width * index) + ((slot_width - bar_width) / 2)
        bar_height = (value / max_value) * plot_height
        y = top_padding + plot_height - bar_height
        center_x = x + (bar_width / 2)
        line_y = top_padding + plot_height - ((cumulative / 100) * plot_height)
        line_points.append(f"{center_x:.1f},{line_y:.1f}")
        line_dots.append({"x": round(center_x, 1), "y": round(line_y, 1), "label": fmt_percent(cumulative)})
        bars.append(
            {
                "label": item["label"],
                "short_label": item.get("short_label", item["label"]),
                "value_display": formatter(value),
                "share_pct": share_pct,
                "share_display": fmt_percent(share_pct),
                "cumulative_pct": cumulative,
                "cumulative_display": fmt_percent(cumulative),
                "x": round(x, 1),
                "y": round(y, 1),
                "width": round(bar_width, 1),
                "height": round(bar_height, 1),
                "center_x": round(center_x, 1),
                "fill": f"rgba(255, 255, 255, {max(0.34, 0.92 - (index * 0.08)):.2f})",
            }
        )

    left_ticks = []
    for step in range(4):
        tick_value = max_value * step / 3
        y = top_padding + plot_height - ((tick_value / max_value) * plot_height if max_value else 0)
        left_ticks.append({"y": round(y, 1), "label": formatter(tick_value)})
    left_ticks.sort(key=lambda item: item["y"])

    right_ticks = []
    for tick_value in [100, 50, 0]:
        y = top_padding + plot_height - ((tick_value / 100) * plot_height)
        right_ticks.append({"y": round(y, 1), "label": fmt_percent(tick_value)})

    return {
        "width": width,
        "height": height,
        "bars": bars,
        "line_points": " ".join(line_points),
        "line_dots": line_dots,
        "left_ticks": left_ticks,
        "right_ticks": right_ticks,
    }


def build_business_pie_chart(items, center_title, center_value):
    palette = [
        "rgba(255, 255, 255, 0.92)",
        "rgba(255, 255, 255, 0.72)",
        "rgba(255, 255, 255, 0.48)",
        "rgba(255, 255, 255, 0.26)",
        "rgba(255, 255, 255, 0.14)",
    ]
    normalized_items = [item for item in items if item.get("value") not in (None, 0)]
    total_value = sum(float(item["value"]) for item in normalized_items) or 1.0

    gradient_parts = []
    legend = []
    cursor = 0.0
    for index, item in enumerate(normalized_items):
        value = float(item["value"])
        share = (value / total_value) * 100
        start = cursor
        end = cursor + share
        color = palette[index % len(palette)]
        gradient_parts.append(f"{color} {start:.2f}% {end:.2f}%")
        legend.append(
            {
                "label": item["label"],
                "value_display": fmt_brl_compact(value),
                "share_display": fmt_percent(share),
                "color": color,
            }
        )
        cursor = end

    if not gradient_parts:
        gradient_parts = ["rgba(255, 255, 255, 0.16) 0% 100%"]

    return {
        "gradient": f"conic-gradient({', '.join(gradient_parts)})",
        "legend": legend,
        "center_title": center_title,
        "center_value": center_value,
    }


def fmt_quote_date(value):
    if value is None or pd.isna(value):
        return "--"
    try:
        return pd.to_datetime(int(value), unit="s", utc=True).strftime("%d/%m/%Y")
    except (TypeError, ValueError):
        return "--"


def fmt_signed_int(value):
    if value is None or pd.isna(value):
        return "--"
    rounded = int(round(value))
    prefix = "+" if rounded > 0 else ""
    return f"{prefix}{rounded}"


def tone_for_number(value):
    if value is None or pd.isna(value):
        return "neutral"
    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "neutral"


def parse_year(raw_year, fallback, min_year, max_year):
    try:
        year = int(raw_year)
    except (TypeError, ValueError):
        return fallback
    return min(max(year, min_year), max_year)


def parse_limit(raw_limit):
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        return 25
    return limit if limit in LIMIT_OPTIONS else 25


def parse_source(raw_source):
    return raw_source if raw_source in {"wb", "fmi"} else "wb"


def parse_metric(raw_metric):
    return raw_metric if raw_metric in METRIC_DEFINITIONS else "gdp"


def parse_macro_metric(raw_metric):
    return raw_metric if raw_metric in MACRO_METRIC_DEFINITIONS else "gdp"


def parse_hdi_group(raw_group):
    return raw_group if raw_group in HDI_GROUP_FILTERS else "all"


def parse_stock_market(raw_market):
    return raw_market if raw_market in GLOBAL_STOCK_MARKET_FILTERS else "all"


def parse_amount(raw_amount, fallback=100.0):
    try:
        amount = float(str(raw_amount).replace(",", "."))
    except (TypeError, ValueError):
        return fallback
    return max(amount, 0.0)


def convert_value_to_base(value, quote_code, selected_base, rate_map):
    if value is None or pd.isna(value):
        return None

    currency_code = str(quote_code or "").strip().upper()
    base_code = str(selected_base or "").strip().upper()
    if not currency_code:
        return None
    if currency_code == base_code:
        return value

    rate = rate_map.get(currency_code)
    if rate is None or pd.isna(rate) or rate == 0:
        return None
    return value / rate


def get_hdi_available_years(dataframe):
    if dataframe is None or dataframe.empty:
        return []

    years = []
    for column in dataframe.columns:
        if not str(column).startswith("hdi_"):
            continue
        suffix = str(column).split("_", 1)[1]
        if suffix.isdigit():
            years.append(int(suffix))
    return sorted(set(years))


def parse_language(raw_language):
    return raw_language if raw_language in {"pt", "en"} else "pt"


def normalize_search(value):
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return normalized.encode("ascii", "ignore").decode("ascii").lower().strip()


def build_business_dre_context():
    series = []
    for item in BUSINESS_DRE_SERIES:
        operating_expenses = item["selling_expenses"] + item["administrative_expenses"] + item["research_expenses"]
        net_revenue = item["gross_revenue"] - item["deductions"]
        gross_profit = net_revenue - item["cost_of_sales"]
        ebitda = gross_profit - operating_expenses + item["other_operating_income"]
        ebit = ebitda - item["depreciation_amortization"]
        pretax_income = ebit - item["financial_expenses"]
        net_income = pretax_income - item["income_tax"]
        series.append(
            {
                **item,
                "net_revenue": net_revenue,
                "gross_profit": gross_profit,
                "operating_expenses": operating_expenses,
                "ebitda": ebitda,
                "ebit": ebit,
                "pretax_income": pretax_income,
                "net_income": net_income,
                "gross_margin": (gross_profit / net_revenue) * 100 if net_revenue else None,
                "ebitda_margin": (ebitda / net_revenue) * 100 if net_revenue else None,
                "ebit_margin": (ebit / net_revenue) * 100 if net_revenue else None,
                "net_margin": (net_income / net_revenue) * 100 if net_revenue else None,
                "opex_ratio": (operating_expenses / net_revenue) * 100 if net_revenue else None,
                "financial_ratio": (item["financial_expenses"] / net_revenue) * 100 if net_revenue else None,
                "tax_rate": (item["income_tax"] / pretax_income) * 100 if pretax_income else None,
                "cash_conversion": (net_income / ebitda) * 100 if ebitda else None,
            }
        )

    years = [entry["year"] for entry in series]
    latest = series[-1]
    previous = series[-2]
    first = series[0]

    dre_rows = []
    for definition in BUSINESS_DRE_ROW_DEFINITIONS:
        cells = []
        for index, entry in enumerate(series):
            raw_value = entry.get(definition["key"])
            signed_value = -raw_value if definition["kind"] == "negative" else raw_value
            base_revenue = entry.get("net_revenue")
            previous_raw_value = series[index - 1].get(definition["key"]) if index > 0 else None
            horizontal = ((raw_value / previous_raw_value) - 1) * 100 if previous_raw_value not in (None, 0) else None
            vertical = ((signed_value / base_revenue) * 100) if base_revenue else None
            cells.append(
                {
                    "value_display": fmt_brl_compact(signed_value),
                    "value_title": fmt_brl_full(signed_value),
                    "vertical_display": fmt_percent(vertical),
                    "horizontal_display": "Base" if index == 0 else fmt_percent(horizontal, signed=True),
                    "horizontal_class": (
                        "neutral"
                        if index == 0
                        else "positive"
                        if horizontal and horizontal > 0
                        else "negative"
                        if horizontal and horizontal < 0
                        else "neutral"
                    ),
                }
            )

        dre_rows.append({"label": definition["label"], "row_class": definition["row_class"], "cells": cells})

    summary_cards = [
        {
            "kicker": "Receita liquida 2025",
            "value": fmt_brl_compact(latest["net_revenue"]),
            "chip": fmt_percent(((latest["net_revenue"] / previous["net_revenue"]) - 1) * 100, signed=True),
            "chip_class": "positive",
            "copy": f"Crescimento anual saindo de {fmt_brl_compact(first['net_revenue'])} em {first['year']} para {fmt_brl_compact(latest['net_revenue'])} em {latest['year']}.",
        },
        {
            "kicker": "EBITDA",
            "value": fmt_brl_compact(latest["ebitda"]),
            "chip": f"Margem {fmt_percent(latest['ebitda_margin'])}",
            "chip_class": "positive",
            "copy": f"O EBITDA avancou {fmt_percent(((latest['ebitda'] / previous['ebitda']) - 1) * 100, signed=True)} contra {previous['year']}, com alavancagem operacional.",
        },
        {
            "kicker": "EBIT",
            "value": fmt_brl_compact(latest["ebit"]),
            "chip": f"Margem {fmt_percent(latest['ebit_margin'])}",
            "chip_class": "positive",
            "copy": f"O lucro operacional ficou em {fmt_percent(latest['ebit_margin'])} da receita liquida apos depreciacao e amortizacao.",
        },
        {
            "kicker": "Lucro liquido",
            "value": fmt_brl_compact(latest["net_income"]),
            "chip": fmt_percent(((latest["net_income"] / previous["net_income"]) - 1) * 100, signed=True),
            "chip_class": "positive",
            "copy": f"A margem liquida encerrou {latest['year']} em {fmt_percent(latest['net_margin'])}, contra {fmt_percent(previous['net_margin'])} no ano anterior.",
        },
    ]

    margin_cards = [
        {
            "label": "Margem bruta",
            "value": fmt_percent(latest["gross_margin"]),
            "delta": fmt_pp(latest["gross_margin"] - previous["gross_margin"]),
            "delta_class": "positive" if latest["gross_margin"] >= previous["gross_margin"] else "negative",
            "copy": "Leitura da eficiencia industrial depois do CPV.",
        },
        {
            "label": "Despesas operacionais / RL",
            "value": fmt_percent(latest["opex_ratio"]),
            "delta": fmt_pp(latest["opex_ratio"] - previous["opex_ratio"]),
            "delta_class": "positive" if latest["opex_ratio"] <= previous["opex_ratio"] else "negative",
            "copy": "Soma de despesas comerciais, administrativas e P&D sobre a receita liquida.",
        },
        {
            "label": "Margem EBITDA",
            "value": fmt_percent(latest["ebitda_margin"]),
            "delta": fmt_pp(latest["ebitda_margin"] - previous["ebitda_margin"]),
            "delta_class": "positive" if latest["ebitda_margin"] >= previous["ebitda_margin"] else "negative",
            "copy": "Capacidade operacional antes da estrutura de capital e das amortizacoes.",
        },
        {
            "label": "Margem EBIT",
            "value": fmt_percent(latest["ebit_margin"]),
            "delta": fmt_pp(latest["ebit_margin"] - previous["ebit_margin"]),
            "delta_class": "positive" if latest["ebit_margin"] >= previous["ebit_margin"] else "negative",
            "copy": "Rentabilidade operacional apos depreciacao e amortizacao.",
        },
        {
            "label": "Resultado financeiro / RL",
            "value": fmt_percent(latest["financial_ratio"]),
            "delta": fmt_pp(latest["financial_ratio"] - previous["financial_ratio"]),
            "delta_class": "positive" if latest["financial_ratio"] <= previous["financial_ratio"] else "negative",
            "copy": "Peso do resultado financeiro sobre a receita liquida.",
        },
        {
            "label": "Aliquota efetiva",
            "value": fmt_percent(latest["tax_rate"]),
            "delta": fmt_pp(latest["tax_rate"] - previous["tax_rate"]),
            "delta_class": "negative" if latest["tax_rate"] > previous["tax_rate"] else "positive",
            "copy": "Relacao entre IR/CSLL e lucro antes do IR/CS.",
        },
    ]

    margin_tracks = [
        {"label": "Margem bruta", "value": fmt_percent(latest["gross_margin"]), "width": latest["gross_margin"]},
        {"label": "Margem EBITDA", "value": fmt_percent(latest["ebitda_margin"]), "width": latest["ebitda_margin"]},
        {"label": "Margem EBIT", "value": fmt_percent(latest["ebit_margin"]), "width": latest["ebit_margin"]},
        {"label": "Margem liquida", "value": fmt_percent(latest["net_margin"]), "width": latest["net_margin"]},
    ]

    insight_cards = [
        {
            "kicker": "Alavancagem operacional",
            "title": "Receita cresce acima das despesas",
            "copy": f"A receita liquida avancou {fmt_percent(((latest['net_revenue'] / previous['net_revenue']) - 1) * 100, signed=True)} em {latest['year']}, enquanto as despesas operacionais subiram {fmt_percent(((latest['operating_expenses'] / previous['operating_expenses']) - 1) * 100, signed=True)}.",
        },
        {
            "kicker": "Qualidade de margem",
            "title": "Lucro bruto e EBITDA em expansao",
            "copy": f"A margem bruta ganhou {fmt_pp(latest['gross_margin'] - previous['gross_margin'])} e a margem EBITDA abriu {fmt_pp(latest['ebitda_margin'] - previous['ebitda_margin'])}.",
        },
        {
            "kicker": "Resultado final",
            "title": "Maior retencao no lucro liquido",
            "copy": f"O lucro liquido atingiu {fmt_brl_compact(latest['net_income'])}, com conversao de {fmt_percent(latest['cash_conversion'])} do EBITDA em lucro e peso financeiro de {fmt_percent(latest['financial_ratio'])} da receita liquida.",
        },
    ]

    dre_ebitda_by_year = {entry["year"]: entry["ebitda"] for entry in series}

    balance_series = []
    for item in BUSINESS_BALANCE_SHEET_SERIES:
        total_current_assets = item["cash"] + item["accounts_receivable"] + item["inventory"] + item["other_current_assets"]
        total_non_current_assets = item["fixed_assets"] + item["intangible_assets"] + item["other_non_current_assets"]
        total_assets = total_current_assets + total_non_current_assets
        total_current_liabilities = (
            item["suppliers"] + item["short_term_debt"] + item["taxes_payable"] + item["other_current_liabilities"]
        )
        total_non_current_liabilities = item["long_term_debt"] + item["provisions"]
        total_liabilities = total_current_liabilities + total_non_current_liabilities
        total_equity = item["share_capital"] + item["reserves"] + item["retained_earnings"]
        total_liabilities_equity = total_liabilities + total_equity
        gross_debt = item["short_term_debt"] + item["long_term_debt"]
        net_debt = gross_debt - item["cash"]
        working_capital = total_current_assets - total_current_liabilities
        current_ratio = total_current_assets / total_current_liabilities if total_current_liabilities else None
        quick_ratio = (item["cash"] + item["accounts_receivable"]) / total_current_liabilities if total_current_liabilities else None
        debt_to_equity = gross_debt / total_equity if total_equity else None
        net_debt_to_ebitda = net_debt / dre_ebitda_by_year.get(item["year"]) if dre_ebitda_by_year.get(item["year"]) else None
        equity_ratio = (total_equity / total_assets) * 100 if total_assets else None
        liabilities_ratio = (total_liabilities / total_assets) * 100 if total_assets else None

        balance_series.append(
            {
                **item,
                "total_current_assets": total_current_assets,
                "total_non_current_assets": total_non_current_assets,
                "total_assets": total_assets,
                "total_current_liabilities": total_current_liabilities,
                "total_non_current_liabilities": total_non_current_liabilities,
                "total_liabilities": total_liabilities,
                "total_equity": total_equity,
                "total_liabilities_equity": total_liabilities_equity,
                "gross_debt": gross_debt,
                "net_debt": net_debt,
                "working_capital": working_capital,
                "current_ratio": current_ratio,
                "quick_ratio": quick_ratio,
                "debt_to_equity": debt_to_equity,
                "net_debt_to_ebitda": net_debt_to_ebitda,
                "equity_ratio": equity_ratio,
                "liabilities_ratio": liabilities_ratio,
            }
        )

    latest_balance = balance_series[-1]
    previous_balance = balance_series[-2]
    balance_rows = []
    for definition in BUSINESS_BALANCE_SHEET_ROW_DEFINITIONS:
        cells = []
        for index, entry in enumerate(balance_series):
            raw_value = entry.get(definition["key"])
            base_assets = entry.get("total_assets")
            previous_raw_value = balance_series[index - 1].get(definition["key"]) if index > 0 else None
            horizontal = ((raw_value / previous_raw_value) - 1) * 100 if previous_raw_value not in (None, 0) else None
            vertical = ((raw_value / base_assets) * 100) if base_assets else None
            cells.append(
                {
                    "value_display": fmt_brl_compact(raw_value),
                    "value_title": fmt_brl_full(raw_value),
                    "vertical_display": fmt_percent(vertical),
                    "horizontal_display": "Base" if index == 0 else fmt_percent(horizontal, signed=True),
                    "horizontal_class": (
                        "neutral"
                        if index == 0
                        else "positive"
                        if horizontal and horizontal > 0
                        else "negative"
                        if horizontal and horizontal < 0
                        else "neutral"
                    ),
                }
            )
        balance_rows.append({"label": definition["label"], "row_class": definition["row_class"], "cells": cells})

    balance_summary_cards = [
        {
            "kicker": "Total de ativos",
            "value": fmt_brl_compact(latest_balance["total_assets"]),
            "chip": fmt_percent(((latest_balance["total_assets"] / previous_balance["total_assets"]) - 1) * 100, signed=True),
            "chip_class": "positive",
            "copy": f"A base de ativos encerrou {latest_balance['year']} em {fmt_brl_compact(latest_balance['total_assets'])}, refletindo expansao sobre {previous_balance['year']}.",
        },
        {
            "kicker": "Capital de giro",
            "value": fmt_brl_compact(latest_balance["working_capital"]),
            "chip": f"Liquidez {fmt_multiple(latest_balance['current_ratio'], digits=2)}",
            "chip_class": "positive",
            "copy": f"O capital de giro permaneceu positivo e a liquidez corrente fechou em {fmt_multiple(latest_balance['current_ratio'], digits=2)}.",
        },
        {
            "kicker": "Divida liquida",
            "value": fmt_brl_compact(latest_balance["net_debt"]),
            "chip": fmt_multiple(latest_balance["net_debt_to_ebitda"], digits=2),
            "chip_class": "positive" if latest_balance["net_debt"] <= previous_balance["net_debt"] else "negative",
            "copy": f"A relacao divida liquida / EBITDA ficou em {fmt_multiple(latest_balance['net_debt_to_ebitda'], digits=2)}, reforcando capacidade de cobertura operacional.",
        },
        {
            "kicker": "Patrimonio liquido",
            "value": fmt_brl_compact(latest_balance["total_equity"]),
            "chip": f"PL/Ativo {fmt_percent(latest_balance['equity_ratio'])}",
            "chip_class": "positive",
            "copy": f"O patrimonio liquido atingiu {fmt_brl_compact(latest_balance['total_equity'])}, equivalente a {fmt_percent(latest_balance['equity_ratio'])} do ativo total.",
        },
    ]

    balance_ratio_cards = [
        {
            "label": "Liquidez corrente",
            "value": fmt_multiple(latest_balance["current_ratio"], digits=2),
            "delta": fmt_multiple(latest_balance["current_ratio"] - previous_balance["current_ratio"], digits=2, signed=True),
            "delta_class": "positive" if latest_balance["current_ratio"] >= previous_balance["current_ratio"] else "negative",
            "copy": "Ativo circulante dividido pelo passivo circulante.",
        },
        {
            "label": "Liquidez seca",
            "value": fmt_multiple(latest_balance["quick_ratio"], digits=2),
            "delta": fmt_multiple(latest_balance["quick_ratio"] - previous_balance["quick_ratio"], digits=2, signed=True),
            "delta_class": "positive" if latest_balance["quick_ratio"] >= previous_balance["quick_ratio"] else "negative",
            "copy": "Caixa e contas a receber sobre passivo circulante.",
        },
        {
            "label": "Divida bruta / PL",
            "value": fmt_percent(latest_balance["debt_to_equity"] * 100 if latest_balance["debt_to_equity"] is not None else None),
            "delta": fmt_pp(
                (latest_balance["debt_to_equity"] - previous_balance["debt_to_equity"]) * 100
                if latest_balance["debt_to_equity"] is not None and previous_balance["debt_to_equity"] is not None
                else None
            ),
            "delta_class": "positive" if latest_balance["debt_to_equity"] <= previous_balance["debt_to_equity"] else "negative",
            "copy": "Grau de alavancagem financeira em relacao ao patrimonio liquido.",
        },
        {
            "label": "Divida liquida / EBITDA",
            "value": fmt_multiple(latest_balance["net_debt_to_ebitda"], digits=2),
            "delta": fmt_multiple(
                latest_balance["net_debt_to_ebitda"] - previous_balance["net_debt_to_ebitda"],
                digits=2,
                signed=True,
            ),
            "delta_class": "positive"
            if latest_balance["net_debt_to_ebitda"] <= previous_balance["net_debt_to_ebitda"]
            else "negative",
            "copy": "Integra estrutura de capital com a geracao operacional da DRE.",
        },
        {
            "label": "Passivo total / ativo",
            "value": fmt_percent(latest_balance["liabilities_ratio"]),
            "delta": fmt_pp(latest_balance["liabilities_ratio"] - previous_balance["liabilities_ratio"]),
            "delta_class": "positive" if latest_balance["liabilities_ratio"] <= previous_balance["liabilities_ratio"] else "negative",
            "copy": "Percentual do ativo financiado por passivos de curto e longo prazo.",
        },
        {
            "label": "Patrimonio liquido / ativo",
            "value": fmt_percent(latest_balance["equity_ratio"]),
            "delta": fmt_pp(latest_balance["equity_ratio"] - previous_balance["equity_ratio"]),
            "delta_class": "positive" if latest_balance["equity_ratio"] >= previous_balance["equity_ratio"] else "negative",
            "copy": "Participacao do capital proprio na estrutura total de financiamento.",
        },
    ]

    balance_tracks = [
        {
            "label": "Ativo circulante / ativo",
            "value": fmt_percent((latest_balance["total_current_assets"] / latest_balance["total_assets"]) * 100),
            "width": (latest_balance["total_current_assets"] / latest_balance["total_assets"]) * 100,
        },
        {
            "label": "Ativo nao circulante / ativo",
            "value": fmt_percent((latest_balance["total_non_current_assets"] / latest_balance["total_assets"]) * 100),
            "width": (latest_balance["total_non_current_assets"] / latest_balance["total_assets"]) * 100,
        },
        {
            "label": "Passivo total / ativo",
            "value": fmt_percent(latest_balance["liabilities_ratio"]),
            "width": latest_balance["liabilities_ratio"],
        },
        {
            "label": "Patrimonio liquido / ativo",
            "value": fmt_percent(latest_balance["equity_ratio"]),
            "width": latest_balance["equity_ratio"],
        },
    ]

    balance_insight_cards = [
        {
            "kicker": "Liquidez",
            "title": "Folga financeira no curto prazo",
            "copy": f"O capital de giro fechou em {fmt_brl_compact(latest_balance['working_capital'])} e a liquidez corrente foi de {fmt_multiple(latest_balance['current_ratio'], digits=2)}, acima do ano anterior.",
        },
        {
            "kicker": "Estrutura de capital",
            "title": "Patrimonio ainda financia quase metade do ativo",
            "copy": f"O patrimonio liquido representa {fmt_percent(latest_balance['equity_ratio'])} do ativo total, enquanto o passivo total financia {fmt_percent(latest_balance['liabilities_ratio'])}.",
        },
        {
            "kicker": "Endividamento",
            "title": "Alavancagem permanece controlada",
            "copy": f"A divida liquida encerrou em {fmt_brl_compact(latest_balance['net_debt'])}, com relacao de {fmt_multiple(latest_balance['net_debt_to_ebitda'], digits=2)} sobre o EBITDA e divida bruta equivalente a {fmt_percent(latest_balance['debt_to_equity'] * 100)} do PL.",
        },
    ]

    revenue_by_year = {entry["year"]: entry["net_revenue"] for entry in series}
    ebitda_by_year = {entry["year"]: entry["ebitda"] for entry in series}
    financial_expense_by_year = {entry["year"]: entry["financial_expenses"] for entry in series}
    ending_cash_by_year = {entry["year"]: entry["cash"] for entry in balance_series}

    cash_flow_series = []
    for item in BUSINESS_CASH_FLOW_SERIES:
        operating_cash_flow = (
            item["net_income"]
            + item["depreciation_amortization"]
            + item["other_operational_adjustments"]
            - item["working_capital_variation"]
            - item["interest_and_taxes_paid"]
        )
        investing_cash_flow = item["capex"] + item["intangibles_investments"]
        financing_cash_flow = item["debt_raised"] - item["debt_repayment"] - item["dividends"]
        free_cash_flow = operating_cash_flow - investing_cash_flow
        net_change_in_cash = operating_cash_flow - investing_cash_flow + financing_cash_flow
        ending_cash = item["opening_cash"] + net_change_in_cash
        revenue = revenue_by_year.get(item["year"])
        ebitda_value = ebitda_by_year.get(item["year"])
        financial_expense = financial_expense_by_year.get(item["year"])

        cash_flow_series.append(
            {
                **item,
                "operating_cash_flow": operating_cash_flow,
                "investing_cash_flow": investing_cash_flow,
                "financing_cash_flow": abs(financing_cash_flow),
                "financing_cash_flow_signed": financing_cash_flow,
                "free_cash_flow": free_cash_flow,
                "net_change_in_cash": net_change_in_cash,
                "ending_cash": ending_cash,
                "cash_conversion_ebitda": operating_cash_flow / ebitda_value if ebitda_value else None,
                "cash_conversion_income": operating_cash_flow / item["net_income"] if item["net_income"] else None,
                "operating_cash_margin": (operating_cash_flow / revenue) * 100 if revenue else None,
                "free_cash_flow_margin": (free_cash_flow / revenue) * 100 if revenue else None,
                "capex_intensity": (item["capex"] + item["intangibles_investments"]) / revenue * 100 if revenue else None,
                "dividend_payout_cash": (item["dividends"] / operating_cash_flow) * 100 if operating_cash_flow else None,
                "cash_interest_coverage": operating_cash_flow / financial_expense if financial_expense else None,
            }
        )

    latest_cash_flow = cash_flow_series[-1]
    previous_cash_flow = cash_flow_series[-2]
    cash_flow_rows = []
    for definition in BUSINESS_CASH_FLOW_ROW_DEFINITIONS:
        cells = []
        for index, entry in enumerate(cash_flow_series):
            raw_value = entry.get(definition["key"])
            signed_value = -raw_value if definition["kind"] == "negative" else raw_value
            base_revenue = revenue_by_year.get(entry["year"])
            previous_raw_value = cash_flow_series[index - 1].get(definition["key"]) if index > 0 else None
            horizontal = ((raw_value / previous_raw_value) - 1) * 100 if previous_raw_value not in (None, 0) else None
            vertical = ((signed_value / base_revenue) * 100) if base_revenue else None
            cells.append(
                {
                    "value_display": fmt_brl_compact(signed_value),
                    "value_title": fmt_brl_full(signed_value),
                    "vertical_display": fmt_percent(vertical),
                    "horizontal_display": "Base" if index == 0 else fmt_percent(horizontal, signed=True),
                    "horizontal_class": (
                        "neutral"
                        if index == 0
                        else "positive"
                        if horizontal and horizontal > 0
                        else "negative"
                        if horizontal and horizontal < 0
                        else "neutral"
                    ),
                }
            )
        cash_flow_rows.append({"label": definition["label"], "row_class": definition["row_class"], "cells": cells})

    cash_flow_summary_cards = [
        {
            "kicker": "Fluxo operacional",
            "value": fmt_brl_compact(latest_cash_flow["operating_cash_flow"]),
            "chip": fmt_percent(((latest_cash_flow["operating_cash_flow"] / previous_cash_flow["operating_cash_flow"]) - 1) * 100, signed=True),
            "chip_class": "positive",
            "copy": f"A geracao operacional de caixa fechou {latest_cash_flow['year']} em {fmt_brl_compact(latest_cash_flow['operating_cash_flow'])}, equivalente a {fmt_percent(latest_cash_flow['operating_cash_margin'])} da receita liquida.",
        },
        {
            "kicker": "Fluxo de caixa livre",
            "value": fmt_brl_compact(latest_cash_flow["free_cash_flow"]),
            "chip": f"Margem {fmt_percent(latest_cash_flow['free_cash_flow_margin'])}",
            "chip_class": "positive",
            "copy": f"O caixa livre apos investimentos recorrentes ficou em {fmt_brl_compact(latest_cash_flow['free_cash_flow'])}, mantendo espaco para reduzir alavancagem e remunerar acionistas.",
        },
        {
            "kicker": "Caixa final",
            "value": fmt_brl_compact(latest_cash_flow["ending_cash"]),
            "chip": fmt_percent(((latest_cash_flow["ending_cash"] / previous_cash_flow["ending_cash"]) - 1) * 100, signed=True),
            "chip_class": "positive",
            "copy": f"O saldo final de caixa subiu para {fmt_brl_compact(latest_cash_flow['ending_cash'])}, em linha com o caixa do balanco patrimonial.",
        },
        {
            "kicker": "Conversao de EBITDA em caixa",
            "value": fmt_multiple(latest_cash_flow["cash_conversion_ebitda"], digits=2),
            "chip": fmt_multiple(latest_cash_flow["cash_conversion_income"], digits=2),
            "chip_class": "positive",
            "copy": f"O fluxo operacional representou {fmt_multiple(latest_cash_flow['cash_conversion_ebitda'], digits=2)} do EBITDA e {fmt_multiple(latest_cash_flow['cash_conversion_income'], digits=2)} do lucro liquido.",
        },
    ]

    cash_flow_ratio_cards = [
        {
            "label": "Caixa operacional / receita",
            "value": fmt_percent(latest_cash_flow["operating_cash_margin"]),
            "delta": fmt_pp(latest_cash_flow["operating_cash_margin"] - previous_cash_flow["operating_cash_margin"]),
            "delta_class": "positive" if latest_cash_flow["operating_cash_margin"] >= previous_cash_flow["operating_cash_margin"] else "negative",
            "copy": "Capacidade de transformar receita em caixa gerado pelas operacoes.",
        },
        {
            "label": "Fluxo livre / receita",
            "value": fmt_percent(latest_cash_flow["free_cash_flow_margin"]),
            "delta": fmt_pp(latest_cash_flow["free_cash_flow_margin"] - previous_cash_flow["free_cash_flow_margin"]),
            "delta_class": "positive" if latest_cash_flow["free_cash_flow_margin"] >= previous_cash_flow["free_cash_flow_margin"] else "negative",
            "copy": "Caixa disponivel apos os investimentos de manutencao e expansao.",
        },
        {
            "label": "Capex + intangivel / receita",
            "value": fmt_percent(latest_cash_flow["capex_intensity"]),
            "delta": fmt_pp(latest_cash_flow["capex_intensity"] - previous_cash_flow["capex_intensity"]),
            "delta_class": "positive" if latest_cash_flow["capex_intensity"] <= previous_cash_flow["capex_intensity"] else "negative",
            "copy": "Intensidade de investimento em ativos fisicos e tecnologia sobre a receita.",
        },
        {
            "label": "Dividendos / caixa operacional",
            "value": fmt_percent(latest_cash_flow["dividend_payout_cash"]),
            "delta": fmt_pp(latest_cash_flow["dividend_payout_cash"] - previous_cash_flow["dividend_payout_cash"]),
            "delta_class": "positive" if latest_cash_flow["dividend_payout_cash"] <= previous_cash_flow["dividend_payout_cash"] else "negative",
            "copy": "Percentual da geracao operacional comprometido com distribuicoes aos acionistas.",
        },
        {
            "label": "Cobertura de juros por caixa",
            "value": fmt_multiple(latest_cash_flow["cash_interest_coverage"], digits=2),
            "delta": fmt_multiple(latest_cash_flow["cash_interest_coverage"] - previous_cash_flow["cash_interest_coverage"], digits=2, signed=True),
            "delta_class": "positive" if latest_cash_flow["cash_interest_coverage"] >= previous_cash_flow["cash_interest_coverage"] else "negative",
            "copy": "Fluxo operacional em relacao ao gasto financeiro do mesmo periodo.",
        },
        {
            "label": "Financiamento liquido",
            "value": fmt_brl_compact(latest_cash_flow["financing_cash_flow_signed"]),
            "delta": fmt_percent(((latest_cash_flow["financing_cash_flow"] / previous_cash_flow["financing_cash_flow"]) - 1) * 100, signed=True),
            "delta_class": "positive" if latest_cash_flow["financing_cash_flow"] <= previous_cash_flow["financing_cash_flow"] else "negative",
            "copy": "Caixa consumido ou gerado pela estrutura de capital, apos captacoes, amortizacoes e dividendos.",
        },
    ]

    cash_flow_tracks = [
        {
            "label": "Fluxo operacional / receita",
            "value": fmt_percent(latest_cash_flow["operating_cash_margin"]),
            "width": latest_cash_flow["operating_cash_margin"],
        },
        {
            "label": "Investimentos / receita",
            "value": fmt_percent(latest_cash_flow["capex_intensity"]),
            "width": latest_cash_flow["capex_intensity"],
        },
        {
            "label": "Fluxo livre / receita",
            "value": fmt_percent(latest_cash_flow["free_cash_flow_margin"]),
            "width": latest_cash_flow["free_cash_flow_margin"],
        },
        {
            "label": "Caixa final / receita",
            "value": fmt_percent((latest_cash_flow["ending_cash"] / revenue_by_year.get(latest_cash_flow["year"])) * 100),
            "width": (latest_cash_flow["ending_cash"] / revenue_by_year.get(latest_cash_flow["year"])) * 100,
        },
    ]

    cash_flow_insight_cards = [
        {
            "kicker": "Geracao operacional",
            "title": "Caixa cresce acima do lucro",
            "copy": f"O fluxo operacional atingiu {fmt_brl_compact(latest_cash_flow['operating_cash_flow'])} em {latest_cash_flow['year']}, o equivalente a {fmt_multiple(latest_cash_flow['cash_conversion_income'], digits=2)} do lucro liquido e {fmt_multiple(latest_cash_flow['cash_conversion_ebitda'], digits=2)} do EBITDA.",
        },
        {
            "kicker": "Disciplina de investimento",
            "title": "Capex segue financiado pelo proprio caixa",
            "copy": f"A intensidade de investimento ficou em {fmt_percent(latest_cash_flow['capex_intensity'])} da receita, enquanto o fluxo de caixa livre permaneceu positivo em {fmt_brl_compact(latest_cash_flow['free_cash_flow'])}.",
        },
        {
            "kicker": "Estrutura financeira",
            "title": "Empresa reduz dependencia de financiamento",
            "copy": f"O fluxo de financiamento foi de {fmt_brl_compact(latest_cash_flow['financing_cash_flow_signed'])} em {latest_cash_flow['year']}, ainda assim o caixa final subiu para {fmt_brl_compact(ending_cash_by_year.get(latest_cash_flow['year']))}.",
        },
    ]

    opex_series = []
    for entry in series:
        sgna = entry["selling_expenses"] + entry["administrative_expenses"]
        total_opex = entry["operating_expenses"]
        net_opex = total_opex - entry["other_operating_income"]
        opex_series.append(
            {
                **entry,
                "sgna": sgna,
                "total_opex": total_opex,
                "net_opex": net_opex,
                "selling_ratio": (entry["selling_expenses"] / entry["net_revenue"]) * 100 if entry["net_revenue"] else None,
                "administrative_ratio": (entry["administrative_expenses"] / entry["net_revenue"]) * 100 if entry["net_revenue"] else None,
                "research_ratio": (entry["research_expenses"] / entry["net_revenue"]) * 100 if entry["net_revenue"] else None,
                "sgna_ratio": (sgna / entry["net_revenue"]) * 100 if entry["net_revenue"] else None,
                "net_opex_ratio": (net_opex / entry["net_revenue"]) * 100 if entry["net_revenue"] else None,
                "gross_profit_coverage": entry["gross_profit"] / total_opex if total_opex else None,
                "research_share_opex": (entry["research_expenses"] / total_opex) * 100 if total_opex else None,
            }
        )

    latest_opex = opex_series[-1]
    previous_opex = opex_series[-2]
    opex_rows = []
    for definition in BUSINESS_OPEX_ROW_DEFINITIONS:
        cells = []
        for index, entry in enumerate(opex_series):
            raw_value = entry.get(definition["key"])
            signed_value = -raw_value if definition["kind"] == "negative" else raw_value
            base_revenue = entry.get("net_revenue")
            previous_raw_value = opex_series[index - 1].get(definition["key"]) if index > 0 else None
            horizontal = ((raw_value / previous_raw_value) - 1) * 100 if previous_raw_value not in (None, 0) else None
            vertical = ((signed_value / base_revenue) * 100) if base_revenue else None
            cells.append(
                {
                    "value_display": fmt_brl_compact(signed_value),
                    "value_title": fmt_brl_full(signed_value),
                    "vertical_display": fmt_percent(vertical),
                    "horizontal_display": "Base" if index == 0 else fmt_percent(horizontal, signed=True),
                    "horizontal_class": (
                        "neutral"
                        if index == 0
                        else "positive"
                        if horizontal and horizontal > 0
                        else "negative"
                        if horizontal and horizontal < 0
                        else "neutral"
                    ),
                }
            )
        opex_rows.append({"label": definition["label"], "row_class": definition["row_class"], "cells": cells})

    opex_summary_cards = [
        {
            "kicker": "Opex total",
            "value": fmt_brl_compact(latest_opex["total_opex"]),
            "chip": fmt_percent(((latest_opex["total_opex"] / previous_opex["total_opex"]) - 1) * 100, signed=True),
            "chip_class": "positive" if latest_opex["opex_ratio"] <= previous_opex["opex_ratio"] else "negative",
            "copy": f"As despesas operacionais somaram {fmt_brl_compact(latest_opex['total_opex'])} em {latest_opex['year']}, equivalentes a {fmt_percent(latest_opex['opex_ratio'])} da receita liquida.",
        },
        {
            "kicker": "Opex liquido",
            "value": fmt_brl_compact(latest_opex["net_opex"]),
            "chip": f"Margem {fmt_percent(latest_opex['net_opex_ratio'])}",
            "chip_class": "positive",
            "copy": f"Depois de outras receitas operacionais, o consumo liquido de despesas ficou em {fmt_brl_compact(latest_opex['net_opex'])}.",
        },
        {
            "kicker": "SG&A",
            "value": fmt_brl_compact(latest_opex["sgna"]),
            "chip": fmt_percent(latest_opex["sgna_ratio"]),
            "chip_class": "positive",
            "copy": f"As despesas comerciais e administrativas fecharam em {fmt_percent(latest_opex['sgna_ratio'])} da receita liquida, com melhora frente a {previous_opex['year']}.",
        },
        {
            "kicker": "Cobertura do lucro bruto",
            "value": fmt_multiple(latest_opex["gross_profit_coverage"], digits=2),
            "chip": fmt_percent(latest_opex["research_share_opex"]),
            "chip_class": "positive",
            "copy": f"O lucro bruto cobre {fmt_multiple(latest_opex['gross_profit_coverage'], digits=2)} do opex, enquanto P&D representa {fmt_percent(latest_opex['research_share_opex'])} das despesas operacionais.",
        },
    ]

    opex_ratio_cards = [
        {
            "label": "Comercial / receita",
            "value": fmt_percent(latest_opex["selling_ratio"]),
            "delta": fmt_pp(latest_opex["selling_ratio"] - previous_opex["selling_ratio"]),
            "delta_class": "positive" if latest_opex["selling_ratio"] <= previous_opex["selling_ratio"] else "negative",
            "copy": "Peso da frente comercial sobre a receita liquida.",
        },
        {
            "label": "Administrativo / receita",
            "value": fmt_percent(latest_opex["administrative_ratio"]),
            "delta": fmt_pp(latest_opex["administrative_ratio"] - previous_opex["administrative_ratio"]),
            "delta_class": "positive" if latest_opex["administrative_ratio"] <= previous_opex["administrative_ratio"] else "negative",
            "copy": "Carga administrativa em relacao ao volume de vendas.",
        },
        {
            "label": "P&D / receita",
            "value": fmt_percent(latest_opex["research_ratio"]),
            "delta": fmt_pp(latest_opex["research_ratio"] - previous_opex["research_ratio"]),
            "delta_class": "positive" if latest_opex["research_ratio"] <= previous_opex["research_ratio"] else "negative",
            "copy": "Intensidade de investimento operacional em inovacao.",
        },
        {
            "label": "SG&A / receita",
            "value": fmt_percent(latest_opex["sgna_ratio"]),
            "delta": fmt_pp(latest_opex["sgna_ratio"] - previous_opex["sgna_ratio"]),
            "delta_class": "positive" if latest_opex["sgna_ratio"] <= previous_opex["sgna_ratio"] else "negative",
            "copy": "Soma das despesas comerciais e administrativas sobre a receita.",
        },
        {
            "label": "Opex liquido / receita",
            "value": fmt_percent(latest_opex["net_opex_ratio"]),
            "delta": fmt_pp(latest_opex["net_opex_ratio"] - previous_opex["net_opex_ratio"]),
            "delta_class": "positive" if latest_opex["net_opex_ratio"] <= previous_opex["net_opex_ratio"] else "negative",
            "copy": "Consumo liquido de despesas depois de outras receitas operacionais.",
        },
        {
            "label": "Lucro bruto / opex",
            "value": fmt_multiple(latest_opex["gross_profit_coverage"], digits=2),
            "delta": fmt_multiple(latest_opex["gross_profit_coverage"] - previous_opex["gross_profit_coverage"], digits=2, signed=True),
            "delta_class": "positive" if latest_opex["gross_profit_coverage"] >= previous_opex["gross_profit_coverage"] else "negative",
            "copy": "Capacidade do lucro bruto absorver o bloco de despesas operacionais.",
        },
    ]

    opex_tracks = [
        {"label": "Comercial / opex", "value": fmt_percent((latest_opex["selling_expenses"] / latest_opex["total_opex"]) * 100), "width": (latest_opex["selling_expenses"] / latest_opex["total_opex"]) * 100},
        {"label": "Administrativo / opex", "value": fmt_percent((latest_opex["administrative_expenses"] / latest_opex["total_opex"]) * 100), "width": (latest_opex["administrative_expenses"] / latest_opex["total_opex"]) * 100},
        {"label": "P&D / opex", "value": fmt_percent((latest_opex["research_expenses"] / latest_opex["total_opex"]) * 100), "width": (latest_opex["research_expenses"] / latest_opex["total_opex"]) * 100},
        {"label": "Opex total / receita", "value": fmt_percent(latest_opex["opex_ratio"]), "width": latest_opex["opex_ratio"]},
    ]

    opex_insight_cards = [
        {
            "kicker": "Diluição operacional",
            "title": "Receita cresce mais que as despesas",
            "copy": f"O opex total subiu {fmt_percent(((latest_opex['total_opex'] / previous_opex['total_opex']) - 1) * 100, signed=True)} em {latest_opex['year']}, abaixo do crescimento da receita liquida, que foi de {fmt_percent(((latest_opex['net_revenue'] / previous_opex['net_revenue']) - 1) * 100, signed=True)}.",
        },
        {
            "kicker": "Produtividade comercial",
            "title": "Despesas comerciais ganham eficiencia",
            "copy": f"As despesas comerciais fecharam em {fmt_percent(latest_opex['selling_ratio'])} da receita, contra {fmt_percent(previous_opex['selling_ratio'])} no ano anterior, abrindo espaco para margem EBITDA.",
        },
        {
            "kicker": "Base fixa",
            "title": "SG&A segue disciplinado",
            "copy": f"O SG&A encerrou em {fmt_percent(latest_opex['sgna_ratio'])} da receita e o lucro bruto cobre {fmt_multiple(latest_opex['gross_profit_coverage'], digits=2)} do bloco de opex.",
        },
    ]

    cash_flow_by_year = {entry["year"]: entry for entry in cash_flow_series}
    capex_series = []
    for item in BUSINESS_CAPEX_SERIES:
        total_physical_capex = item["maintenance_capex"] + item["expansion_capex"] + item["technology_capex"]
        total_investments = total_physical_capex + item["intangibles_investments"]
        revenue = revenue_by_year.get(item["year"])
        depreciation_value = next((entry["depreciation_amortization"] for entry in series if entry["year"] == item["year"]), None)
        free_cash_after_investments = cash_flow_by_year[item["year"]]["free_cash_flow"] if item["year"] in cash_flow_by_year else None
        capex_series.append(
            {
                **item,
                "total_physical_capex": total_physical_capex,
                "total_investments": total_investments,
                "depreciation_amortization": depreciation_value,
                "free_cash_after_investments": free_cash_after_investments,
                "capex_intensity": (total_investments / revenue) * 100 if revenue else None,
                "maintenance_ratio": (item["maintenance_capex"] / revenue) * 100 if revenue else None,
                "expansion_ratio": (item["expansion_capex"] / revenue) * 100 if revenue else None,
                "technology_ratio": (item["technology_capex"] / revenue) * 100 if revenue else None,
                "reinvestment_multiple": total_investments / depreciation_value if depreciation_value else None,
                "expansion_share": (item["expansion_capex"] / total_investments) * 100 if total_investments else None,
                "maintenance_share": (item["maintenance_capex"] / total_investments) * 100 if total_investments else None,
                "technology_share": (item["technology_capex"] / total_investments) * 100 if total_investments else None,
                "intangibles_share": (item["intangibles_investments"] / total_investments) * 100 if total_investments else None,
            }
        )

    latest_capex = capex_series[-1]
    previous_capex = capex_series[-2]
    capex_rows = []
    for definition in BUSINESS_CAPEX_ROW_DEFINITIONS:
        cells = []
        for index, entry in enumerate(capex_series):
            raw_value = entry.get(definition["key"])
            signed_value = -raw_value if definition["kind"] == "negative" else raw_value
            base_revenue = revenue_by_year.get(entry["year"])
            previous_raw_value = capex_series[index - 1].get(definition["key"]) if index > 0 else None
            horizontal = ((raw_value / previous_raw_value) - 1) * 100 if previous_raw_value not in (None, 0) else None
            vertical = ((signed_value / base_revenue) * 100) if base_revenue else None
            cells.append(
                {
                    "value_display": fmt_brl_compact(signed_value),
                    "value_title": fmt_brl_full(signed_value),
                    "vertical_display": fmt_percent(vertical),
                    "horizontal_display": "Base" if index == 0 else fmt_percent(horizontal, signed=True),
                    "horizontal_class": (
                        "neutral"
                        if index == 0
                        else "positive"
                        if horizontal and horizontal > 0
                        else "negative"
                        if horizontal and horizontal < 0
                        else "neutral"
                    ),
                }
            )
        capex_rows.append({"label": definition["label"], "row_class": definition["row_class"], "cells": cells})

    capex_summary_cards = [
        {
            "kicker": "Investimento total",
            "value": fmt_brl_compact(latest_capex["total_investments"]),
            "chip": fmt_percent(((latest_capex["total_investments"] / previous_capex["total_investments"]) - 1) * 100, signed=True),
            "chip_class": "negative" if latest_capex["capex_intensity"] > previous_capex["capex_intensity"] else "positive",
            "copy": f"O capex total ficou em {fmt_brl_compact(latest_capex['total_investments'])} em {latest_capex['year']}, com intensidade de {fmt_percent(latest_capex['capex_intensity'])} da receita liquida.",
        },
        {
            "kicker": "Capex fisico",
            "value": fmt_brl_compact(latest_capex["total_physical_capex"]),
            "chip": fmt_percent(latest_capex["expansion_share"]),
            "chip_class": "positive",
            "copy": f"A frente de expansao responde por {fmt_percent(latest_capex['expansion_share'])} do investimento total do periodo.",
        },
        {
            "kicker": "Reinvestimento",
            "value": fmt_multiple(latest_capex["reinvestment_multiple"], digits=2),
            "chip": fmt_brl_compact(latest_capex["free_cash_after_investments"]),
            "chip_class": "positive",
            "copy": f"O investimento total representa {fmt_multiple(latest_capex['reinvestment_multiple'], digits=2)} da depreciacao e amortizacao, com caixa livre ainda positivo apos os investimentos.",
        },
        {
            "kicker": "Intangivel e software",
            "value": fmt_brl_compact(latest_capex["intangibles_investments"]),
            "chip": fmt_percent(latest_capex["intangibles_share"]),
            "chip_class": "positive",
            "copy": f"Os investimentos em tecnologia e intangivel somaram {fmt_percent(latest_capex['intangibles_share'])} do pacote total de capex.",
        },
    ]

    capex_ratio_cards = [
        {
            "label": "Capex manutencao / receita",
            "value": fmt_percent(latest_capex["maintenance_ratio"]),
            "delta": fmt_pp(latest_capex["maintenance_ratio"] - previous_capex["maintenance_ratio"]),
            "delta_class": "positive" if latest_capex["maintenance_ratio"] <= previous_capex["maintenance_ratio"] else "negative",
            "copy": "Parte do investimento destinada a manter a base operacional.",
        },
        {
            "label": "Capex expansao / receita",
            "value": fmt_percent(latest_capex["expansion_ratio"]),
            "delta": fmt_pp(latest_capex["expansion_ratio"] - previous_capex["expansion_ratio"]),
            "delta_class": "negative" if latest_capex["expansion_ratio"] > previous_capex["expansion_ratio"] else "positive",
            "copy": "Parcela da receita reinvestida em crescimento de capacidade.",
        },
        {
            "label": "Capex tecnologia / receita",
            "value": fmt_percent(latest_capex["technology_ratio"]),
            "delta": fmt_pp(latest_capex["technology_ratio"] - previous_capex["technology_ratio"]),
            "delta_class": "negative" if latest_capex["technology_ratio"] > previous_capex["technology_ratio"] else "positive",
            "copy": "Investimento em automacao, digitalizacao e sistemas.",
        },
        {
            "label": "Investimento total / receita",
            "value": fmt_percent(latest_capex["capex_intensity"]),
            "delta": fmt_pp(latest_capex["capex_intensity"] - previous_capex["capex_intensity"]),
            "delta_class": "positive" if latest_capex["capex_intensity"] <= previous_capex["capex_intensity"] else "negative",
            "copy": "Intensidade total do investimento em relacao ao volume de receita.",
        },
        {
            "label": "Investimento / D&A",
            "value": fmt_multiple(latest_capex["reinvestment_multiple"], digits=2),
            "delta": fmt_multiple(latest_capex["reinvestment_multiple"] - previous_capex["reinvestment_multiple"], digits=2, signed=True),
            "delta_class": "negative" if latest_capex["reinvestment_multiple"] > previous_capex["reinvestment_multiple"] else "positive",
            "copy": "Relação entre investimento total e depreciacao/amortizacao do periodo.",
        },
        {
            "label": "Caixa livre apos capex",
            "value": fmt_brl_compact(latest_capex["free_cash_after_investments"]),
            "delta": fmt_percent(((latest_capex["free_cash_after_investments"] / previous_capex["free_cash_after_investments"]) - 1) * 100, signed=True),
            "delta_class": "positive" if latest_capex["free_cash_after_investments"] >= previous_capex["free_cash_after_investments"] else "negative",
            "copy": "Caixa residual depois de sustentar todo o ciclo de investimento do ano.",
        },
    ]

    capex_tracks = [
        {"label": "Manutencao / investimento total", "value": fmt_percent(latest_capex["maintenance_share"]), "width": latest_capex["maintenance_share"]},
        {"label": "Expansao / investimento total", "value": fmt_percent(latest_capex["expansion_share"]), "width": latest_capex["expansion_share"]},
        {"label": "Tecnologia / investimento total", "value": fmt_percent(latest_capex["technology_share"]), "width": latest_capex["technology_share"]},
        {"label": "Intangivel / investimento total", "value": fmt_percent(latest_capex["intangibles_share"]), "width": latest_capex["intangibles_share"]},
    ]

    capex_insight_cards = [
        {
            "kicker": "Portifolio de investimento",
            "title": "Capex avanca com foco em expansao",
            "copy": f"A parcela de expansao atingiu {fmt_percent(latest_capex['expansion_share'])} do investimento total em {latest_capex['year']}, acima da manutencao em {fmt_percent(latest_capex['maintenance_share'])}.",
        },
        {
            "kicker": "Cobertura por caixa",
            "title": "Investimento segue financiado pela operacao",
            "copy": f"O caixa livre apos investimentos ficou em {fmt_brl_compact(latest_capex['free_cash_after_investments'])}, mesmo com intensidade de capex total em {fmt_percent(latest_capex['capex_intensity'])} da receita.",
        },
        {
            "kicker": "Reinvestimento",
            "title": "Empresa reinveste acima da depreciacao",
            "copy": f"O investimento total representa {fmt_multiple(latest_capex['reinvestment_multiple'], digits=2)} da depreciacao e amortizacao, indicando reforco de capacidade e modernizacao da base operacional.",
        },
    ]

    balance_by_year = {entry["year"]: entry for entry in balance_series}
    opex_by_year = {entry["year"]: entry for entry in opex_series}
    capex_by_year = {entry["year"]: entry for entry in capex_series}
    risk_free_rate = 11.0
    market_premium = 6.0
    beta = 1.05

    indicator_series = []
    for index, entry in enumerate(series):
        year = entry["year"]
        balance_entry = balance_by_year[year]
        cash_entry = cash_flow_by_year[year]
        opex_entry = opex_by_year[year]
        capex_entry = capex_by_year[year]
        previous_year = years[index - 1] if index > 0 else None
        previous_balance_entry = balance_by_year[previous_year] if previous_year else balance_entry
        previous_invested_capital = previous_balance_entry["total_equity"] + previous_balance_entry["gross_debt"] - previous_balance_entry["cash"]
        invested_capital = balance_entry["total_equity"] + balance_entry["gross_debt"] - balance_entry["cash"]
        avg_assets = (balance_entry["total_assets"] + previous_balance_entry["total_assets"]) / 2 if previous_year else balance_entry["total_assets"]
        avg_equity = (balance_entry["total_equity"] + previous_balance_entry["total_equity"]) / 2 if previous_year else balance_entry["total_equity"]
        avg_invested_capital = (invested_capital + previous_invested_capital) / 2 if previous_year else invested_capital
        tax_rate_decimal = (entry["tax_rate"] or 0) / 100
        nopat = entry["ebit"] * (1 - tax_rate_decimal)
        cost_equity = risk_free_rate + beta * market_premium
        gross_debt = balance_entry["gross_debt"]
        debt_cost_pre_tax = (entry["financial_expenses"] / gross_debt) * 100 if gross_debt else None
        capital_base = balance_entry["total_equity"] + gross_debt
        equity_weight = (balance_entry["total_equity"] / capital_base) if capital_base else None
        debt_weight = (gross_debt / capital_base) if capital_base else None
        wacc = (
            (equity_weight * cost_equity) + (debt_weight * debt_cost_pre_tax * (1 - tax_rate_decimal))
            if equity_weight is not None and debt_weight is not None and debt_cost_pre_tax is not None
            else None
        )
        eva = nopat - (avg_invested_capital * wacc / 100) if wacc is not None else None
        variable_selling_expenses = entry["selling_expenses"] * 0.45
        contribution_amount = entry["net_revenue"] - entry["cost_of_sales"] - variable_selling_expenses
        contribution_margin = (contribution_amount / entry["net_revenue"]) * 100 if entry["net_revenue"] else None
        fixed_cost_base = (
            (entry["selling_expenses"] - variable_selling_expenses)
            + entry["administrative_expenses"]
            + entry["research_expenses"]
            + entry["depreciation_amortization"]
            - entry["other_operating_income"]
        )
        contribution_ratio_decimal = contribution_amount / entry["net_revenue"] if entry["net_revenue"] else None
        break_even_revenue = fixed_cost_base / contribution_ratio_decimal if contribution_ratio_decimal else None
        interest_coverage = entry["ebit"] / entry["financial_expenses"] if entry["financial_expenses"] else None
        pmr = (balance_entry["accounts_receivable"] / entry["net_revenue"]) * 365 if entry["net_revenue"] else None
        pme = (balance_entry["inventory"] / entry["cost_of_sales"]) * 365 if entry["cost_of_sales"] else None
        pmp = (balance_entry["suppliers"] / entry["cost_of_sales"]) * 365 if entry["cost_of_sales"] else None
        financial_cycle = pmr + pme - pmp if pmr is not None and pme is not None and pmp is not None else None
        working_capital_need = balance_entry["accounts_receivable"] + balance_entry["inventory"] - balance_entry["suppliers"]
        short_term_debt_ratio = (balance_entry["short_term_debt"] / gross_debt) * 100 if gross_debt else None
        interest_bearing_liabilities_assets = (gross_debt / balance_entry["total_assets"]) * 100 if balance_entry["total_assets"] else None
        debt_service = cash_entry["debt_repayment"] + entry["financial_expenses"]
        dscr = cash_entry["operating_cash_flow"] / debt_service if debt_service else None
        fcf_yield_ebitda = (cash_entry["free_cash_flow"] / entry["ebitda"]) * 100 if entry["ebitda"] else None
        asset_productivity = entry["net_revenue"] / avg_assets if avg_assets else None
        invested_capital_turnover = entry["net_revenue"] / avg_invested_capital if avg_invested_capital else None
        roic = (nopat / avg_invested_capital) * 100 if avg_invested_capital else None
        roic_spread = roic - wacc if roic is not None and wacc is not None else None
        debt_to_equity_pct = (balance_entry["debt_to_equity"] * 100) if balance_entry["debt_to_equity"] is not None else None
        operating_leverage = contribution_amount / entry["ebit"] if entry["ebit"] else None
        health_score = sum(
            [
                max(0.0, min((entry["ebitda_margin"] / 25) * 20, 20)),
                max(0.0, min((balance_entry["current_ratio"] / 2.0) * 20, 20)),
                max(0.0, min(((3.0 - balance_entry["net_debt_to_ebitda"]) / 3.0) * 20, 20)) if balance_entry["net_debt_to_ebitda"] is not None else 0,
                max(0.0, min((cash_entry["free_cash_flow_margin"] / 10) * 20, 20)) if cash_entry["free_cash_flow_margin"] is not None else 0,
                max(0.0, min(((roic_spread + 5) / 10) * 20, 20)) if roic_spread is not None else 0,
            ]
        )

        indicator_series.append(
            {
                "year": year,
                "roe": (entry["net_income"] / avg_equity) * 100 if avg_equity else None,
                "roa": (entry["net_income"] / avg_assets) * 100 if avg_assets else None,
                "roic": roic,
                "wacc": wacc,
                "roic_spread": roic_spread,
                "eva": eva,
                "contribution_margin": contribution_margin,
                "interest_coverage": interest_coverage,
                "cash_conversion_ebitda_pct": cash_entry["cash_conversion_ebitda"] * 100 if cash_entry["cash_conversion_ebitda"] is not None else None,
                "fcf_yield_revenue": cash_entry["free_cash_flow_margin"],
                "fcf_yield_ebitda": fcf_yield_ebitda,
                "pmr": pmr,
                "pmp": pmp,
                "pme": pme,
                "financial_cycle": financial_cycle,
                "working_capital_need": working_capital_need,
                "net_debt_to_ebitda": balance_entry["net_debt_to_ebitda"],
                "debt_to_equity_pct": debt_to_equity_pct,
                "short_term_debt_ratio": short_term_debt_ratio,
                "interest_bearing_liabilities_assets": interest_bearing_liabilities_assets,
                "dscr": dscr,
                "opex_ratio": opex_entry["opex_ratio"],
                "sgna_ratio": opex_entry["sgna_ratio"],
                "research_ratio": opex_entry["research_ratio"],
                "asset_productivity": asset_productivity,
                "invested_capital_turnover": invested_capital_turnover,
                "maintenance_ratio": capex_entry["maintenance_ratio"],
                "expansion_ratio": capex_entry["expansion_ratio"],
                "reinvestment_multiple": capex_entry["reinvestment_multiple"],
                "capex_intensity": capex_entry["capex_intensity"],
                "operating_leverage": operating_leverage,
                "break_even_revenue": break_even_revenue,
                "health_score": health_score,
            }
        )

    latest_indicators = indicator_series[-1]
    previous_indicators = indicator_series[-2]
    indicator_rows = []
    for definition in BUSINESS_INDICATOR_ROW_DEFINITIONS:
        cells = []
        for index, entry in enumerate(indicator_series):
            raw_value = entry.get(definition["key"])
            previous_raw_value = indicator_series[index - 1].get(definition["key"]) if index > 0 else None
            horizontal = ((raw_value / previous_raw_value) - 1) * 100 if previous_raw_value not in (None, 0) else None
            cells.append(
                {
                    "value_display": format_business_indicator_value(definition["unit"], raw_value),
                    "horizontal_display": "Base" if index == 0 else fmt_percent(horizontal, signed=True),
                    "horizontal_class": (
                        "neutral"
                        if index == 0
                        else "positive"
                        if horizontal and horizontal > 0
                        else "negative"
                        if horizontal and horizontal < 0
                        else "neutral"
                    ),
                }
            )
        indicator_rows.append({"label": definition["label"], "row_class": definition["row_class"], "cells": cells})

    indicator_summary_cards = [
        {
            "kicker": "ROIC",
            "value": fmt_percent(latest_indicators["roic"]),
            "chip": fmt_pp(latest_indicators["roic_spread"]),
            "chip_class": "positive" if latest_indicators["roic_spread"] and latest_indicators["roic_spread"] > 0 else "negative",
            "copy": f"O retorno sobre o capital investido ficou em {fmt_percent(latest_indicators['roic'])}, contra um WACC estimado de {fmt_percent(latest_indicators['wacc'])}.",
        },
        {
            "kicker": "ROE",
            "value": fmt_percent(latest_indicators["roe"]),
            "chip": fmt_pp(latest_indicators["roe"] - previous_indicators["roe"]),
            "chip_class": "positive",
            "copy": f"O retorno ao acionista encerrou {latest['year']} em {fmt_percent(latest_indicators['roe'])}, sustentado por maior lucro liquido e reforco do patrimonio.",
        },
        {
            "kicker": "EVA",
            "value": fmt_brl_compact(latest_indicators["eva"]),
            "chip": fmt_percent(((latest_indicators["eva"] / previous_indicators["eva"]) - 1) * 100, signed=True),
            "chip_class": "positive" if latest_indicators["eva"] and latest_indicators["eva"] > 0 else "negative",
            "copy": f"O lucro economico estimado ficou em {fmt_brl_compact(latest_indicators['eva'])}, capturando o spread positivo entre retorno e custo de capital.",
        },
        {
            "kicker": "Saude financeira",
            "value": fmt_score(latest_indicators["health_score"]),
            "chip": fmt_points(latest_indicators["health_score"] - previous_indicators["health_score"]),
            "chip_class": "positive",
            "copy": "Score sintético que combina margem EBITDA, liquidez, alavancagem, caixa livre e spread ROIC-WACC.",
        },
    ]

    profitability_indicator_cards = [
        {
            "label": "ROE",
            "value": fmt_percent(latest_indicators["roe"]),
            "delta": fmt_pp(latest_indicators["roe"] - previous_indicators["roe"]),
            "delta_class": "positive" if latest_indicators["roe"] >= previous_indicators["roe"] else "negative",
            "copy": "Retorno do lucro liquido sobre o patrimonio liquido medio.",
        },
        {
            "label": "ROA",
            "value": fmt_percent(latest_indicators["roa"]),
            "delta": fmt_pp(latest_indicators["roa"] - previous_indicators["roa"]),
            "delta_class": "positive" if latest_indicators["roa"] >= previous_indicators["roa"] else "negative",
            "copy": "Retorno do lucro liquido sobre a base media de ativos.",
        },
        {
            "label": "ROIC",
            "value": fmt_percent(latest_indicators["roic"]),
            "delta": fmt_pp(latest_indicators["roic"] - previous_indicators["roic"]),
            "delta_class": "positive" if latest_indicators["roic"] >= previous_indicators["roic"] else "negative",
            "copy": "Retorno operacional apos impostos sobre o capital investido medio.",
        },
        {
            "label": "WACC",
            "value": fmt_percent(latest_indicators["wacc"]),
            "delta": fmt_pp(latest_indicators["wacc"] - previous_indicators["wacc"]),
            "delta_class": "positive" if latest_indicators["wacc"] <= previous_indicators["wacc"] else "negative",
            "copy": "Custo medio ponderado de capital estimado a partir da estrutura de financiamento atual.",
        },
        {
            "label": "Spread ROIC - WACC",
            "value": fmt_pp(latest_indicators["roic_spread"]),
            "delta": fmt_pp(latest_indicators["roic_spread"] - previous_indicators["roic_spread"]),
            "delta_class": "positive" if latest_indicators["roic_spread"] >= previous_indicators["roic_spread"] else "negative",
            "copy": "Diferenca entre retorno operacional e custo de capital.",
        },
        {
            "label": "EVA",
            "value": fmt_brl_compact(latest_indicators["eva"]),
            "delta": fmt_percent(((latest_indicators["eva"] / previous_indicators["eva"]) - 1) * 100, signed=True),
            "delta_class": "positive" if latest_indicators["eva"] >= previous_indicators["eva"] else "negative",
            "copy": "Valor economico agregado apos remunerar o capital investido.",
        },
    ]

    working_capital_indicator_cards = [
        {
            "label": "PMR",
            "value": fmt_days(latest_indicators["pmr"]),
            "delta": fmt_signed_days(latest_indicators["pmr"] - previous_indicators["pmr"]),
            "delta_class": "positive" if latest_indicators["pmr"] <= previous_indicators["pmr"] else "negative",
            "copy": "Prazo medio de recebimento dos clientes.",
        },
        {
            "label": "PME",
            "value": fmt_days(latest_indicators["pme"]),
            "delta": fmt_signed_days(latest_indicators["pme"] - previous_indicators["pme"]),
            "delta_class": "positive" if latest_indicators["pme"] <= previous_indicators["pme"] else "negative",
            "copy": "Prazo medio de permanencia dos estoques.",
        },
        {
            "label": "PMP",
            "value": fmt_days(latest_indicators["pmp"]),
            "delta": fmt_signed_days(latest_indicators["pmp"] - previous_indicators["pmp"]),
            "delta_class": "positive" if latest_indicators["pmp"] >= previous_indicators["pmp"] else "negative",
            "copy": "Prazo medio de pagamento a fornecedores.",
        },
        {
            "label": "Ciclo financeiro",
            "value": fmt_days(latest_indicators["financial_cycle"]),
            "delta": fmt_signed_days(latest_indicators["financial_cycle"] - previous_indicators["financial_cycle"]),
            "delta_class": "positive" if latest_indicators["financial_cycle"] <= previous_indicators["financial_cycle"] else "negative",
            "copy": "Tempo liquido que o caixa fica preso no capital de giro.",
        },
        {
            "label": "Necessidade de capital de giro",
            "value": fmt_brl_compact(latest_indicators["working_capital_need"]),
            "delta": fmt_percent(((latest_indicators["working_capital_need"] / previous_indicators["working_capital_need"]) - 1) * 100, signed=True),
            "delta_class": "positive" if latest_indicators["working_capital_need"] <= previous_indicators["working_capital_need"] else "negative",
            "copy": "Clientes mais estoques menos fornecedores.",
        },
        {
            "label": "Produtividade do ativo",
            "value": fmt_multiple(latest_indicators["asset_productivity"], digits=2),
            "delta": fmt_multiple(latest_indicators["asset_productivity"] - previous_indicators["asset_productivity"], digits=2, signed=True),
            "delta_class": "positive" if latest_indicators["asset_productivity"] >= previous_indicators["asset_productivity"] else "negative",
            "copy": "Receita gerada por unidade de ativo medio.",
        },
    ]

    leverage_indicator_cards = [
        {
            "label": "Divida liquida / EBITDA",
            "value": fmt_multiple(latest_indicators["net_debt_to_ebitda"], digits=2),
            "delta": fmt_multiple(latest_indicators["net_debt_to_ebitda"] - previous_indicators["net_debt_to_ebitda"], digits=2, signed=True),
            "delta_class": "positive" if latest_indicators["net_debt_to_ebitda"] <= previous_indicators["net_debt_to_ebitda"] else "negative",
            "copy": "Alavancagem financeira liquida sobre a geracao operacional.",
        },
        {
            "label": "Divida bruta / PL",
            "value": fmt_percent(latest_indicators["debt_to_equity_pct"]),
            "delta": fmt_pp(latest_indicators["debt_to_equity_pct"] - previous_indicators["debt_to_equity_pct"]),
            "delta_class": "positive" if latest_indicators["debt_to_equity_pct"] <= previous_indicators["debt_to_equity_pct"] else "negative",
            "copy": "Grau de endividamento em relacao ao capital proprio.",
        },
        {
            "label": "Divida CP / divida total",
            "value": fmt_percent(latest_indicators["short_term_debt_ratio"]),
            "delta": fmt_pp(latest_indicators["short_term_debt_ratio"] - previous_indicators["short_term_debt_ratio"]),
            "delta_class": "positive" if latest_indicators["short_term_debt_ratio"] <= previous_indicators["short_term_debt_ratio"] else "negative",
            "copy": "Parcela do endividamento concentrada no curto prazo.",
        },
        {
            "label": "Passivo oneroso / ativo",
            "value": fmt_percent(latest_indicators["interest_bearing_liabilities_assets"]),
            "delta": fmt_pp(latest_indicators["interest_bearing_liabilities_assets"] - previous_indicators["interest_bearing_liabilities_assets"]),
            "delta_class": "positive" if latest_indicators["interest_bearing_liabilities_assets"] <= previous_indicators["interest_bearing_liabilities_assets"] else "negative",
            "copy": "Peso das obrigacoes financeiras na estrutura de ativos.",
        },
        {
            "label": "Cobertura de juros",
            "value": fmt_multiple(latest_indicators["interest_coverage"], digits=2),
            "delta": fmt_multiple(latest_indicators["interest_coverage"] - previous_indicators["interest_coverage"], digits=2, signed=True),
            "delta_class": "positive" if latest_indicators["interest_coverage"] >= previous_indicators["interest_coverage"] else "negative",
            "copy": "EBIT dividido pela despesa financeira do periodo.",
        },
        {
            "label": "DSCR",
            "value": fmt_multiple(latest_indicators["dscr"], digits=2),
            "delta": fmt_multiple(latest_indicators["dscr"] - previous_indicators["dscr"], digits=2, signed=True),
            "delta_class": "positive" if latest_indicators["dscr"] >= previous_indicators["dscr"] else "negative",
            "copy": "Capacidade de pagar servico da divida com o caixa operacional.",
        },
    ]

    efficiency_indicator_cards = [
        {
            "label": "Margem de contribuicao",
            "value": fmt_percent(latest_indicators["contribution_margin"]),
            "delta": fmt_pp(latest_indicators["contribution_margin"] - previous_indicators["contribution_margin"]),
            "delta_class": "positive" if latest_indicators["contribution_margin"] >= previous_indicators["contribution_margin"] else "negative",
            "copy": "Margem apos CPV e componente variavel comercial.",
        },
        {
            "label": "Caixa / EBITDA",
            "value": fmt_percent(latest_indicators["cash_conversion_ebitda_pct"]),
            "delta": fmt_pp(latest_indicators["cash_conversion_ebitda_pct"] - previous_indicators["cash_conversion_ebitda_pct"]),
            "delta_class": "positive" if latest_indicators["cash_conversion_ebitda_pct"] >= previous_indicators["cash_conversion_ebitda_pct"] else "negative",
            "copy": "Conversao do EBITDA em fluxo operacional de caixa.",
        },
        {
            "label": "FCF yield / receita",
            "value": fmt_percent(latest_indicators["fcf_yield_revenue"]),
            "delta": fmt_pp(latest_indicators["fcf_yield_revenue"] - previous_indicators["fcf_yield_revenue"]),
            "delta_class": "positive" if latest_indicators["fcf_yield_revenue"] >= previous_indicators["fcf_yield_revenue"] else "negative",
            "copy": "Caixa livre gerado sobre a receita liquida.",
        },
        {
            "label": "FCF yield / EBITDA",
            "value": fmt_percent(latest_indicators["fcf_yield_ebitda"]),
            "delta": fmt_pp(latest_indicators["fcf_yield_ebitda"] - previous_indicators["fcf_yield_ebitda"]),
            "delta_class": "positive" if latest_indicators["fcf_yield_ebitda"] >= previous_indicators["fcf_yield_ebitda"] else "negative",
            "copy": "Caixa livre gerado em relacao ao EBITDA do periodo.",
        },
        {
            "label": "Alavancagem operacional",
            "value": fmt_multiple(latest_indicators["operating_leverage"], digits=2),
            "delta": fmt_multiple(latest_indicators["operating_leverage"] - previous_indicators["operating_leverage"], digits=2, signed=True),
            "delta_class": "negative" if latest_indicators["operating_leverage"] > previous_indicators["operating_leverage"] else "positive",
            "copy": "Sensibilidade do EBIT a variacoes na margem de contribuicao.",
        },
        {
            "label": "Ponto de equilibrio",
            "value": fmt_brl_compact(latest_indicators["break_even_revenue"]),
            "delta": fmt_percent(((latest_indicators["break_even_revenue"] / previous_indicators["break_even_revenue"]) - 1) * 100, signed=True),
            "delta_class": "positive" if latest_indicators["break_even_revenue"] <= previous_indicators["break_even_revenue"] else "negative",
            "copy": "Receita minima estimada para cobrir a estrutura fixa operacional.",
        },
    ]

    cost_structure_indicator_cards = [
        {
            "label": "Opex / receita",
            "value": fmt_percent(latest_indicators["opex_ratio"]),
            "delta": fmt_pp(latest_indicators["opex_ratio"] - previous_indicators["opex_ratio"]),
            "delta_class": "positive" if latest_indicators["opex_ratio"] <= previous_indicators["opex_ratio"] else "negative",
            "copy": "Despesa operacional total sobre a receita liquida.",
        },
        {
            "label": "SG&A / receita",
            "value": fmt_percent(latest_indicators["sgna_ratio"]),
            "delta": fmt_pp(latest_indicators["sgna_ratio"] - previous_indicators["sgna_ratio"]),
            "delta_class": "positive" if latest_indicators["sgna_ratio"] <= previous_indicators["sgna_ratio"] else "negative",
            "copy": "Comercial e administrativo sobre a receita liquida.",
        },
        {
            "label": "P&D / receita",
            "value": fmt_percent(latest_indicators["research_ratio"]),
            "delta": fmt_pp(latest_indicators["research_ratio"] - previous_indicators["research_ratio"]),
            "delta_class": "positive" if latest_indicators["research_ratio"] <= previous_indicators["research_ratio"] else "negative",
            "copy": "Intensidade de pesquisa e desenvolvimento na estrutura de custo.",
        },
        {
            "label": "Capex manutencao / receita",
            "value": fmt_percent(latest_indicators["maintenance_ratio"]),
            "delta": fmt_pp(latest_indicators["maintenance_ratio"] - previous_indicators["maintenance_ratio"]),
            "delta_class": "positive" if latest_indicators["maintenance_ratio"] <= previous_indicators["maintenance_ratio"] else "negative",
            "copy": "Peso do investimento de sustentacao da base instalada.",
        },
        {
            "label": "Capex expansao / receita",
            "value": fmt_percent(latest_indicators["expansion_ratio"]),
            "delta": fmt_pp(latest_indicators["expansion_ratio"] - previous_indicators["expansion_ratio"]),
            "delta_class": "negative" if latest_indicators["expansion_ratio"] > previous_indicators["expansion_ratio"] else "positive",
            "copy": "Parcela reinvestida em crescimento e aumento de capacidade.",
        },
        {
            "label": "Capex / depreciacao",
            "value": fmt_multiple(latest_indicators["reinvestment_multiple"], digits=2),
            "delta": fmt_multiple(latest_indicators["reinvestment_multiple"] - previous_indicators["reinvestment_multiple"], digits=2, signed=True),
            "delta_class": "negative" if latest_indicators["reinvestment_multiple"] > previous_indicators["reinvestment_multiple"] else "positive",
            "copy": "Ritmo de reinvestimento frente ao desgaste contabil do ativo.",
        },
    ]

    indicator_insight_cards = [
        {
            "kicker": "Rentabilidade e valor",
            "title": "Retorno segue acima do custo de capital",
            "copy": f"O ROIC fechou {latest['year']} em {fmt_percent(latest_indicators['roic'])}, frente a um WACC estimado de {fmt_percent(latest_indicators['wacc'])}, gerando spread de {fmt_pp(latest_indicators['roic_spread'])} e EVA de {fmt_brl_compact(latest_indicators['eva'])}.",
        },
        {
            "kicker": "Capital de giro",
            "title": "Ciclo financeiro permanece controlado",
            "copy": f"O ciclo financeiro ficou em {fmt_days(latest_indicators['financial_cycle'])}, com PMR de {fmt_days(latest_indicators['pmr'])}, PME de {fmt_days(latest_indicators['pme'])} e PMP de {fmt_days(latest_indicators['pmp'])}.",
        },
        {
            "kicker": "Eficiência e caixa",
            "title": "Negocio converte margem em caixa e reinveste",
            "copy": f"A margem de contribuicao atingiu {fmt_percent(latest_indicators['contribution_margin'])}, o FCF yield interno foi de {fmt_percent(latest_indicators['fcf_yield_revenue'])} da receita e o score de saude financeira ficou em {fmt_score(latest_indicators['health_score'])}.",
        },
    ]

    result_chart = build_business_line_chart(
        years,
        [
            {
                "label": "Receita liquida",
                "slug": "revenue",
                "color": "#f8fbff",
                "values": [entry["net_revenue"] for entry in series],
            },
            {
                "label": "EBITDA",
                "slug": "ebitda",
                "color": "rgba(248, 251, 255, 0.74)",
                "values": [entry["ebitda"] for entry in series],
            },
            {
                "label": "Lucro liquido",
                "slug": "net-income",
                "color": "rgba(248, 251, 255, 0.48)",
                "values": [entry["net_income"] for entry in series],
            },
        ],
    )

    cash_chart = build_business_grouped_bar_chart(
        [
            {
                "label": str(year),
                "operating_cash_flow": cash_flow_by_year[year]["operating_cash_flow"],
                "investments": capex_by_year[year]["total_investments"],
                "free_cash_flow": cash_flow_by_year[year]["free_cash_flow"],
            }
            for year in years
        ],
        [
            {"key": "operating_cash_flow", "label": "Caixa operacional", "short_label": "Operac.", "tone": "bright"},
            {"key": "investments", "label": "Investimentos", "short_label": "Invest.", "tone": "mid"},
            {"key": "free_cash_flow", "label": "Caixa livre", "short_label": "FCL", "tone": "soft"},
        ],
    )

    opex_column_chart = build_business_grouped_bar_chart(
        [
            {
                "label": str(entry["year"]),
                "selling_expenses": entry["selling_expenses"],
                "administrative_expenses": entry["administrative_expenses"],
                "research_expenses": entry["research_expenses"],
            }
            for entry in opex_series
        ],
        [
            {"key": "selling_expenses", "label": "Comercial", "short_label": "Com.", "tone": "bright"},
            {"key": "administrative_expenses", "label": "Administrativo", "short_label": "Adm.", "tone": "mid"},
            {"key": "research_expenses", "label": "P&D", "short_label": "P&D", "tone": "soft"},
        ],
    )

    balance_mix_rows = []
    for entry in balance_series:
        total_assets = entry["total_assets"] or 1
        current_assets_ratio = (entry["total_current_assets"] / total_assets) * 100
        non_current_assets_ratio = (entry["total_non_current_assets"] / total_assets) * 100
        balance_mix_rows.append(
            {
                "year": entry["year"],
                "asset_segments": [
                    {
                        "label": "Ativo circulante",
                        "value": fmt_percent(current_assets_ratio),
                        "width": clamp_percent(current_assets_ratio),
                        "tone": "bright",
                    },
                    {
                        "label": "Ativo nao circulante",
                        "value": fmt_percent(non_current_assets_ratio),
                        "width": clamp_percent(non_current_assets_ratio),
                        "tone": "mid",
                    },
                ],
                "funding_segments": [
                    {
                        "label": "Passivo",
                        "value": fmt_percent(entry["liabilities_ratio"]),
                        "width": clamp_percent(entry["liabilities_ratio"]),
                        "tone": "soft",
                    },
                    {
                        "label": "Patrimonio liquido",
                        "value": fmt_percent(entry["equity_ratio"]),
                        "width": clamp_percent(entry["equity_ratio"]),
                        "tone": "bright",
                    },
                ],
            }
        )

    pareto_chart = build_business_pareto_chart(
        [
            {"label": "CPV", "short_label": "CPV", "value": latest["cost_of_sales"]},
            {"label": "Comercial", "short_label": "Com.", "value": latest["selling_expenses"]},
            {"label": "Administrativo", "short_label": "Adm.", "value": latest["administrative_expenses"]},
            {"label": "IR/CSLL", "short_label": "IR", "value": latest["income_tax"]},
            {"label": "Depreciacao", "short_label": "Deprec.", "value": latest["depreciation_amortization"]},
            {"label": "P&D", "short_label": "P&D", "value": latest["research_expenses"]},
            {"label": "Financeiro", "short_label": "Fin.", "value": latest["financial_expenses"]},
        ]
    )

    capex_pie_chart = build_business_pie_chart(
        [
            {"label": "Capex de manutencao", "value": latest_capex["maintenance_capex"]},
            {"label": "Capex de expansao", "value": latest_capex["expansion_capex"]},
            {"label": "Capex de tecnologia", "value": latest_capex["technology_capex"]},
            {"label": "Intangivel e software", "value": latest_capex["intangibles_investments"]},
        ],
        center_title=f"CAPEX {latest_capex['year']}",
        center_value=fmt_brl_compact(latest_capex["total_investments"]),
    )

    top_three_pareto_share = sum(item["share_pct"] for item in pareto_chart["bars"][:3])
    top_pareto_label = pareto_chart["bars"][0]["label"]
    chart_summary_cards = [
        {
            "kicker": "Receita liquida",
            "value": fmt_brl_compact(latest["net_revenue"]),
            "chip": fmt_percent(((latest["net_revenue"] / previous["net_revenue"]) - 1) * 100, signed=True),
            "chip_class": "positive",
            "copy": f"A linha de receita saiu de {fmt_brl_compact(previous['net_revenue'])} em {previous['year']} para {fmt_brl_compact(latest['net_revenue'])} em {latest['year']}.",
        },
        {
            "kicker": "EBITDA",
            "value": fmt_brl_compact(latest["ebitda"]),
            "chip": f"Margem {fmt_percent(latest['ebitda_margin'])}",
            "chip_class": "positive",
            "copy": "O grafico de resultado consolida crescimento de receita, margem operacional e lucro liquido na mesma leitura.",
        },
        {
            "kicker": "Caixa operacional",
            "value": fmt_brl_compact(latest_cash_flow["operating_cash_flow"]),
            "chip": f"FCL {fmt_brl_compact(latest_cash_flow['free_cash_flow'])}",
            "chip_class": "positive",
            "copy": "A aba de graficos destaca a cobertura dos investimentos recorrentes pela geracao operacional.",
        },
        {
            "kicker": "Pareto de despesas",
            "value": fmt_percent(top_three_pareto_share),
            "chip": top_pareto_label,
            "chip_class": "positive",
            "copy": f"As tres maiores rubricas concentram {fmt_percent(top_three_pareto_share)} da pressao total sobre o resultado em {latest['year']}.",
        },
    ]

    chart_insight_cards = [
        {
            "kicker": "Resultado",
            "title": "Crescimento preserva a expansao de margem",
            "copy": f"Receita liquida, EBITDA e lucro liquido sobem em sequencia ate {latest['year']}, com margem EBITDA em {fmt_percent(latest['ebitda_margin'])} e margem liquida em {fmt_percent(latest['net_margin'])}.",
        },
        {
            "kicker": "Caixa e investimento",
            "title": "Operacao cobre o ciclo de investimento",
            "copy": f"O caixa operacional de {fmt_brl_compact(latest_cash_flow['operating_cash_flow'])} segue acima do pacote de investimentos de {fmt_brl_compact(latest_capex['total_investments'])}, sustentando FCL positivo em {fmt_brl_compact(latest_cash_flow['free_cash_flow'])}.",
        },
        {
            "kicker": "Pareto",
            "title": "CPV lidera a concentracao de despesas",
            "copy": f"{pareto_chart['bars'][0]['label']}, {pareto_chart['bars'][1]['label']} e {pareto_chart['bars'][2]['label']} explicam {fmt_percent(top_three_pareto_share)} da pressao total sobre o resultado.",
        },
    ]

    capex_project_rows = []
    for project in BUSINESS_CAPEX_PROJECTS:
        roi = (project["annual_ebit_gain"] / project["investment"]) * 100 if project["investment"] else None
        payback = (project["investment"] / project["annual_cash_gain"]) if project["annual_cash_gain"] else None
        capex_project_rows.append(
            {
                "name": project["name"],
                "focus": project["focus"],
                "year": project["year"],
                "investment": fmt_brl_compact(project["investment"]),
                "annual_cash_gain": fmt_brl_compact(project["annual_cash_gain"]),
                "annual_ebit_gain": fmt_brl_compact(project["annual_ebit_gain"]),
                "roi": fmt_percent(roi),
                "payback": f"{payback:.1f} anos".replace(".", ",") if payback is not None else "--",
            }
        )

    return {
        "company_name": "DS CORP Holding",
        "period_label": f"{years[0]} a {years[-1]}",
        "report_currency": "BRL",
        "analysis_basis": "Analise vertical com base na receita liquida = 100%. Analise horizontal calculada sobre a variacao anual de cada linha em valor absoluto.",
        "years": years,
        "summary_cards": summary_cards,
        "margin_cards": margin_cards,
        "margin_tracks": margin_tracks,
        "dre_rows": dre_rows,
        "insight_cards": insight_cards,
        "balance_analysis_basis": "Analise vertical com base no ativo total = 100%. Analise horizontal calculada sobre a variacao anual de cada conta patrimonial em valor absoluto.",
        "balance_summary_cards": balance_summary_cards,
        "balance_ratio_cards": balance_ratio_cards,
        "balance_tracks": balance_tracks,
        "balance_rows": balance_rows,
        "balance_insight_cards": balance_insight_cards,
        "cash_flow_analysis_basis": "Analise vertical com base na receita liquida = 100%. Analise horizontal calculada sobre a variacao anual da magnitude de cada fluxo de caixa.",
        "cash_flow_summary_cards": cash_flow_summary_cards,
        "cash_flow_ratio_cards": cash_flow_ratio_cards,
        "cash_flow_tracks": cash_flow_tracks,
        "cash_flow_rows": cash_flow_rows,
        "cash_flow_insight_cards": cash_flow_insight_cards,
        "chart_summary_cards": chart_summary_cards,
        "result_chart": result_chart,
        "cash_chart": cash_chart,
        "opex_column_chart": opex_column_chart,
        "balance_mix_rows": balance_mix_rows,
        "pareto_chart": pareto_chart,
        "capex_pie_chart": capex_pie_chart,
        "chart_insight_cards": chart_insight_cards,
        "indicator_summary_cards": indicator_summary_cards,
        "profitability_indicator_cards": profitability_indicator_cards,
        "working_capital_indicator_cards": working_capital_indicator_cards,
        "leverage_indicator_cards": leverage_indicator_cards,
        "efficiency_indicator_cards": efficiency_indicator_cards,
        "cost_structure_indicator_cards": cost_structure_indicator_cards,
        "indicator_rows": indicator_rows,
        "indicator_insight_cards": indicator_insight_cards,
        "opex_analysis_basis": "Analise vertical com base na receita liquida = 100%. Analise horizontal calculada sobre a variacao anual de cada bloco de despesas operacionais.",
        "opex_summary_cards": opex_summary_cards,
        "opex_ratio_cards": opex_ratio_cards,
        "opex_tracks": opex_tracks,
        "opex_rows": opex_rows,
        "opex_insight_cards": opex_insight_cards,
        "capex_analysis_basis": "Analise vertical com base na receita liquida = 100%. Analise horizontal calculada sobre a variacao anual de cada frente de investimento.",
        "capex_summary_cards": capex_summary_cards,
        "capex_ratio_cards": capex_ratio_cards,
        "capex_tracks": capex_tracks,
        "capex_rows": capex_rows,
        "capex_insight_cards": capex_insight_cards,
        "capex_project_rows": capex_project_rows,
        "latest_year": latest["year"],
    }


def load_country_locale_names(force=False):
    global COUNTRY_NAME_CACHE

    if force:
        COUNTRY_NAME_CACHE = None

    if COUNTRY_NAME_CACHE is not None and not COUNTRY_NAME_CACHE.empty:
        return COUNTRY_NAME_CACHE

    if os.path.exists(COUNTRY_NAME_CACHE_FILE):
        COUNTRY_NAME_CACHE = pd.read_csv(COUNTRY_NAME_CACHE_FILE)
        if not COUNTRY_NAME_CACHE.empty:
            if "name_en_key" not in COUNTRY_NAME_CACHE.columns:
                COUNTRY_NAME_CACHE["name_en_key"] = COUNTRY_NAME_CACHE["name_en"].map(normalize_search)
            return COUNTRY_NAME_CACHE

    url = "https://restcountries.com/v3.1/all?fields=cca3,name,translations"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        rows = []
        for item in payload:
            code = item.get("cca3")
            if not code:
                continue
            rows.append(
                {
                    "country_code": code,
                    "name_en": item.get("name", {}).get("common"),
                    "name_pt": item.get("translations", {}).get("por", {}).get("common"),
                }
            )
        COUNTRY_NAME_CACHE = pd.DataFrame(rows).dropna(subset=["country_code"]).sort_values("country_code")
        COUNTRY_NAME_CACHE["name_en_key"] = COUNTRY_NAME_CACHE["name_en"].map(normalize_search)
        COUNTRY_NAME_CACHE.to_csv(COUNTRY_NAME_CACHE_FILE, index=False)
        return COUNTRY_NAME_CACHE
    except Exception:
        return pd.DataFrame(columns=["country_code", "name_en", "name_pt", "name_en_key"])


def get_country_display_parts(name, country_code=None, language="pt"):
    raw_name = str(name or "").strip()
    locale_names = load_country_locale_names()
    display_name = None
    normalized_name = normalize_search(raw_name)

    if country_code and not locale_names.empty:
        match = locale_names.loc[locale_names["country_code"] == str(country_code).strip().upper()]
        if not match.empty:
            locale_column = "name_pt" if language == "pt" else "name_en"
            display_name = match.iloc[0].get(locale_column)

    if not display_name:
        if language == "pt":
            display_name = COUNTRY_NAME_ALIASES.get(raw_name) or COUNTRY_NAME_ALIASES.get(normalized_name)
            if not display_name and not locale_names.empty:
                match = locale_names.loc[locale_names["name_en_key"] == normalized_name]
                if not match.empty:
                    display_name = match.iloc[0].get("name_pt")
        else:
            display_name = raw_name

    if not display_name:
        display_name = raw_name

    detail = raw_name if display_name != raw_name else ""
    return display_name, detail


def normalize_hdi_dataframe(dataframe):
    if dataframe.empty:
        return pd.DataFrame(
            columns=[
                "rank",
                "country",
                "hdi",
                "life_expectancy",
                "expected_schooling",
                "mean_schooling",
                "gni_per_capita",
                "gni_rank_gap",
                "hdi_rank_2022",
                "development_group",
                "report_year",
            ]
        )

    numeric_columns = [
        "rank",
        "hdi",
        "life_expectancy",
        "expected_schooling",
        "mean_schooling",
        "gni_per_capita",
        "gni_rank_gap",
        "hdi_rank_2022",
        "report_year",
    ]
    normalized = dataframe.copy()
    for column in numeric_columns:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized


def extract_hdi_report_edition(url):
    raw_url = str(url or "")
    full_year_match = re.search(r"/(20\d{2})_HDR/", raw_url, flags=re.IGNORECASE)
    if full_year_match:
        return int(full_year_match.group(1))

    short_year_match = re.search(r"HDR(\d{2})_", raw_url, flags=re.IGNORECASE)
    if short_year_match:
        return 2000 + int(short_year_match.group(1))

    return None


def discover_hdi_data_url():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": HDI_SOURCE_PAGE_URL,
    }

    try:
        response = requests.get(HDI_SOURCE_PAGE_URL, headers=headers, timeout=30)
        response.raise_for_status()
        match = re.search(
            r'href="([^"]+Statistical_Annex_HDI_Table\.xlsx)"',
            response.text,
            flags=re.IGNORECASE,
        )
        if match:
            return requests.compat.urljoin(HDI_SOURCE_PAGE_URL, match.group(1))
    except Exception:
        pass

    return HDI_DATA_FALLBACK_URL


def discover_hdi_time_series_url():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": HDI_SOURCE_PAGE_URL,
    }

    try:
        response = requests.get(f"{HDI_SOURCE_PAGE_URL.rsplit('/', 1)[0]}/documentation-and-downloads", headers=headers, timeout=30)
        response.raise_for_status()
        match = re.search(
            r'href="([^"]+Composite_indices_complete_time_series\.csv)"',
            response.text,
            flags=re.IGNORECASE,
        )
        if match:
            return requests.compat.urljoin(HDI_SOURCE_PAGE_URL, match.group(1))
    except Exception:
        pass

    return HDI_TIME_SERIES_FALLBACK_URL


def parse_hdi_excel_bytes(content):
    try:
        raw = pd.read_excel(BytesIO(content), header=5)
    except Exception:
        return normalize_hdi_dataframe(pd.DataFrame())

    if raw.empty or len(raw.columns) < 15:
        return normalize_hdi_dataframe(pd.DataFrame())

    report_year = pd.to_numeric(raw.iloc[0, 2], errors="coerce")
    current_group = ""
    rows = []

    for _, row in raw.iloc[1:].iterrows():
        rank = pd.to_numeric(row.iloc[0], errors="coerce")
        country_label = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""

        if pd.isna(rank):
            if country_label and "human development" in country_label.lower() and country_label.lower() != "human development groups":
                current_group = country_label
            continue

        rows.append(
            {
                "rank": rank,
                "country": country_label,
                "hdi": row.iloc[2],
                "life_expectancy": row.iloc[4],
                "expected_schooling": row.iloc[6],
                "mean_schooling": row.iloc[8],
                "gni_per_capita": row.iloc[10],
                "gni_rank_gap": row.iloc[12],
                "hdi_rank_2022": row.iloc[14],
                "development_group": current_group,
                "report_year": int(report_year) if not pd.isna(report_year) else None,
            }
        )

    parsed = normalize_hdi_dataframe(pd.DataFrame(rows))
    if parsed.empty:
        return parsed

    return parsed.sort_values(["rank", "country"]).reset_index(drop=True)


def load_hdi_data(force=False):
    global HDI_CACHE, HDI_DATA_URL_CACHE

    if force:
        HDI_CACHE = None

    if HDI_CACHE is not None and not HDI_CACHE.empty:
        return HDI_CACHE

    if os.path.exists(HDI_CACHE_FILE):
        HDI_CACHE = normalize_hdi_dataframe(pd.read_csv(HDI_CACHE_FILE))
        if not HDI_CACHE.empty:
            return HDI_CACHE

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": HDI_SOURCE_PAGE_URL,
    }
    hdi_data_url = discover_hdi_data_url()
    HDI_DATA_URL_CACHE = hdi_data_url

    try:
        response = requests.get(hdi_data_url, headers=headers, timeout=60)
        response.raise_for_status()
        HDI_CACHE = parse_hdi_excel_bytes(response.content)
        if not HDI_CACHE.empty:
            HDI_CACHE.to_csv(HDI_CACHE_FILE, index=False)
        return HDI_CACHE
    except Exception:
        return normalize_hdi_dataframe(pd.DataFrame())


def load_hdi_time_series_data(force=False):
    global HDI_TIME_SERIES_CACHE, HDI_TIME_SERIES_URL_CACHE

    if force:
        HDI_TIME_SERIES_CACHE = None

    if HDI_TIME_SERIES_CACHE is not None and not HDI_TIME_SERIES_CACHE.empty:
        return HDI_TIME_SERIES_CACHE

    if os.path.exists(HDI_TIME_SERIES_CACHE_FILE):
        HDI_TIME_SERIES_CACHE = pd.read_csv(HDI_TIME_SERIES_CACHE_FILE)
        if not HDI_TIME_SERIES_CACHE.empty:
            return HDI_TIME_SERIES_CACHE

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": HDI_SOURCE_PAGE_URL,
    }
    time_series_url = discover_hdi_time_series_url()
    HDI_TIME_SERIES_URL_CACHE = time_series_url

    try:
        response = requests.get(time_series_url, headers=headers, timeout=60)
        response.raise_for_status()
        HDI_TIME_SERIES_CACHE = pd.read_csv(BytesIO(response.content), encoding="latin1")
        if not HDI_TIME_SERIES_CACHE.empty:
            HDI_TIME_SERIES_CACHE.to_csv(HDI_TIME_SERIES_CACHE_FILE, index=False)
        return HDI_TIME_SERIES_CACHE
    except Exception:
        return pd.DataFrame()


def build_dashboard_link(source, selected_year, selected_limit, selected_query, selected_metric, selected_language):
    query = f"/pib-mundial/?source={source}&year={selected_year}&top={selected_limit}&metric={selected_metric}&lang={selected_language}"
    if selected_query:
        query += f"&q={requests.utils.quote(selected_query)}"
    return query


def build_hdi_dashboard_link(selected_year, selected_limit, selected_group, selected_query):
    query = f"/idh-mundial/?year={selected_year}&top={selected_limit}&group={selected_group}"
    if selected_query:
        query += f"&q={requests.utils.quote(selected_query)}"
    return query


def build_macro_dashboard_link(selected_year, selected_limit, selected_query, selected_metric):
    query = f"/panorama-macroeconomico/?year={selected_year}&top={selected_limit}&metric={selected_metric}"
    if selected_query:
        query += f"&q={requests.utils.quote(selected_query)}"
    return query


def build_currency_dashboard_link(selected_base, selected_amount, selected_query):
    query = f"/cotacao-moedas/?base={selected_base}&amount={selected_amount}"
    if selected_query:
        query += f"&q={requests.utils.quote(selected_query)}"
    return query


def build_stock_dashboard_link(selected_base, selected_capital, selected_market, selected_query):
    query = f"/acoes-mercado-mundial/?base={selected_base}&capital={selected_capital}&market={selected_market}"
    if selected_query:
        query += f"&q={requests.utils.quote(selected_query)}"
    return query


def format_metric_value(metric_key, value):
    if metric_key == "growth":
        return fmt_percent(value, signed=True)
    if metric_key == "per_capita":
        return fmt_currency_unit(value)
    return fmt_currency(value)


def format_macro_metric_value(metric_key, value):
    if metric_key in {"growth_real", "inflation", "real_interest"}:
        return fmt_percent(value, signed=True)
    if metric_key == "unemployment":
        return fmt_percent(value, signed=False)
    if metric_key in {"current_account", "exports"}:
        return fmt_currency(value)
    if metric_key == "per_capita":
        return fmt_currency_unit(value)
    return fmt_currency(value)


def collect_available_years(*dataframes):
    years = set()
    for dataframe in dataframes:
        if dataframe is None or dataframe.empty or "Ano" not in dataframe.columns:
            continue
        numeric_years = pd.to_numeric(dataframe["Ano"], errors="coerce").dropna().astype(int)
        years.update(numeric_years.tolist())
    return sorted(years)


def merge_indicator_frames_by_year(indicator_frames, selected_year):
    merged = None
    for dataframe, column in indicator_frames:
        if dataframe is None or dataframe.empty or column not in dataframe.columns:
            continue
        year_frame = dataframe[dataframe["Ano"] == selected_year][["Pais", column]].copy()
        if merged is None:
            merged = year_frame
        else:
            merged = merged.merge(year_frame, on="Pais", how="outer")

    if merged is None:
        return pd.DataFrame(columns=["Pais"])
    return merged


def merge_country_history_frames(country_name, indicator_frames):
    merged = None
    for dataframe, column in indicator_frames:
        if dataframe is None or dataframe.empty or column not in dataframe.columns:
            continue
        country_frame = dataframe[dataframe["Pais"] == country_name][["Ano", column]].copy()
        if merged is None:
            merged = country_frame
        else:
            merged = merged.merge(country_frame, on="Ano", how="outer")

    if merged is None:
        return pd.DataFrame(columns=["Ano"])

    merged["Ano"] = pd.to_numeric(merged["Ano"], errors="coerce")
    return merged.dropna(subset=["Ano"]).sort_values("Ano")


def index(request):
    return render(request, "dashboard/index.html")


def empresarial(request):
    return render(request, "dashboard/empresarial.html", build_business_dre_context())


def cotacao_moedas(request):
    force_refresh = request.GET.get("refresh") == "1"
    selected_query = request.GET.get("q", "").strip()
    query_key = normalize_search(selected_query)
    selected_amount = parse_amount(request.GET.get("amount"), 100.0)
    catalog = load_currency_catalog(force=force_refresh)
    crypto_markets = load_crypto_markets(force=force_refresh)

    if catalog.empty:
        return render(
            request,
            "dashboard/cotacao_moedas.html",
            {
                "cards": [],
                "quick_cards": [],
                "table_rows": [],
                "crypto_cards": [],
                "crypto_table_rows": [],
                "currency_options": [],
                "selected_base": CURRENCY_DEFAULT_BASE,
                "selected_base_label": CURRENCY_DEFAULT_BASE,
                "selected_amount": selected_amount,
                "selected_amount_display": fmt_amount_value(selected_amount),
                "selected_query": selected_query,
                "updated_date": "--",
                "results_label": "Nenhuma moeda encontrada",
                "crypto_results_label": "Nenhuma criptomoeda encontrada",
                "clear_link": build_currency_dashboard_link(CURRENCY_DEFAULT_BASE, 100, ""),
                "empty_message": "Nao foi possivel carregar as cotacoes.",
                "crypto_empty_message": "Nao foi possivel carregar os dados de criptomoedas.",
                "source_title": "Frankfurter",
                "source_points": [],
            },
        )

    available_codes = catalog["code"].tolist()
    selected_base = str(request.GET.get("base") or CURRENCY_DEFAULT_BASE).strip().upper()
    if selected_base not in available_codes:
        selected_base = CURRENCY_DEFAULT_BASE if CURRENCY_DEFAULT_BASE in available_codes else available_codes[0]

    rates = load_currency_rates(selected_base, force=force_refresh)
    if rates.empty:
        return render(
            request,
            "dashboard/cotacao_moedas.html",
            {
                "cards": [],
                "quick_cards": [],
                "table_rows": [],
                "crypto_cards": [],
                "crypto_table_rows": [],
                "currency_options": [
                    {"code": row["code"], "label": f'{row["code"]} - {row["name"]}'}
                    for _, row in catalog.iterrows()
                ],
                "selected_base": selected_base,
                "selected_base_label": f"{selected_base} - {selected_base}",
                "selected_amount": selected_amount,
                "selected_amount_display": fmt_amount_value(selected_amount),
                "selected_query": selected_query,
                "updated_date": "--",
                "results_label": "Nenhuma moeda encontrada",
                "crypto_results_label": "Nenhuma criptomoeda encontrada",
                "clear_link": build_currency_dashboard_link(CURRENCY_DEFAULT_BASE, 100, ""),
                "empty_message": "Nao foi possivel carregar as cotacoes para a moeda base selecionada.",
                "crypto_empty_message": "Nao foi possivel carregar os dados de criptomoedas.",
                "source_title": "Frankfurter",
                "source_points": [],
            },
        )

    rates["quote"] = rates["quote"].astype(str).str.upper()
    rates["rate"] = pd.to_numeric(rates["rate"], errors="coerce")
    rates = rates[rates["rate"].notna()].copy()
    rates = rates[rates["quote"] != selected_base].copy()
    rates = rates.merge(catalog, left_on="quote", right_on="code", how="left")
    rates["search_key"] = (
        rates["quote"].fillna("")
        + " "
        + rates["name"].fillna("")
        + " "
        + rates["symbol"].fillna("")
    ).map(normalize_search)
    all_rates = rates.sort_values(["quote", "name"]).reset_index(drop=True)
    if query_key:
        rates = all_rates[all_rates["search_key"].str.contains(query_key, na=False)].copy()
    else:
        rates = all_rates.copy()

    updated_date = str(all_rates["date"].dropna().iloc[0]) if not all_rates.empty and all_rates["date"].notna().any() else "--"
    base_name_match = catalog.loc[catalog["code"] == selected_base]
    base_name = base_name_match.iloc[0]["name"] if not base_name_match.empty else selected_base
    if selected_base == "USD":
        usd_to_base = 1.0
    else:
        usd_rate_match = all_rates.loc[all_rates["quote"] == "USD", "rate"]
        usd_to_base = (1 / usd_rate_match.iloc[0]) if not usd_rate_match.empty and usd_rate_match.iloc[0] else None

    cards = [
        {"label": "Moeda base", "value": selected_base, "copy": base_name},
        {"label": "Valor de referencia", "value": f"{fmt_amount_value(selected_amount)} {selected_base}", "copy": "Conversao aplicada em todos os cards."},
        {"label": "Data da cotacao", "value": updated_date, "copy": "Serie diaria via Frankfurter."},
        {"label": "Moedas listadas", "value": str(len(rates)), "copy": "Cotacoes disponiveis para a base selecionada."},
    ]

    quick_cards = []
    quick_frame = rates[rates["quote"].isin([code for code in CURRENCY_QUICK_QUOTES if code != selected_base])].copy()
    quick_frame = quick_frame.sort_values("quote")
    for _, row in quick_frame.iterrows():
        converted_amount = selected_amount * row["rate"]
        inverse_rate = (1 / row["rate"]) if row["rate"] else None
        quick_cards.append(
            {
                "code": row["quote"],
                "name": row["name"] or row["quote"],
                "rate": fmt_rate(row["rate"]),
                "conversion": f'{fmt_amount_value(selected_amount)} {selected_base} = {fmt_amount_value(converted_amount)} {row["quote"]}',
                "inverse": f'1 {row["quote"]} = {fmt_rate(inverse_rate)} {selected_base}' if inverse_rate else "--",
            }
        )

    table_rows = []
    for _, row in rates.iterrows():
        inverse_rate = (1 / row["rate"]) if row["rate"] else None
        converted_amount = selected_amount * row["rate"]
        table_rows.append(
            {
                "quote": row["quote"],
                "name": row["name"] or row["quote"],
                "symbol": row["symbol"] or "--",
                "rate": fmt_rate(row["rate"]),
                "inverse_rate": fmt_rate(inverse_rate),
                "converted_amount": fmt_amount_value(converted_amount),
            }
        )

    crypto_cards = []
    crypto_table_rows = []
    if not crypto_markets.empty and usd_to_base is not None:
        crypto_markets = crypto_markets.copy()
        crypto_markets["symbol"] = crypto_markets["symbol"].astype(str).str.upper()
        crypto_markets["name"] = crypto_markets["name"].fillna("")
        crypto_markets["search_key"] = (
            crypto_markets["symbol"].fillna("")
            + " "
            + crypto_markets["name"].fillna("")
        ).map(normalize_search)
        if query_key:
            crypto_markets = crypto_markets[crypto_markets["search_key"].str.contains(query_key, na=False)].copy()

        crypto_markets = crypto_markets.sort_values(["market_cap_rank", "name"], ascending=[True, True]).reset_index(drop=True)
        for _, row in crypto_markets.iterrows():
            price_in_base = row["current_price_usd"] * usd_to_base
            amount_in_crypto = (selected_amount / price_in_base) if price_in_base else None
            crypto_row = {
                "symbol": row["symbol"],
                "name": row["name"] or row["symbol"],
                "rank": int(row["market_cap_rank"]) if pd.notna(row["market_cap_rank"]) else "--",
                "price_usd": fmt_rate(row["current_price_usd"]),
                "price_base": fmt_rate(price_in_base),
                "change_24h": fmt_percent(row["price_change_pct_24h"], signed=True),
                "change_24h_tone": tone_for_number(row["price_change_pct_24h"]),
                "amount_buys": fmt_rate(amount_in_crypto),
            }
            crypto_table_rows.append(crypto_row)

        for crypto_row in crypto_table_rows[:4]:
            crypto_cards.append(
                {
                    "symbol": crypto_row["symbol"],
                    "name": crypto_row["name"],
                    "price_usd": crypto_row["price_usd"],
                    "price_base": crypto_row["price_base"],
                    "change_24h": crypto_row["change_24h"],
                    "change_24h_tone": crypto_row["change_24h_tone"],
                    "amount_buys": crypto_row["amount_buys"],
                }
            )

    source_points = [
        'Fonte: <a href="https://frankfurter.dev/" target="_blank" rel="noopener noreferrer">Frankfurter API</a>.',
        "A documentacao oficial informa que a API agrega taxas diarias de 20+ bancos centrais e instituicoes oficiais.",
        "As cotacoes desta tela sao referenciais diarias; nao sao intraday.",
        'A secao de cripto usa <a href="https://docs.coingecko.com/" target="_blank" rel="noopener noreferrer">CoinGecko API</a> para preco spot em USD e variacao de 24h.',
    ]
    context = {
        "cards": cards,
        "quick_cards": quick_cards,
        "table_rows": table_rows,
        "crypto_cards": crypto_cards,
        "crypto_table_rows": crypto_table_rows,
        "currency_options": [
            {"code": row["code"], "label": f'{row["code"]} - {row["name"]}'}
            for _, row in catalog.iterrows()
        ],
        "selected_base": selected_base,
        "selected_base_label": f"{selected_base} - {base_name}",
        "selected_amount": selected_amount,
        "selected_amount_display": fmt_amount_value(selected_amount),
        "selected_query": selected_query,
        "updated_date": updated_date,
        "results_label": f"{len(table_rows)} moedas exibidas" if table_rows else "Nenhuma moeda encontrada",
        "crypto_results_label": f"{len(crypto_table_rows)} criptomoedas exibidas" if crypto_table_rows else "Nenhuma criptomoeda encontrada",
        "clear_link": build_currency_dashboard_link(CURRENCY_DEFAULT_BASE, 100, ""),
        "empty_message": "Nenhuma moeda encontrada para o filtro atual." if not table_rows else "",
        "crypto_empty_message": "Nenhuma criptomoeda encontrada para o filtro atual." if not crypto_table_rows else "",
        "source_title": "Frankfurter",
        "source_points": source_points,
    }
    return render(request, "dashboard/cotacao_moedas.html", context)


def acoes_mercado_mundial(request):
    force_refresh = request.GET.get("refresh") == "1"
    selected_query = request.GET.get("q", "").strip()
    query_key = normalize_search(selected_query)
    selected_capital = parse_amount(request.GET.get("capital"), GLOBAL_STOCK_DEFAULT_CAPITAL)
    selected_market = parse_stock_market(request.GET.get("market"))
    selected_market_label = GLOBAL_STOCK_MARKET_FILTERS[selected_market]["label"]

    stock_frame = load_global_stock_quotes(force=force_refresh)
    currency_catalog = load_currency_catalog(force=force_refresh)
    available_codes = currency_catalog["code"].tolist() if not currency_catalog.empty else []
    selected_base = str(request.GET.get("base") or GLOBAL_STOCK_DEFAULT_BASE).strip().upper()
    if available_codes and selected_base not in available_codes:
        if GLOBAL_STOCK_DEFAULT_BASE in available_codes:
            selected_base = GLOBAL_STOCK_DEFAULT_BASE
        else:
            selected_base = available_codes[0]

    base_match = currency_catalog.loc[currency_catalog["code"] == selected_base] if not currency_catalog.empty else pd.DataFrame()
    base_name = base_match.iloc[0]["name"] if not base_match.empty else selected_base
    selected_base_label = f"{selected_base} - {base_name}" if base_name else selected_base
    currency_options = (
        [{"code": row["code"], "label": f'{row["code"]} - {row["name"]}'} for _, row in currency_catalog.iterrows()]
        if not currency_catalog.empty
        else [{"code": selected_base, "label": selected_base_label}]
    )
    market_options = [
        {"key": key, "label": config["label"]}
        for key, config in GLOBAL_STOCK_MARKET_FILTERS.items()
    ]

    if stock_frame.empty:
        return render(
            request,
            "dashboard/acoes_mercado_mundial.html",
            {
                "cards": [],
                "quick_cards": [],
                "table_rows": [],
                "currency_options": currency_options,
                "market_options": market_options,
                "selected_base": selected_base,
                "selected_base_label": selected_base_label,
                "selected_capital": selected_capital,
                "selected_capital_display": fmt_amount_value(selected_capital),
                "selected_query": selected_query,
                "selected_market": selected_market,
                "selected_market_label": selected_market_label,
                "updated_date": "--",
                "results_label": "Nenhuma acao encontrada",
                "clear_link": build_stock_dashboard_link(GLOBAL_STOCK_DEFAULT_BASE, GLOBAL_STOCK_DEFAULT_CAPITAL, "all", ""),
                "empty_message": "Nao foi possivel carregar as acoes globais no momento.",
                "source_title": "Yahoo Finance",
                "source_points": [],
            },
        )

    stock_frame = stock_frame.copy()
    quote_currencies = sorted(
        {
            str(currency).strip().upper()
            for currency in stock_frame["currency"].dropna().tolist()
            if str(currency).strip()
        }
    )
    conversion_map = {selected_base: 1.0}
    if quote_currencies:
        rates = load_currency_rates(selected_base, quotes=quote_currencies, force=force_refresh)
        if not rates.empty:
            rates["quote"] = rates["quote"].astype(str).str.upper()
            rates["rate"] = pd.to_numeric(rates["rate"], errors="coerce")
            for _, row in rates.iterrows():
                if pd.notna(row["rate"]) and row["rate"]:
                    conversion_map[row["quote"]] = row["rate"]

    stock_frame["currency"] = stock_frame["currency"].astype(str).str.upper()
    stock_frame["price_local"] = pd.to_numeric(stock_frame["price_local"], errors="coerce")
    stock_frame["previous_close"] = pd.to_numeric(stock_frame["previous_close"], errors="coerce")
    stock_frame["day_high"] = pd.to_numeric(stock_frame["day_high"], errors="coerce")
    stock_frame["day_low"] = pd.to_numeric(stock_frame["day_low"], errors="coerce")
    stock_frame["volume"] = pd.to_numeric(stock_frame["volume"], errors="coerce")
    stock_frame["regular_market_time"] = pd.to_numeric(stock_frame["regular_market_time"], errors="coerce")
    stock_frame["change_pct"] = (
        (stock_frame["price_local"] - stock_frame["previous_close"]) / stock_frame["previous_close"] * 100
    )
    stock_frame["price_base"] = stock_frame.apply(
        lambda row: convert_value_to_base(row["price_local"], row["currency"], selected_base, conversion_map),
        axis=1,
    )
    stock_frame["capital_buys"] = stock_frame["price_base"].apply(
        lambda price: (selected_capital / price) if price and not pd.isna(price) else None
    )
    stock_frame["search_key"] = (
        stock_frame["symbol"].fillna("")
        + " "
        + stock_frame["company"].fillna("")
        + " "
        + stock_frame["market_label"].fillna("")
        + " "
        + stock_frame["sector"].fillna("")
        + " "
        + stock_frame["exchange"].fillna("")
    ).map(normalize_search)

    filtered = stock_frame.copy()
    if selected_market != "all":
        filtered = filtered[filtered["market_key"] == selected_market].copy()
    if query_key:
        filtered = filtered[filtered["search_key"].str.contains(query_key, na=False)].copy()

    filtered = filtered.sort_values(["market_label", "symbol"]).reset_index(drop=True)
    latest_timestamp = stock_frame["regular_market_time"].dropna().max() if stock_frame["regular_market_time"].notna().any() else None
    updated_date = fmt_quote_date(latest_timestamp)

    performance_frame = filtered.dropna(subset=["change_pct"]).sort_values(["change_pct", "symbol"], ascending=[False, True])
    best_row = performance_frame.iloc[0] if not performance_frame.empty else None
    cards = [
        {"label": "Mercado ativo", "value": selected_market_label, "copy": "Watchlist global distribuido por regioes."},
        {"label": "Moeda base", "value": selected_base, "copy": base_name or selected_base},
        {
            "label": "Capital simulado",
            "value": f"{fmt_amount_value(selected_capital)} {selected_base}",
            "copy": "Quantidade estimada de acoes em cada papel.",
        },
        {
            "label": "Melhor pregao",
            "value": best_row["symbol"] if best_row is not None else "--",
            "copy": (
                f'{best_row["company"]} | {fmt_percent(best_row["change_pct"], signed=True)}'
                if best_row is not None
                else "Sem variacao disponivel no filtro atual."
            ),
        },
    ]

    quick_cards = []
    quick_frame = filtered.dropna(subset=["change_pct"]).copy()
    quick_frame["abs_change"] = quick_frame["change_pct"].abs()
    quick_frame = quick_frame.sort_values(["abs_change", "symbol"], ascending=[False, True]).head(8)
    for _, row in quick_frame.iterrows():
        quick_cards.append(
            {
                "symbol": row["symbol"],
                "company": row["company"],
                "market_short": row["market_short"] or row["market_label"],
                "price_base": fmt_rate(row["price_base"]),
                "price_local": fmt_rate(row["price_local"]),
                "currency": row["currency"],
                "change": fmt_percent(row["change_pct"], signed=True),
                "change_tone": tone_for_number(row["change_pct"]),
                "capital_buys": fmt_rate(row["capital_buys"]),
            }
        )

    table_rows = []
    for _, row in filtered.iterrows():
        day_range = "--"
        if pd.notna(row["day_low"]) and pd.notna(row["day_high"]):
            day_range = f'{fmt_rate(row["day_low"])} - {fmt_rate(row["day_high"])} {row["currency"]}'

        table_rows.append(
            {
                "symbol": row["symbol"],
                "company": row["company"],
                "market_label": row["market_label"],
                "exchange": row["exchange"] or "--",
                "sector": row["sector"] or "--",
                "price_local": fmt_rate(row["price_local"]),
                "currency": row["currency"] or "--",
                "price_base": fmt_rate(row["price_base"]),
                "change": fmt_percent(row["change_pct"], signed=True),
                "change_tone": tone_for_number(row["change_pct"]),
                "day_range": day_range,
                "volume": fmt_compact_number(row["volume"]),
                "capital_buys": fmt_rate(row["capital_buys"]),
                "quote_date": fmt_quote_date(row["regular_market_time"]),
            }
        )

    source_points = [
        'Fonte principal: <a href="https://finance.yahoo.com/" target="_blank" rel="noopener noreferrer">Yahoo Finance</a> para ultimo preco regular, fechamento anterior, faixa diaria e volume por papel.',
        'Conversao para a moeda base via <a href="https://frankfurter.dev/" target="_blank" rel="noopener noreferrer">Frankfurter</a>.',
        "Como cada bolsa fecha em horarios diferentes, a data do pregao pode variar de uma acao para outra.",
        "A tela trabalha com uma watchlist internacional curada para leitura rapida de Estados Unidos, Europa, Asia-Pacifico e America Latina.",
    ]
    context = {
        "cards": cards,
        "quick_cards": quick_cards,
        "table_rows": table_rows,
        "currency_options": currency_options,
        "market_options": market_options,
        "selected_base": selected_base,
        "selected_base_label": selected_base_label,
        "selected_capital": selected_capital,
        "selected_capital_display": fmt_amount_value(selected_capital),
        "selected_query": selected_query,
        "selected_market": selected_market,
        "selected_market_label": selected_market_label,
        "updated_date": updated_date,
        "results_label": f"{len(table_rows)} acoes exibidas" if table_rows else "Nenhuma acao encontrada",
        "clear_link": build_stock_dashboard_link(GLOBAL_STOCK_DEFAULT_BASE, GLOBAL_STOCK_DEFAULT_CAPITAL, "all", ""),
        "empty_message": "Nenhuma acao encontrada para o filtro atual." if not table_rows else "",
        "source_title": "Yahoo Finance + Frankfurter",
        "source_points": source_points,
    }
    return render(request, "dashboard/acoes_mercado_mundial.html", context)


def idh_mundial(request):
    force_refresh = request.GET.get("refresh") == "1"
    selected_limit = parse_limit(request.GET.get("top"))
    selected_group = parse_hdi_group(request.GET.get("group"))
    selected_query = request.GET.get("q", "").strip()
    query_key = normalize_search(selected_query)

    dataframe = load_hdi_data(force=force_refresh)
    time_series = load_hdi_time_series_data(force=force_refresh)
    report_year = int(dataframe["report_year"].dropna().iloc[0]) if not dataframe.empty and dataframe["report_year"].notna().any() else 2023
    year_options = get_hdi_available_years(time_series) or [report_year]
    min_year = min(year_options)
    max_year = max(year_options)
    selected_year = parse_year(request.GET.get("year"), max_year, min_year, max_year)
    comparison_year = selected_year - 1 if (selected_year - 1) in year_options else None
    hdi_report_edition = extract_hdi_report_edition(HDI_DATA_URL_CACHE) or extract_hdi_report_edition(HDI_TIME_SERIES_URL_CACHE)
    hdi_report_title = f"Human Development Report {hdi_report_edition}" if hdi_report_edition else "Human Development Reports"
    hdi_report_url = (
        HDI_REPORT_PAGE_URL_TEMPLATE.format(edition=hdi_report_edition)
        if hdi_report_edition
        else HDI_SOURCE_PAGE_URL
    )
    source_title = f"HDR {hdi_report_edition} / PNUD / ONU" if hdi_report_edition else "PNUD / ONU"
    source_points = [
        f'Fonte: <a href="{hdi_report_url}" target="_blank" rel="noopener noreferrer">{hdi_report_title}</a> e <a href="{HDI_SOURCE_PAGE_URL}" target="_blank" rel="noopener noreferrer">Human Development Reports Data Center</a>.',
        (
            f"Os arquivos oficiais atualmente carregados sao do HDR {hdi_report_edition}; o ranking mais recente disponivel nele usa o ano-base {report_year}."
            if hdi_report_edition
            else f"A tabela oficial atualmente carregada usa o ano-base {report_year} para o ranking mais recente do IDH."
        ),
        f"O ranking principal desta tela pode ser recalculado de 1990 a {max_year} a partir da serie oficial do PNUD.",
        "O IDH combina saude, educacao e renda per capita em um indice de 0 a 1.",
    ]

    if dataframe.empty:
        return render(
            request,
            "dashboard/idh_mundial.html",
            {
                "cards": [],
                "historical_cards": [],
                "table_rows": [],
                "limit_options": LIMIT_OPTIONS,
                "selected_limit": 25,
                "selected_year": selected_year,
                "year_options": list(reversed(year_options)),
                "selected_group": "all",
                "selected_group_label": HDI_GROUP_FILTERS["all"]["label"],
                "group_options": HDI_GROUP_FILTERS,
                "selected_query": "",
                "comparison_year_label": comparison_year,
                "results_label": "Nenhum pais encontrado",
                "source_title": source_title,
                "source_url": HDI_SOURCE_PAGE_URL,
                "source_points": source_points,
                "report_year": report_year,
                "total_countries": 0,
                "group_chips": [],
                "history_rows": [],
                "history_country": "",
                "clear_link": build_hdi_dashboard_link(selected_year, 25, "all", ""),
                "top_10_link": build_hdi_dashboard_link(selected_year, 10, "all", ""),
                "all_link": build_hdi_dashboard_link(selected_year, 0, "all", ""),
                "empty_message": "Nao foi possivel carregar os dados de IDH.",
            },
        )

    ranking = dataframe.copy()
    country_parts = ranking["country"].map(lambda country_name: get_country_display_parts(country_name, language="pt"))
    ranking["country_display"] = country_parts.str[0]
    ranking["country_key"] = ranking["country"].map(normalize_search)
    ranking["development_group_display"] = ranking["development_group"].map(HDI_GROUP_LABELS).fillna("Sem classificacao")
    ranking["search_key"] = (
        ranking["country"].fillna("")
        + " "
        + ranking["country_display"].fillna("")
        + " "
        + ranking["development_group_display"].fillna("")
    ).map(normalize_search)

    history_lookup = pd.DataFrame()
    if not time_series.empty:
        history_lookup = time_series.copy()
        history_lookup["country_key"] = history_lookup["country"].map(normalize_search)
        metric_columns = [
            "country_key",
            "hdi_1990",
            f"hdi_{selected_year}",
            f"le_{selected_year}",
            f"eys_{selected_year}",
            f"mys_{selected_year}",
            f"gnipc_{selected_year}",
        ]
        if comparison_year:
            metric_columns.append(f"hdi_{comparison_year}")
        metric_columns = list(dict.fromkeys(column for column in metric_columns if column in history_lookup.columns))
        if metric_columns:
            history_metrics = history_lookup[metric_columns].drop_duplicates(subset=["country_key"])
            ranking = ranking.merge(history_metrics, on="country_key", how="left")

    ranking["hdi_selected"] = pd.to_numeric(ranking.get(f"hdi_{selected_year}", ranking.get("hdi")), errors="coerce")
    ranking["life_expectancy_selected"] = pd.to_numeric(ranking.get(f"le_{selected_year}", ranking.get("life_expectancy")), errors="coerce")
    ranking["expected_schooling_selected"] = pd.to_numeric(ranking.get(f"eys_{selected_year}", ranking.get("expected_schooling")), errors="coerce")
    ranking["mean_schooling_selected"] = pd.to_numeric(ranking.get(f"mys_{selected_year}", ranking.get("mean_schooling")), errors="coerce")
    ranking["gni_per_capita_selected"] = pd.to_numeric(ranking.get(f"gnipc_{selected_year}", ranking.get("gni_per_capita")), errors="coerce")
    ranking["hdi_1990"] = pd.to_numeric(ranking.get("hdi_1990"), errors="coerce")
    ranking["hdi_previous_year"] = pd.to_numeric(ranking.get(f"hdi_{comparison_year}"), errors="coerce") if comparison_year else None
    ranking["hdi_change_from_1990"] = ranking["hdi_selected"] - ranking["hdi_1990"]
    ranking = ranking[ranking["hdi_selected"].notna()].copy()
    ranking = ranking.sort_values(["hdi_selected", "country_display"], ascending=[False, True]).reset_index(drop=True)
    ranking["rank"] = ranking["hdi_selected"].rank(method="min", ascending=False).astype(int)

    if comparison_year:
        previous_ranks = ranking[["country_key", "hdi_previous_year"]].dropna().copy()
        previous_ranks = previous_ranks.sort_values(["hdi_previous_year", "country_key"], ascending=[False, True])
        previous_ranks["previous_rank"] = previous_ranks["hdi_previous_year"].rank(method="min", ascending=False).astype(int)
        ranking = ranking.merge(previous_ranks[["country_key", "previous_rank"]], on="country_key", how="left")
    else:
        ranking["previous_rank"] = None

    active_group = HDI_GROUP_FILTERS[selected_group]["group"]
    current_frame = ranking.copy()
    if active_group:
        current_frame = current_frame[current_frame["development_group"] == active_group].copy()

    if query_key:
        current_frame = current_frame[current_frame["search_key"].str.contains(query_key, na=False)].copy()

    total_matches = len(current_frame)
    table_frame = current_frame if selected_limit == 0 else current_frame.head(selected_limit).copy()

    leader_name = "--"
    leader_value = "--"
    if not current_frame.empty:
        leader_name = current_frame.iloc[0]["country_display"]
        leader_value = fmt_decimal(current_frame.iloc[0]["hdi_selected"], digits=3)

    mean_hdi = current_frame["hdi_selected"].mean() if not current_frame.empty else None
    current_label = HDI_GROUP_FILTERS[selected_group]["label"]
    historical_candidates = current_frame[current_frame["hdi_change_from_1990"].notna()].copy()

    cards = [
        {"label": "Faixa ativa", "value": current_label, "copy": "Recorte oficial do ranking de IDH."},
        {"label": "Lider do ranking", "value": leader_name, "copy": f"IDH {leader_value} em {selected_year}" if leader_value != "--" else "--"},
        {"label": "Paises listados", "value": str(len(table_frame)), "copy": f"De {total_matches} paises encontrados."},
        {"label": "IDH medio", "value": fmt_decimal(mean_hdi, digits=3), "copy": "Media do recorte atualmente exibido."},
    ]

    historical_cards = []
    if selected_year > 1990 and not historical_candidates.empty:
        best_gain = historical_candidates.sort_values("hdi_change_from_1990", ascending=False).iloc[0]
        average_gain = historical_candidates["hdi_change_from_1990"].mean()
        historical_cards = [
            {"label": "Cobertura historica", "value": f"1990-{selected_year}", "copy": "Serie oficial do PNUD para o IDH."},
            {
                "label": "Maior avanco no recorte",
                "value": best_gain["country_display"],
                "copy": f"+{fmt_decimal(best_gain['hdi_change_from_1990'], digits=3)} pontos de IDH desde 1990.",
            },
            {
                "label": "Avanco medio",
                "value": fmt_decimal(average_gain, digits=3),
                "copy": f"Variacao media do IDH entre 1990 e {selected_year} no recorte filtrado.",
            },
            {
                "label": "Paises com serie",
                "value": str(len(historical_candidates)),
                "copy": "Quantidade de paises do recorte com historico comparavel completo.",
            },
        ]

    table_rows = []
    for _, row in table_frame.iterrows():
        previous_rank = row.get("previous_rank")
        rank_change = previous_rank - row["rank"] if previous_rank is not None and not pd.isna(previous_rank) else None
        table_rows.append(
            {
                "rank": int(row["rank"]),
                "country": row["country_display"],
                "country_detail": row["development_group_display"],
                "hdi": fmt_decimal(row["hdi_selected"], digits=3),
                "life_expectancy": fmt_decimal(row["life_expectancy_selected"], digits=1),
                "expected_schooling": fmt_decimal(row["expected_schooling_selected"], digits=1),
                "mean_schooling": fmt_decimal(row["mean_schooling_selected"], digits=1),
                "gni_per_capita": fmt_currency_unit(row["gni_per_capita_selected"]),
                "historical_change": (
                    f"+{fmt_decimal(row['hdi_change_from_1990'], digits=3)}"
                    if selected_year > 1990 and not pd.isna(row["hdi_change_from_1990"])
                    else "--"
                ),
                "rank_change": fmt_signed_int(rank_change),
                "rank_change_tone": tone_for_number(rank_change),
            }
        )

    group_counts = ranking["development_group"].value_counts().to_dict()
    group_chips = [
        {
            "label": HDI_GROUP_LABELS[group_name],
            "count": int(group_counts.get(group_name, 0)),
        }
        for group_name in HDI_GROUP_LABELS
    ]

    history_country = ""
    history_rows = []
    if total_matches == 1 and not history_lookup.empty:
        history_key = current_frame.iloc[0]["country_key"]
        history_country = current_frame.iloc[0]["country_display"]
        history_frame = history_lookup.loc[history_lookup["country_key"] == history_key]
        if not history_frame.empty:
            history_row = history_frame.iloc[0]
            previous_hdi = None
            for year in range(1990, 2024):
                hdi_value = pd.to_numeric(history_row.get(f"hdi_{year}"), errors="coerce")
                if pd.isna(hdi_value):
                    continue
                annual_change = hdi_value - previous_hdi if previous_hdi is not None else None
                history_rows.append(
                    {
                        "year": year,
                        "hdi": fmt_decimal(hdi_value, digits=3),
                        "change": fmt_decimal(annual_change, digits=3) if annual_change is not None else "--",
                        "change_tone": tone_for_number(annual_change),
                        "life_expectancy": fmt_decimal(pd.to_numeric(history_row.get(f"le_{year}"), errors="coerce"), digits=1),
                        "expected_schooling": fmt_decimal(pd.to_numeric(history_row.get(f"eys_{year}"), errors="coerce"), digits=1),
                        "mean_schooling": fmt_decimal(pd.to_numeric(history_row.get(f"mys_{year}"), errors="coerce"), digits=1),
                        "gni_per_capita": fmt_currency_unit(pd.to_numeric(history_row.get(f"gnipc_{year}"), errors="coerce")),
                    }
                )
                previous_hdi = hdi_value

            history_rows = sorted(history_rows, key=lambda row: row["year"], reverse=True)

    context = {
        "cards": cards,
        "historical_cards": historical_cards,
        "table_rows": table_rows,
        "limit_options": LIMIT_OPTIONS,
        "selected_limit": selected_limit,
        "selected_year": selected_year,
        "year_options": list(reversed(year_options)),
        "selected_group": selected_group,
        "selected_group_label": current_label,
        "group_options": HDI_GROUP_FILTERS,
        "selected_query": selected_query,
        "comparison_year_label": comparison_year,
        "results_label": f"{len(table_rows)} de {total_matches} paises exibidos" if table_rows else "Nenhum pais encontrado",
        "source_title": source_title,
        "source_url": HDI_SOURCE_PAGE_URL,
        "source_points": source_points,
        "report_year": report_year,
        "total_countries": len(ranking),
        "group_chips": group_chips,
        "history_rows": history_rows,
        "history_country": history_country,
        "clear_link": build_hdi_dashboard_link(max_year, 25, "all", ""),
        "top_10_link": build_hdi_dashboard_link(selected_year, 10, selected_group, selected_query),
        "all_link": build_hdi_dashboard_link(selected_year, 0, selected_group, selected_query),
        "empty_message": "Nenhum pais encontrado para o filtro atual." if not table_rows else "",
    }
    return render(request, "dashboard/idh_mundial.html", context)


def panorama_macroeconomico(request):
    force_refresh = request.GET.get("refresh") == "1"
    selected_metric = parse_macro_metric(request.GET.get("metric"))
    selected_limit = parse_limit(request.GET.get("top"))
    selected_query = request.GET.get("q", "").strip()
    query_key = normalize_search(selected_query)
    metric_definition = MACRO_METRIC_DEFINITIONS[selected_metric]
    source_title = "Banco Mundial"
    source_points = [
        'Fonte: <a href="https://data.worldbank.org/indicator/NY.GDP.MKTP.CD" target="_blank" rel="noopener noreferrer">PIB nominal</a>, <a href="https://data.worldbank.org/indicator/NY.GDP.PCAP.CD" target="_blank" rel="noopener noreferrer">PIB per capita</a>, <a href="https://data.worldbank.org/indicator/NY.GDP.MKTP.KD.ZG" target="_blank" rel="noopener noreferrer">crescimento real do PIB</a>, <a href="https://data.worldbank.org/indicator/FP.CPI.TOTL.ZG" target="_blank" rel="noopener noreferrer">inflacao ao consumidor</a> e <a href="https://data.worldbank.org/indicator/SL.UEM.TOTL.ZS" target="_blank" rel="noopener noreferrer">desemprego</a> do Banco Mundial.',
        'O painel agora inclui tambem <a href="https://data.worldbank.org/indicator/BN.CAB.XOKA.CD" target="_blank" rel="noopener noreferrer">conta corrente</a>, <a href="https://data.worldbank.org/indicator/NE.EXP.GNFS.CD" target="_blank" rel="noopener noreferrer">exportacoes</a> e <a href="https://data.worldbank.org/indicator/FR.INR.RINR" target="_blank" rel="noopener noreferrer">juros reais</a>, todos em serie anual oficial.',
        "O ranking ordena os paises pelo indicador selecionado, mas mantem as demais variaveis macroeconomicas lado a lado na mesma tabela.",
        "Quando a busca retorna um unico pais, a tela abre a serie historica completa para atividade, renda, inflacao e mercado de trabalho.",
    ]

    gdp_dataframe = load_gdp_data(force=force_refresh)
    per_capita_dataframe = load_gdp_per_capita_data(force=force_refresh)
    growth_real_dataframe = load_gdp_growth_real_data(force=force_refresh)
    inflation_dataframe = load_inflation_data(force=force_refresh)
    unemployment_dataframe = load_unemployment_data(force=force_refresh)
    current_account_dataframe = load_current_account_data(force=force_refresh)
    exports_dataframe = load_exports_data(force=force_refresh)
    real_interest_dataframe = load_real_interest_rate_data(force=force_refresh)
    world_bank_metadata = load_world_bank_country_metadata(force=force_refresh)
    indicator_frames = [
        (gdp_dataframe, "gdp"),
        (per_capita_dataframe, "gdp_per_capita"),
        (growth_real_dataframe, "growth_real_pct"),
        (inflation_dataframe, "inflation_pct"),
        (unemployment_dataframe, "unemployment_pct"),
        (current_account_dataframe, "current_account_usd"),
        (exports_dataframe, "exports_usd"),
        (real_interest_dataframe, "real_interest_pct"),
    ]
    indicator_frame_map = {
        "gdp": gdp_dataframe,
        "per_capita": per_capita_dataframe,
        "growth_real": growth_real_dataframe,
        "inflation": inflation_dataframe,
        "unemployment": unemployment_dataframe,
        "current_account": current_account_dataframe,
        "exports": exports_dataframe,
        "real_interest": real_interest_dataframe,
    }

    available_years = collect_available_years(
        gdp_dataframe,
        per_capita_dataframe,
        growth_real_dataframe,
        inflation_dataframe,
        unemployment_dataframe,
        current_account_dataframe,
        exports_dataframe,
        real_interest_dataframe,
    )
    metric_years = collect_available_years(indicator_frame_map.get(selected_metric))
    year_options = metric_years or available_years
    fallback_year = year_options[-1] if year_options else None
    default_clear_year = collect_available_years(gdp_dataframe)
    default_clear_year = default_clear_year[-1] if default_clear_year else 2024

    if not available_years:
        return render(
            request,
            "dashboard/panorama_macroeconomico.html",
            {
                "cards": [],
                "table_rows": [],
                "history_rows": [],
                "history_country": "",
                "year_options": [],
                "selected_year": None,
                "selected_metric": selected_metric,
                "metric_label": metric_definition["label"],
                "metric_options": MACRO_METRIC_DEFINITIONS,
                "selected_limit": selected_limit,
                "limit_options": LIMIT_OPTIONS,
                "selected_query": selected_query,
                "results_label": "Nenhum pais encontrado",
                "coverage_label": "--",
                "source_title": source_title,
                "source_points": source_points,
                "clear_link": build_macro_dashboard_link(default_clear_year, 25, "", "gdp"),
                "empty_message": "Nao foi possivel carregar os dados macroeconomicos.",
            },
        )

    min_year = min(year_options)
    max_year = max(year_options)
    selected_year = parse_year(request.GET.get("year"), fallback_year, min_year, max_year)
    full_year = merge_indicator_frames_by_year(indicator_frames, selected_year)

    if full_year.empty:
        return render(
            request,
            "dashboard/panorama_macroeconomico.html",
            {
                "cards": [],
                "table_rows": [],
                "history_rows": [],
                "history_country": "",
                "year_options": list(reversed(year_options)),
                "selected_year": selected_year,
                "selected_metric": selected_metric,
                "metric_label": metric_definition["label"],
                "metric_options": MACRO_METRIC_DEFINITIONS,
                "selected_limit": selected_limit,
                "limit_options": LIMIT_OPTIONS,
                "selected_query": selected_query,
                "results_label": "Nenhum pais encontrado",
                "coverage_label": f"{min_year} ate {max_year}",
                "source_title": source_title,
                "source_points": source_points,
                "clear_link": build_macro_dashboard_link(default_clear_year, 25, "", "gdp"),
                "empty_message": "Nao ha dados disponiveis para o ano selecionado.",
            },
        )

    valid_country_names = (
        set(world_bank_metadata.loc[~world_bank_metadata["is_aggregate"], "Pais"].tolist())
        if not world_bank_metadata.empty
        else set()
    )
    if valid_country_names:
        full_year = full_year[full_year["Pais"].isin(valid_country_names)]
    full_year = full_year[full_year["Pais"] != "World"].copy()

    if not world_bank_metadata.empty:
        full_year = full_year.merge(
            world_bank_metadata[["Pais", "country_code", "region"]],
            on="Pais",
            how="left",
        )
    else:
        full_year["country_code"] = None
        full_year["region"] = ""

    if not full_year.empty:
        country_parts = full_year.apply(
            lambda row: get_country_display_parts(row["Pais"], row.get("country_code"), "pt"),
            axis=1,
        )
        full_year["country_display"] = country_parts.str[0]
        full_year["country_detail"] = country_parts.str[1]
    else:
        full_year["country_display"] = pd.Series(dtype=str)
        full_year["country_detail"] = pd.Series(dtype=str)

    full_year["region"] = full_year["region"].fillna("").replace("", "--")
    full_year["metric_value"] = pd.to_numeric(full_year[metric_definition["field"]], errors="coerce")
    full_year = (
        full_year[full_year["metric_value"].notna()]
        .sort_values("metric_value", ascending=False)
        .reset_index(drop=True)
    )
    full_year["rank"] = full_year.index + 1
    full_year["search_key"] = (
        full_year["Pais"].fillna("")
        + " "
        + full_year["country_display"].fillna("")
        + " "
        + full_year["region"].fillna("")
    ).map(normalize_search)

    current_year = full_year.copy()
    if query_key:
        current_year = current_year[current_year["search_key"].str.contains(query_key, na=False)].copy()

    total_matches = len(current_year)
    year_frame = current_year if selected_limit == 0 else current_year.head(selected_limit).copy()

    first_country = year_frame.iloc[0]["country_display"] if not year_frame.empty else "--"
    first_value = format_macro_metric_value(selected_metric, year_frame.iloc[0]["metric_value"]) if not year_frame.empty else "--"
    cards = [
        {"label": "Indicador ativo", "value": metric_definition["short_label"], "copy": f"Banco Mundial | ano {selected_year}."},
        {"label": "Primeiro no recorte", "value": first_country, "copy": first_value},
        {"label": "Paises listados", "value": str(len(year_frame)), "copy": f"De {total_matches} resultados encontrados."},
        {"label": "Cobertura", "value": f"{min_year}-{max_year}", "copy": "Serie macroeconomica consolidada."},
    ]

    table_rows = [
        {
            "rank": int(row["rank"]),
            "country": row["country_display"],
            "country_detail": row["country_detail"],
            "country_raw": row["Pais"],
            "region": row["region"],
            "gdp": fmt_currency(row.get("gdp")),
            "gdp_full": build_full_currency_display(row.get("gdp"))["formatted"],
            "per_capita": fmt_currency_unit(row.get("gdp_per_capita")),
            "growth_real": fmt_percent(row.get("growth_real_pct"), signed=True),
            "growth_real_tone": tone_for_number(row.get("growth_real_pct")),
            "inflation": fmt_percent(row.get("inflation_pct"), signed=True),
            "inflation_tone": "neutral",
            "unemployment": fmt_percent(row.get("unemployment_pct")),
            "unemployment_tone": "neutral",
            "current_account": fmt_currency(row.get("current_account_usd")),
            "current_account_full": build_full_currency_display(row.get("current_account_usd"))["formatted"],
            "exports": fmt_currency(row.get("exports_usd")),
            "exports_full": build_full_currency_display(row.get("exports_usd"))["formatted"],
            "real_interest": fmt_percent(row.get("real_interest_pct"), signed=True),
            "real_interest_tone": tone_for_number(row.get("real_interest_pct")),
        }
        for _, row in year_frame.iterrows()
    ]

    history_country = ""
    history_rows = []
    if len(table_rows) == 1:
        history_country = table_rows[0]["country"]
        history_frame = merge_country_history_frames(table_rows[0]["country_raw"], indicator_frames)
        history_frame = history_frame[
            (history_frame["Ano"] >= min_year)
            & (history_frame["Ano"] <= max_year)
        ].sort_values("Ano", ascending=False)
        history_rows = [
            {
                "year": int(row["Ano"]),
                "gdp": fmt_currency(row.get("gdp")),
                "gdp_full": build_full_currency_display(row.get("gdp"))["formatted"],
                "per_capita": fmt_currency_unit(row.get("gdp_per_capita")),
                "growth_real": fmt_percent(row.get("growth_real_pct"), signed=True),
                "growth_real_tone": tone_for_number(row.get("growth_real_pct")),
                "inflation": fmt_percent(row.get("inflation_pct"), signed=True),
                "inflation_tone": "neutral",
                "unemployment": fmt_percent(row.get("unemployment_pct")),
                "unemployment_tone": "neutral",
                "current_account": fmt_currency(row.get("current_account_usd")),
                "current_account_full": build_full_currency_display(row.get("current_account_usd"))["formatted"],
                "exports": fmt_currency(row.get("exports_usd")),
                "exports_full": build_full_currency_display(row.get("exports_usd"))["formatted"],
                "real_interest": fmt_percent(row.get("real_interest_pct"), signed=True),
                "real_interest_tone": tone_for_number(row.get("real_interest_pct")),
            }
            for _, row in history_frame.iterrows()
        ]

    context = {
        "cards": cards,
        "table_rows": table_rows,
        "history_rows": history_rows,
        "history_country": history_country,
        "year_options": list(reversed(year_options)),
        "selected_year": selected_year,
        "selected_metric": selected_metric,
        "metric_label": metric_definition["label"],
        "metric_options": MACRO_METRIC_DEFINITIONS,
        "selected_limit": selected_limit,
        "limit_options": LIMIT_OPTIONS,
        "selected_query": selected_query,
        "results_label": f"{len(table_rows)} de {total_matches} paises exibidos" if table_rows else "Nenhum pais encontrado",
        "coverage_label": f"{min_year} ate {max_year}",
        "source_title": source_title,
        "source_points": source_points,
        "clear_link": build_macro_dashboard_link(default_clear_year, 25, "", "gdp"),
        "empty_message": "Nenhum dado encontrado para o filtro atual." if not table_rows else "",
    }
    return render(request, "dashboard/panorama_macroeconomico.html", context)


def pib_mundial(request):
    force_refresh = request.GET.get("refresh") == "1"
    source_tab = parse_source(request.GET.get("source"))
    selected_metric = parse_metric(request.GET.get("metric"))
    selected_language = parse_language(request.GET.get("lang"))
    metric_definition = METRIC_DEFINITIONS[selected_metric]
    selected_limit = parse_limit(request.GET.get("top"))
    selected_query = request.GET.get("q", "").strip()
    query_key = normalize_search(selected_query)

    if source_tab == "fmi":
        dataframe = load_imf_weo_data(force=force_refresh)
        per_capita_dataframe = pd.DataFrame()
    else:
        dataframe = load_gdp_data(force=force_refresh)
        per_capita_dataframe = load_gdp_per_capita_data(force=force_refresh)

    if dataframe.empty:
        return render(
            request,
            "dashboard/pib_mundial.html",
            {
                "year_options": [],
                "selected_year": None,
                "cards": [],
                "bar_rows": [],
                "table_rows": [],
                "limit_options": [],
                "selected_limit": 25,
                "selected_query": "",
                "selected_metric": selected_metric,
                "selected_language": selected_language,
                "metric_options": METRIC_DEFINITIONS,
                "history_rows": [],
                "history_country": "",
                "source_tab": source_tab,
                "source_title": "FMI" if source_tab == "fmi" else "Banco Mundial",
                "source_url": "https://data.imf.org/en/datasets/IMF.RES:WEO" if source_tab == "fmi" else "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD",
                "per_capita_source_url": "https://data.imf.org/en/datasets/IMF.RES:WEO" if source_tab == "fmi" else "https://data.worldbank.org/indicator/NY.GDP.PCAP.CD",
                "source_points": [],
                "metric_label": metric_definition["label"],
                "source_version": "FMI" if source_tab == "fmi" else "Banco Mundial",
                "wb_link": build_dashboard_link("wb", request.GET.get("year", "2024"), 25, selected_query, selected_metric, selected_language),
                "fmi_link": build_dashboard_link("fmi", request.GET.get("year", "2024"), 25, selected_query, selected_metric, selected_language),
                "pt_link": build_dashboard_link(source_tab, request.GET.get("year", "2024"), 25, selected_query, selected_metric, "pt"),
                "en_link": build_dashboard_link(source_tab, request.GET.get("year", "2024"), 25, selected_query, selected_metric, "en"),
                "empty_message": "Nao foi possivel carregar os dados de PIB.",
            },
        )

    min_year = max(1960, int(dataframe["Ano"].min()))
    max_year = int(dataframe["Ano"].max())
    selected_year = parse_year(request.GET.get("year"), max_year, min_year, max_year)
    if source_tab == "fmi":
        min_year = max(1980, min_year)
        full_year = dataframe[dataframe["Ano"] == selected_year].copy()
        country_parts = full_year.apply(
            lambda row: get_country_display_parts(row["Pais"], row.get("COUNTRY.ID"), selected_language),
            axis=1,
        )
        full_year["country_display"] = country_parts.str[0]
        full_year["country_detail"] = country_parts.str[1]
        full_year["metric_value"] = full_year[metric_definition["field"]]
        full_year = full_year[full_year["metric_value"].notna()].sort_values("metric_value", ascending=False).reset_index(drop=True)
        full_year["rank"] = full_year.index + 1
        full_year["search_key"] = (
            full_year["Pais"].fillna("")
            + " "
            + full_year["country_display"].fillna("")
        ).map(normalize_search)
        current_year = full_year.copy()

        if query_key:
            current_year = current_year[current_year["search_key"].str.contains(query_key, na=False)]

        total_matches = len(current_year)
        year_frame = current_year if selected_limit == 0 else current_year.head(selected_limit).copy()
        total_gdp = full_year["gdp"].sum() if not full_year.empty else None

        leader_name = full_year.iloc[0]["country_display"] if not full_year.empty else "--"
        leader_value = format_metric_value(selected_metric, full_year.iloc[0]["metric_value"]) if not full_year.empty else "--"
        max_value = year_frame["gdp"].max() if not year_frame.empty else 0

        bar_rows = []
        for _, row in year_frame.iterrows():
            width = (row["gdp"] / max_value * 100) if max_value else 0
            width = max(width, 3) if width else 0
            bar_rows.append(
                {
                    "rank": int(row["rank"]),
                    "country": row["country_display"],
                    "value": fmt_currency(row["gdp"]),
                    "share": fmt_percent((row["gdp"] / total_gdp * 100), signed=False) if total_gdp else "--",
                    "width": f"{width:.2f}",
                }
            )

        table_rows = [
            {
                "rank": int(row["rank"]),
                "country": row["country_display"],
                "country_detail": row["country_detail"],
                "country_raw": row["Pais"],
                "gdp_display": fmt_currency(row["gdp"]),
                "gdp_full": build_full_currency_display(row["gdp"])["formatted"],
                "gdp_full_prefix": build_full_currency_display(row["gdp"])["prefix"],
                "gdp_full_number": build_full_currency_display(row["gdp"])["number"],
                "growth": fmt_percent(row.get("growth_pct"), signed=True),
                "growth_tone": tone_for_number(row.get("growth_pct")),
                "per_capita": fmt_currency_unit(row.get("gdp_per_capita")),
            }
            for _, row in year_frame.iterrows()
        ]

        history_country = ""
        history_rows = []
        if len(table_rows) == 1:
            history_country = table_rows[0]["country"]
            history_frame = dataframe[
                (dataframe["Pais"] == table_rows[0]["country_raw"])
                & (dataframe["Ano"] >= min_year)
                & (dataframe["Ano"] <= max_year)
            ].sort_values("Ano", ascending=False)

            history_rows = [
                {
                    "year": int(row["Ano"]),
                    "value_compact": fmt_currency(row["gdp"]),
                    "value_full": build_full_currency_display(row["gdp"])["formatted"],
                    "value_full_prefix": build_full_currency_display(row["gdp"])["prefix"],
                    "value_full_number": build_full_currency_display(row["gdp"])["number"],
                    "growth": fmt_percent(row.get("growth_pct"), signed=True),
                    "growth_tone": tone_for_number(row.get("growth_pct")),
                }
                for _, row in history_frame.iterrows()
            ]

        cards = [
            {"label": "Indicador ativo", "value": metric_definition["short_label"], "copy": "FMI | WEO Outubro de 2025."},
            {"label": "Lider do ranking", "value": leader_name, "copy": leader_value},
            {"label": "Paises listados", "value": str(len(table_rows)), "copy": f"De {total_matches} resultados encontrados."},
            {"label": "Cobertura", "value": f"{min_year}-{max_year}", "copy": "Historico e projecoes WEO."},
        ]
        total_countries = len(full_year)
        source_title = "FMI"
        source_url = "https://data.imf.org/en/datasets/IMF.RES:WEO"
        per_capita_source_url = "https://data.imf.org/en/datasets/IMF.RES:WEO"
        source_points = [
            'Fonte: <a href="https://data.imf.org/en/datasets/IMF.RES:WEO" target="_blank" rel="noopener noreferrer">FMI, World Economic Outlook</a>, edi&ccedil;&atilde;o de outubro de 2025.',
            "A leitura principal combina <strong>PIB nominal</strong>, <strong>crescimento anual</strong> e <strong>PIB per capita</strong> no mesmo ranking.",
            "O crescimento do PIB segue a s&eacute;rie oficial do WEO para varia&ccedil;&atilde;o real anual.",
        ]
        source_version = "FMI"
    else:
        world_bank_metadata = load_world_bank_country_metadata(force=force_refresh)
        valid_country_names = set(
            world_bank_metadata.loc[~world_bank_metadata["is_aggregate"], "Pais"].tolist()
        ) if not world_bank_metadata.empty else set()
        full_year = dataframe[
            (dataframe["Ano"] == selected_year)
            & (dataframe["Pais"] != "World")
        ].copy()
        if valid_country_names:
            full_year = full_year[full_year["Pais"].isin(valid_country_names)]
        if not world_bank_metadata.empty:
            full_year = full_year.merge(
                world_bank_metadata[["Pais", "country_code"]],
                on="Pais",
                how="left",
            )

        previous_year = dataframe[
            (dataframe["Ano"] == selected_year - 1)
            & (dataframe["Pais"] != "World")
        ][["Pais", "gdp"]].rename(columns={"gdp": "gdp_prev"})
        full_year = full_year.merge(previous_year, on="Pais", how="left")
        if not per_capita_dataframe.empty:
            per_capita_year = per_capita_dataframe[
                (per_capita_dataframe["Ano"] == selected_year)
                & (per_capita_dataframe["Pais"] != "World")
            ][["Pais", "gdp_per_capita"]]
            full_year = full_year.merge(per_capita_year, on="Pais", how="left")
        else:
            full_year["gdp_per_capita"] = None

        full_year["growth_pct"] = (
            (full_year["gdp"] - full_year["gdp_prev"]) / full_year["gdp_prev"] * 100
        )
        country_parts = full_year.apply(
            lambda row: get_country_display_parts(row["Pais"], row.get("country_code"), selected_language),
            axis=1,
        )
        full_year["country_display"] = country_parts.str[0]
        full_year["country_detail"] = country_parts.str[1]
        full_year["metric_value"] = full_year[metric_definition["field"]]
        full_year = full_year[full_year["metric_value"].notna()].sort_values("metric_value", ascending=False).reset_index(drop=True)
        full_year["rank"] = full_year.index + 1
        full_year["search_key"] = (
            full_year["Pais"].fillna("")
            + " "
            + full_year["country_display"].fillna("")
        ).map(normalize_search)

        if query_key:
            current_year = full_year[full_year["search_key"].str.contains(query_key, na=False)].copy()
        else:
            current_year = full_year.copy()

        total_matches = len(current_year)
        year_frame = current_year if selected_limit == 0 else current_year.head(selected_limit).copy()

        world_frame = dataframe[(dataframe["Pais"] == "World") & (dataframe["Ano"] == selected_year)]
        world_value = world_frame["gdp"].iloc[0] if not world_frame.empty else full_year["gdp"].sum()

        leader_name = full_year.iloc[0]["country_display"] if not full_year.empty else "--"
        leader_value = format_metric_value(selected_metric, full_year.iloc[0]["metric_value"]) if not full_year.empty else "--"
        max_value = year_frame["gdp"].max() if not year_frame.empty else 0

        bar_rows = []
        for _, row in year_frame.iterrows():
            width = (row["gdp"] / max_value * 100) if max_value else 0
            width = max(width, 3) if width else 0
            bar_rows.append(
                {
                    "rank": int(row["rank"]),
                    "country": row["country_display"],
                    "value": fmt_currency(row["gdp"]),
                    "share": fmt_percent((row["gdp"] / world_value * 100), signed=False) if world_value else "--",
                    "width": f"{width:.2f}",
                    "growth_pct": row["growth_pct"],
                }
            )

        table_rows = [
            {
                "rank": int(row["rank"]),
                "country": row["country_display"],
                "country_detail": row["country_detail"],
                "country_raw": row["Pais"],
                "gdp_display": fmt_currency(row["gdp"]),
                "gdp_full": build_full_currency_display(row["gdp"])["formatted"],
                "gdp_full_prefix": build_full_currency_display(row["gdp"])["prefix"],
                "gdp_full_number": build_full_currency_display(row["gdp"])["number"],
                "growth": fmt_percent(row["growth_pct"], signed=True),
                "growth_tone": tone_for_number(row["growth_pct"]),
                "per_capita": fmt_currency_unit(row["gdp_per_capita"]),
            }
            for _, row in year_frame.iterrows()
        ]

        history_country = ""
        history_rows = []
        if len(table_rows) == 1:
            history_country = table_rows[0]["country"]
            history_frame = dataframe[
                (dataframe["Pais"] == table_rows[0]["country_raw"])
                & (dataframe["Ano"] >= min_year)
                & (dataframe["Ano"] <= max_year)
            ].sort_values("Ano").copy()
            history_frame["gdp_prev"] = history_frame["gdp"].shift(1)
            history_frame["growth_pct"] = (
                (history_frame["gdp"] - history_frame["gdp_prev"]) / history_frame["gdp_prev"] * 100
            )

            history_rows = [
                {
                    "year": int(row["Ano"]),
                    "value_compact": fmt_currency(row["gdp"]),
                    "value_full": build_full_currency_display(row["gdp"])["formatted"],
                    "value_full_prefix": build_full_currency_display(row["gdp"])["prefix"],
                    "value_full_number": build_full_currency_display(row["gdp"])["number"],
                    "growth": fmt_percent(row["growth_pct"], signed=True),
                    "growth_tone": tone_for_number(row["growth_pct"]),
                }
                for _, row in history_frame.sort_values("Ano", ascending=False).iterrows()
            ]

        cards = [
            {"label": "Indicador ativo", "value": metric_definition["short_label"], "copy": f"Banco Mundial | ano {selected_year}."},
            {"label": "Lider do ranking", "value": leader_name, "copy": leader_value},
            {"label": "Paises listados", "value": str(len(table_rows)), "copy": f"De {total_matches} resultados encontrados."},
            {"label": "Cobertura", "value": f"{min_year}-{max_year}", "copy": "Serie historica nominal disponivel."},
        ]
        total_countries = len(full_year)
        source_title = "Banco Mundial"
        source_url = "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD"
        per_capita_source_url = "https://data.worldbank.org/indicator/NY.GDP.PCAP.CD"
        source_points = [
            'Fonte: <a href="https://data.worldbank.org/indicator/NY.GDP.MKTP.CD" target="_blank" rel="noopener noreferrer">Banco Mundial</a>, com s&eacute;rie hist&oacute;rica de PIB nominal em d&oacute;lares correntes.',
            "A tabela re&uacute;ne <strong>PIB nominal</strong>, <strong>valor total</strong>, <strong>crescimento anual</strong> e <strong>PIB per capita</strong> em uma leitura direta.",
            'O PIB per capita usa o indicador <a href="https://data.worldbank.org/indicator/NY.GDP.PCAP.CD" target="_blank" rel="noopener noreferrer">NY.GDP.PCAP.CD</a>.',
        ]
        source_version = "Banco Mundial"

    context = {
        "year_options": list(range(max_year, min_year - 1, -1)),
        "selected_year": selected_year,
        "selected_limit": selected_limit,
        "limit_options": LIMIT_OPTIONS,
        "selected_query": selected_query,
        "selected_metric": selected_metric,
        "selected_language": selected_language,
        "metric_options": METRIC_DEFINITIONS,
        "cards": cards,
        "bar_rows": bar_rows[:10],
        "table_rows": table_rows,
        "history_rows": history_rows,
        "history_country": history_country,
        "coverage_label": f"{min_year} ate {max_year}",
        "results_label": f"{len(table_rows)} de {total_matches} paises exibidos" if table_rows else "Nenhum pais encontrado",
        "total_countries": total_countries,
        "source_url": source_url,
        "per_capita_source_url": per_capita_source_url,
        "source_tab": source_tab,
        "source_title": source_title,
        "source_points": source_points,
        "source_version": source_version,
        "metric_label": metric_definition["label"],
        "wb_link": build_dashboard_link("wb", selected_year, selected_limit, selected_query, selected_metric, selected_language),
        "fmi_link": build_dashboard_link("fmi", selected_year, selected_limit, selected_query, selected_metric, selected_language),
        "pt_link": build_dashboard_link(source_tab, selected_year, selected_limit, selected_query, selected_metric, "pt"),
        "en_link": build_dashboard_link(source_tab, selected_year, selected_limit, selected_query, selected_metric, "en"),
        "empty_message": "Nenhum dado encontrado para o filtro atual." if not table_rows else "",
    }
    return render(request, "dashboard/pib_mundial.html", context)


# ---- Impostômetro ----

def fetch_impostometro_live():
    """Try to fetch live accumulated value from ACSP impostômetro API."""
    import datetime as _dt
    global IMPOSTOMETRO_CACHE, IMPOSTOMETRO_CACHE_TS
    now = _dt.datetime.now()
    if (
        IMPOSTOMETRO_CACHE is not None
        and IMPOSTOMETRO_CACHE_TS is not None
        and (now - IMPOSTOMETRO_CACHE_TS).total_seconds() < 300
    ):
        return IMPOSTOMETRO_CACHE
    try:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Referer": "https://impostometro.com.br/",
        }
        response = requests.post(IMPOSTOMETRO_API_URL, json={}, headers=headers, timeout=8)
        response.raise_for_status()
        data = response.json()
        accumulated = (
            data.get("Acumulado")
            or data.get("acumulado")
            or data.get("ACUMULADO")
        )
        if accumulated is not None and float(accumulated) > 0:
            value = float(accumulated)
            IMPOSTOMETRO_CACHE = value
            IMPOSTOMETRO_CACHE_TS = now
            return value
    except Exception:
        pass
    return None


def compute_impostometro_ytd(year_total, year):
    """Compute accumulated tax from Jan 1 of year to now, proportional to annual total."""
    import datetime as _dt
    now = _dt.datetime.now()
    year_start = _dt.datetime(year, 1, 1)
    year_end = _dt.datetime(year + 1, 1, 1)
    year_seconds = (year_end - year_start).total_seconds()
    elapsed = max(0.0, (now - year_start).total_seconds())
    return year_total * (elapsed / year_seconds)


def build_impostometro_history_chart(years, values, width=680, height=270):
    """Build SVG-compatible bar+line chart data for historical annual arrecadação."""
    left_padding, right_padding, top_padding, bottom_padding = 64, 18, 20, 40
    plot_width = width - left_padding - right_padding
    plot_height = height - top_padding - bottom_padding

    if not values:
        return {"width": width, "height": height, "grid_lines": [], "x_labels": [], "bars": [], "points": "", "dots": []}

    min_val = 0.0
    max_val = max(float(v) for v in values) * 1.08

    def x_pos(i):
        if len(years) <= 1:
            return left_padding + plot_width / 2
        return left_padding + (plot_width / (len(years) - 1)) * i

    def y_pos(v):
        ratio = (float(v) - min_val) / (max_val - min_val) if max_val > min_val else 0
        return top_padding + plot_height - ratio * plot_height

    grid_lines = []
    for step in range(5):
        tick_v = min_val + (max_val - min_val) * step / 4
        grid_lines.append({"y": round(y_pos(tick_v), 1), "label": fmt_brl_compact(tick_v)})
    grid_lines.sort(key=lambda g: g["y"])

    x_labels = [{"x": round(x_pos(i), 1), "label": str(y)} for i, y in enumerate(years)]

    bar_width = max(6.0, (plot_width / len(years)) * 0.64) if years else 10.0
    floor_y = y_pos(min_val)
    bars = []
    points_list = []
    dots = []
    for i, (yr, val) in enumerate(zip(years, values)):
        x = x_pos(i)
        y = y_pos(val)
        bars.append({
            "x": round(x - bar_width / 2, 1),
            "y": round(y, 1),
            "width": round(bar_width, 1),
            "height": round(floor_y - y, 1),
            "year": yr,
            "value_display": fmt_brl_compact(val),
        })
        points_list.append(f"{x:.1f},{y:.1f}")
        dots.append({
            "x": round(x, 1),
            "y": round(y, 1),
            "year": yr,
            "value_display": fmt_brl_compact(val),
        })

    return {
        "width": width,
        "height": height,
        "grid_lines": grid_lines,
        "x_labels": x_labels,
        "bars": bars,
        "points": " ".join(points_list),
        "dots": dots,
    }


def impostometro(request):
    import datetime as _dt
    now = _dt.datetime.now()
    current_year = now.year
    year_total = FEDERAL_ARRECADACAO_ANUAL_BRL.get(current_year) or FEDERAL_ARRECADACAO_ANUAL_BRL.get(current_year - 1, 0)
    year_start = _dt.datetime(current_year, 1, 1)
    year_end = _dt.datetime(current_year + 1, 1, 1)
    seconds_per_year = (year_end - year_start).total_seconds()
    rate_per_second = year_total / seconds_per_year if seconds_per_year else 0

    live_value = fetch_impostometro_live()
    if live_value is not None:
        accumulated = live_value
        data_source = "acsp"
        source_label = "ACSP – Impost\u00f4metro"
        source_url = "https://impostometro.com.br/"
    else:
        accumulated = compute_impostometro_ytd(year_total, current_year)
        data_source = "rfb"
        source_label = "Receita Federal do Brasil"
        source_url = "https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/receitadata/arrecadacao"

    server_timestamp_ms = int(now.timestamp() * 1000)

    chart_years = sorted(FEDERAL_ARRECADACAO_ANUAL_BRL.keys())
    chart_values = [FEDERAL_ARRECADACAO_ANUAL_BRL[y] for y in chart_years]
    history_chart = build_impostometro_history_chart(chart_years, chart_values, width=680, height=270)

    prev_year = current_year - 1
    prev_total = FEDERAL_ARRECADACAO_ANUAL_BRL.get(prev_year)
    yoy_pct = ((year_total - prev_total) / prev_total * 100) if prev_total else None

    day_of_year = now.timetuple().tm_yday
    ytd_days = max(1, day_of_year)
    daily_avg = accumulated / ytd_days
    rate_per_minute = rate_per_second * 60
    rate_per_hour = rate_per_second * 3600

    elapsed_fraction = (now - year_start).total_seconds() / seconds_per_year if seconds_per_year else 1
    projected_year_total = (accumulated / elapsed_fraction) if elapsed_fraction > 0 else year_total

    source_points = [
        f'Fonte: <a href="{source_url}" target="_blank" rel="noopener noreferrer">{source_label}</a>.',
        "A arrecada\u00e7\u00e3o federal inclui impostos e contribui\u00e7\u00f5es administrados pela Receita Federal do Brasil (RFB) e INSS, abrangendo IR, IPI, IOF, CSLL, PIS/COFINS e demais tributos federais.",
        "O contador em tempo real \u00e9 calculado com base na taxa m\u00e9dia di\u00e1ria do exerc\u00edcio atual. Valores do ano corrente incluem proje\u00e7\u00e3o.",
        "Acumulado desde 1\u00b0 de janeiro do ano corrente.",
    ]

    context = {
        "current_year": current_year,
        "accumulated": accumulated,
        "rate_per_second": rate_per_second,
        "rate_per_minute": rate_per_minute,
        "rate_per_hour": rate_per_hour,
        "daily_avg": daily_avg,
        "year_total": year_total,
        "projected_year_total": projected_year_total,
        "yoy_pct": yoy_pct,
        "server_timestamp_ms": server_timestamp_ms,
        "history_chart": history_chart,
        "data_source": data_source,
        "source_label": source_label,
        "source_url": source_url,
        "source_points": source_points,
        "rate_per_second_display": fmt_brl_full(rate_per_second),
        "rate_per_minute_display": fmt_brl_full(rate_per_minute),
        "rate_per_hour_display": fmt_brl_full(rate_per_hour),
        "daily_avg_display": fmt_brl_compact(daily_avg),
        "year_total_display": fmt_brl_compact(year_total),
        "projected_year_total_display": fmt_brl_compact(projected_year_total),
        "yoy_pct_display": fmt_percent(yoy_pct, signed=True) if yoy_pct is not None else "--",
        "ytd_days": ytd_days,
    }
    return render(request, "dashboard/impostometro.html", context)


def build_impostometro_api_headers():
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": IMPOSTOMETRO_SOURCE_URL,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }


def normalize_impostometro_date(value):
    import datetime as _dt

    if isinstance(value, _dt.datetime):
        value = value.date()
    if isinstance(value, _dt.date):
        return value.strftime("%d/%m/%Y")
    return str(value)


def build_impostometro_range_cache_key(start_str, end_str):
    return f"{start_str}|{end_str}"


def get_impostometro_cache_file_path():
    return os.path.join(os.getcwd(), IMPOSTOMETRO_PERSISTED_CACHE_FILE)


def load_impostometro_persisted_cache():
    global IMPOSTOMETRO_PERSISTED_CACHE

    if IMPOSTOMETRO_PERSISTED_CACHE is not None:
        return IMPOSTOMETRO_PERSISTED_CACHE

    cache_data = {"ranges": {}}
    cache_path = get_impostometro_cache_file_path()
    try:
        with open(cache_path, "r", encoding="utf-8") as cache_file:
            loaded = json.load(cache_file)
        if isinstance(loaded, dict) and isinstance(loaded.get("ranges"), dict):
            cache_data["ranges"] = loaded["ranges"]
    except (OSError, ValueError, TypeError):
        pass

    IMPOSTOMETRO_PERSISTED_CACHE = cache_data
    return cache_data


def save_impostometro_persisted_cache(cache_data):
    global IMPOSTOMETRO_PERSISTED_CACHE

    cache_path = get_impostometro_cache_file_path()
    try:
        with open(cache_path, "w", encoding="utf-8") as cache_file:
            json.dump(cache_data, cache_file, ensure_ascii=False, indent=2, sort_keys=True)
        IMPOSTOMETRO_PERSISTED_CACHE = cache_data
    except OSError:
        IMPOSTOMETRO_PERSISTED_CACHE = cache_data


def get_impostometro_persisted_range(start_str, end_str):
    persisted_cache = load_impostometro_persisted_cache()
    cache_key = build_impostometro_range_cache_key(start_str, end_str)
    entry = persisted_cache.get("ranges", {}).get(cache_key)
    return dict(entry) if isinstance(entry, dict) else None


def persist_impostometro_range(result, fetched_at):
    persisted_cache = load_impostometro_persisted_cache()
    persisted_ranges = persisted_cache.setdefault("ranges", {})
    persisted_result = dict(result)
    persisted_result["fetched_at"] = fetched_at.isoformat()
    cache_key = build_impostometro_range_cache_key(result["start_date"], result["end_date"])
    persisted_ranges[cache_key] = persisted_result
    save_impostometro_persisted_cache(persisted_cache)


def parse_impostometro_timestamp(value):
    import datetime as _dt

    if not value:
        return None

    normalized = str(value).strip()
    if "." in normalized:
        head, tail = normalized.split(".", 1)
        digits = "".join(ch for ch in tail if ch.isdigit())[:6]
        normalized = f"{head}.{digits}" if digits else head

    try:
        return _dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None


def get_latest_impostometro_persisted_snapshot(year, max_end_date=None):
    import datetime as _dt

    persisted_ranges = load_impostometro_persisted_cache().get("ranges", {})
    latest_entry = None
    latest_end_date = None

    for entry in persisted_ranges.values():
        if not isinstance(entry, dict):
            continue
        if entry.get("start_date") != f"01/01/{year}":
            continue

        try:
            entry_end_date = _dt.datetime.strptime(entry.get("end_date", ""), "%d/%m/%Y").date()
        except ValueError:
            continue

        if max_end_date is not None and entry_end_date > max_end_date:
            continue
        if latest_end_date is None or entry_end_date > latest_end_date:
            latest_entry = dict(entry)
            latest_end_date = entry_end_date

    return latest_entry


def build_impostometro_live_fallback_from_snapshot(snapshot, now=None):
    import datetime as _dt

    if snapshot is None:
        return None

    now = now or _dt.datetime.now()
    snapshot_value = float(snapshot.get("value") or 0.0)
    snapshot_increment = float(snapshot.get("increment") or 0.0)
    snapshot_timestamp = parse_impostometro_timestamp(snapshot.get("data")) or parse_impostometro_timestamp(snapshot.get("fetched_at"))
    if snapshot_timestamp is None:
        return None

    elapsed_seconds = max(0.0, (now - snapshot_timestamp).total_seconds())
    derived = dict(snapshot)
    derived["value"] = snapshot_value + (snapshot_increment * elapsed_seconds)
    derived["derived_from_snapshot"] = True
    return derived


def fmt_integer_br(value):
    if value is None:
        return "--"
    return f"{int(value):,}".replace(",", ".")


def build_impostometro_counter_groups(value):
    absolute_text = f"{abs(float(value or 0.0)):.2f}"
    integer_text, cents_text = absolute_text.split(".")

    integer_groups = []
    while integer_text:
        integer_groups.insert(0, integer_text[-3:])
        integer_text = integer_text[:-3]
    if not integer_groups:
        integer_groups = ["0"]

    group_count = len(integer_groups) + 1
    labels_by_size = {
        2: (["Real", "Centavo"], ["Reais", "Centavos"]),
        3: (["Mil", "Real", "Centavo"], ["Mil", "Reais", "Centavos"]),
        4: (["Milhão", "Mil", "Real", "Centavo"], ["Milhões", "Mil", "Reais", "Centavos"]),
        5: (["Bilhão", "Milhão", "Mil", "Real", "Centavo"], ["Bilhões", "Milhões", "Mil", "Reais", "Centavos"]),
        6: (["Trilhão", "Bilhão", "Milhão", "Mil", "Real", "Centavo"], ["Trilhões", "Bilhões", "Milhões", "Mil", "Reais", "Centavos"]),
    }
    singular_labels, plural_labels = labels_by_size.get(group_count, labels_by_size[6])

    groups = []
    for group_text, singular_label, plural_label in zip(integer_groups + [cents_text], singular_labels, plural_labels):
        label = singular_label if int(group_text or "0") == 1 else plural_label
        groups.append({"digits": list(group_text), "label": label})
    return groups


def fetch_impostometro_range(start_date, end_date, cache_ttl_seconds=IMPOSTOMETRO_LIVE_CACHE_TTL_SECONDS):
    import datetime as _dt

    global IMPOSTOMETRO_RANGE_CACHE, IMPOSTOMETRO_RANGE_CACHE_TS

    start_str = normalize_impostometro_date(start_date)
    end_str = normalize_impostometro_date(end_date)
    cache_key = (start_str, end_str)
    now = _dt.datetime.now()

    cached = IMPOSTOMETRO_RANGE_CACHE.get(cache_key)
    cached_at = IMPOSTOMETRO_RANGE_CACHE_TS.get(cache_key)
    if (
        cached is not None
        and cached_at is not None
        and (now - cached_at).total_seconds() < cache_ttl_seconds
    ):
        return cached

    try:
        response = requests.get(
            IMPOSTOMETRO_API_URL,
            params={"dataInicial": start_str, "dataFinal": end_str},
            headers=build_impostometro_api_headers(),
            timeout=8,
        )
        response.raise_for_status()
        data = response.json()
        result = {
            "start_date": start_str,
            "end_date": end_str,
            "value": float(data.get("Valor") or 0.0),
            "increment": float(data.get("Incremento") or 0.0),
            "data": data.get("Data") or "",
            "midnight": data.get("Midnight") or "",
        }
        IMPOSTOMETRO_RANGE_CACHE[cache_key] = result
        IMPOSTOMETRO_RANGE_CACHE_TS[cache_key] = now
        persist_impostometro_range(result, now)
        return result
    except Exception:
        persisted = get_impostometro_persisted_range(start_str, end_str)
        if persisted is not None:
            IMPOSTOMETRO_RANGE_CACHE[cache_key] = persisted
            IMPOSTOMETRO_RANGE_CACHE_TS[cache_key] = now
            return persisted
        return None


def fetch_impostometro_live():
    import datetime as _dt

    today = _dt.date.today()
    live_result = fetch_impostometro_range(
        _dt.date(today.year, 1, 1),
        today,
        cache_ttl_seconds=IMPOSTOMETRO_LIVE_CACHE_TTL_SECONDS,
    )
    if live_result is not None:
        return live_result

    latest_snapshot = get_latest_impostometro_persisted_snapshot(today.year, max_end_date=today)
    return build_impostometro_live_fallback_from_snapshot(latest_snapshot)


def fetch_impostometro_year_total(year):
    import datetime as _dt

    year_total = fetch_impostometro_range(
        _dt.date(year, 1, 1),
        _dt.date(year, 12, 31),
        cache_ttl_seconds=IMPOSTOMETRO_HISTORY_CACHE_TTL_SECONDS,
    )
    if year_total is not None:
        return year_total

    seeded_total = IMPOSTOMETRO_ANNUAL_SEED_BRL.get(year)
    if seeded_total is not None:
        return {
            "start_date": f"01/01/{year}",
            "end_date": f"31/12/{year}",
            "value": float(seeded_total),
            "increment": 0.0,
            "data": "",
            "midnight": "",
            "seeded": True,
        }
    return None


def impostometro(request):
    import datetime as _dt

    now = _dt.datetime.now()
    today = now.date()
    current_year = now.year
    year_start = _dt.datetime(current_year, 1, 1)
    year_end = _dt.datetime(current_year + 1, 1, 1)
    seconds_per_year = (year_end - year_start).total_seconds()
    elapsed_fraction = (now - year_start).total_seconds() / seconds_per_year if seconds_per_year else 0.0

    live_data = fetch_impostometro_live()
    previous_year_data = fetch_impostometro_year_total(current_year - 1)
    previous_year_total = previous_year_data["value"] if previous_year_data else None

    if live_data is not None:
        accumulated = live_data["value"]
        rate_per_second = live_data["increment"]
        data_source = "disk_snapshot" if live_data.get("derived_from_snapshot") else "acsp"
        source_label = (
            "Snapshot local do Impostometro"
            if live_data.get("derived_from_snapshot")
            else "ACSP / IBPT - Impostometro"
        )
        source_url = IMPOSTOMETRO_SOURCE_URL
    else:
        baseline_total = previous_year_total or 0.0
        accumulated = baseline_total * elapsed_fraction
        rate_per_second = (baseline_total / seconds_per_year) if seconds_per_year and baseline_total else 0.0
        data_source = "fallback"
        source_label = "Estimativa local com base no ultimo ano fechado"
        source_url = IMPOSTOMETRO_METHODOLOGY_URL

    rate_per_minute = rate_per_second * 60
    rate_per_hour = rate_per_second * 3600
    day_of_year = now.timetuple().tm_yday
    ytd_days = max(1, day_of_year)
    daily_avg = (accumulated / ytd_days) if accumulated else 0.0
    projected_year_total = (accumulated / elapsed_fraction) if elapsed_fraction > 0 else accumulated
    basic_basket_reference_brl = 435.0
    basic_basket_count = max(0, int(accumulated / basic_basket_reference_brl))
    yoy_pct = (
        (projected_year_total - previous_year_total) / previous_year_total * 100
        if previous_year_total
        else None
    )

    history_start_year = max(2005, current_year - IMPOSTOMETRO_HISTORY_YEARS)
    history_points = []
    for year in range(history_start_year, current_year):
        year_data = fetch_impostometro_year_total(year)
        if year_data is not None and year_data["value"] > 0:
            history_points.append((year, year_data["value"]))

    if projected_year_total > 0:
        history_points.append((current_year, projected_year_total))

    chart_years = [year for year, _ in history_points]
    chart_values = [value for _, value in history_points]
    history_chart = build_impostometro_history_chart(chart_years, chart_values, width=680, height=270)

    source_points = [
        f'Fonte principal: <a href="{IMPOSTOMETRO_SOURCE_URL}" target="_blank" rel="noopener noreferrer">Impostometro</a>.',
        (
            f'Metodologia oficial: <a href="{IMPOSTOMETRO_METHODOLOGY_URL}" '
            'target="_blank" rel="noopener noreferrer">ACSP / IBPT</a>.'
        ),
        "O total do Brasil considera a soma dos tributos das esferas federal, estadual e municipal.",
        "O contador usa o mesmo endpoint JSON chamado pela interface oficial do site para o periodo de 1 de janeiro ate hoje.",
        "Quando a API falha, o sistema tenta reutilizar snapshots persistidos localmente e, na ausencia deles, usa totais anuais semeados para manter a projeção.",
        "A serie historica usa totais anuais fechados da mesma API; o ano atual aparece como projecao anualizada.",
    ]

    context = {
        "current_year": current_year,
        "accumulated": accumulated,
        "rate_per_second": rate_per_second,
        "rate_per_minute": rate_per_minute,
        "rate_per_hour": rate_per_hour,
        "daily_avg": daily_avg,
        "previous_year_total": previous_year_total,
        "projected_year_total": projected_year_total,
        "yoy_pct": yoy_pct,
        "server_timestamp_ms": int(now.timestamp() * 1000),
        "history_chart": history_chart,
        "data_source": data_source,
        "source_label": source_label,
        "source_url": source_url,
        "source_points": source_points,
        "rate_per_second_display": fmt_brl_full(rate_per_second),
        "rate_per_minute_display": fmt_brl_full(rate_per_minute),
        "rate_per_hour_display": fmt_brl_full(rate_per_hour),
        "daily_avg_display": fmt_brl_compact(daily_avg),
        "previous_year_total_display": fmt_brl_compact(previous_year_total),
        "projected_year_total_display": fmt_brl_compact(projected_year_total),
        "yoy_pct_display": fmt_percent(yoy_pct, signed=True) if yoy_pct is not None else "--",
        "ytd_days": ytd_days,
        "history_start_year": chart_years[0] if chart_years else current_year,
        "history_last_full_year": current_year - 1,
        "period_start_display": year_start.strftime("%d/%m/%Y"),
        "period_end_display": today.strftime("%d/%m/%Y"),
        "basic_basket_reference_brl": basic_basket_reference_brl,
        "basic_basket_count_display": fmt_integer_br(basic_basket_count),
        "counter_groups": build_impostometro_counter_groups(accumulated),
    }
    return render(request, "dashboard/impostometro.html", context)
