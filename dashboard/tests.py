import pandas as pd
from django.test import SimpleTestCase
from unittest.mock import patch


def build_hdi_frame():
    return pd.DataFrame(
        [
            {
                "rank": 1,
                "country": "Iceland",
                "hdi": 0.972,
                "life_expectancy": 82.691,
                "expected_schooling": 18.8506,
                "mean_schooling": 13.9089,
                "gni_per_capita": 69116.93,
                "gni_rank_gap": 12,
                "hdi_rank_2022": 3,
                "development_group": "Very high human development",
                "report_year": 2023,
            },
            {
                "rank": 48,
                "country": "Uruguay",
                "hdi": 0.830,
                "life_expectancy": 78.240,
                "expected_schooling": 17.4400,
                "mean_schooling": 9.2400,
                "gni_per_capita": 30690.00,
                "gni_rank_gap": 4,
                "hdi_rank_2022": 49,
                "development_group": "High human development",
                "report_year": 2023,
            },
            {
                "rank": 160,
                "country": "Pakistan",
                "hdi": 0.544,
                "life_expectancy": 66.100,
                "expected_schooling": 8.3400,
                "mean_schooling": 4.9000,
                "gni_per_capita": 6240.00,
                "gni_rank_gap": -2,
                "hdi_rank_2022": 161,
                "development_group": "Medium human development",
                "report_year": 2023,
            },
        ]
    )


def build_hdi_history_frame():
    return pd.DataFrame(
        [
            {
                "country": "Iceland",
                "hdi_1990": 0.799,
                "hdi_2023": 0.972,
                "le_1990": 78.0,
                "le_2023": 82.7,
                "eys_1990": 15.0,
                "eys_2023": 18.9,
                "mys_1990": 9.9,
                "mys_2023": 13.9,
                "gnipc_1990": 32000,
                "gnipc_2023": 69116.93,
            },
            {
                "country": "Uruguay",
                "hdi_1990": 0.692,
                "hdi_2023": 0.830,
                "le_1990": 72.1,
                "le_2023": 78.2,
                "eys_1990": 13.2,
                "eys_2023": 17.4,
                "mys_1990": 7.8,
                "mys_2023": 9.2,
                "gnipc_1990": 11800,
                "gnipc_2023": 30690.00,
            },
            {
                "country": "Pakistan",
                "hdi_1990": 0.405,
                "hdi_2023": 0.544,
                "le_1990": 59.1,
                "le_2023": 66.1,
                "eys_1990": 4.2,
                "eys_2023": 8.3,
                "mys_1990": 2.2,
                "mys_2023": 4.9,
                "gnipc_1990": 2700,
                "gnipc_2023": 6240.00,
            },
        ]
    )


def build_macro_gdp_frame():
    return pd.DataFrame(
        [
            {"Pais": "World", "Ano": 2022, "gdp": 101_000_000_000_000},
            {"Pais": "World", "Ano": 2023, "gdp": 105_000_000_000_000},
            {"Pais": "United States", "Ano": 2022, "gdp": 26_000_000_000_000},
            {"Pais": "United States", "Ano": 2023, "gdp": 28_000_000_000_000},
            {"Pais": "India", "Ano": 2022, "gdp": 3_180_000_000_000},
            {"Pais": "India", "Ano": 2023, "gdp": 3_570_000_000_000},
            {"Pais": "Brazil", "Ano": 2022, "gdp": 1_920_000_000_000},
            {"Pais": "Brazil", "Ano": 2023, "gdp": 2_170_000_000_000},
        ]
    )


def build_macro_per_capita_frame():
    return pd.DataFrame(
        [
            {"Pais": "United States", "Ano": 2022, "gdp_per_capita": 79_500},
            {"Pais": "United States", "Ano": 2023, "gdp_per_capita": 83_100},
            {"Pais": "India", "Ano": 2022, "gdp_per_capita": 2_390},
            {"Pais": "India", "Ano": 2023, "gdp_per_capita": 2_480},
            {"Pais": "Brazil", "Ano": 2022, "gdp_per_capita": 8_950},
            {"Pais": "Brazil", "Ano": 2023, "gdp_per_capita": 10_200},
        ]
    )


def build_macro_growth_frame():
    return pd.DataFrame(
        [
            {"Pais": "United States", "Ano": 2022, "growth_real_pct": 1.9},
            {"Pais": "United States", "Ano": 2023, "growth_real_pct": 2.8},
            {"Pais": "India", "Ano": 2022, "growth_real_pct": 7.2},
            {"Pais": "India", "Ano": 2023, "growth_real_pct": 7.8},
            {"Pais": "Brazil", "Ano": 2022, "growth_real_pct": 3.0},
            {"Pais": "Brazil", "Ano": 2023, "growth_real_pct": 2.9},
        ]
    )


def build_macro_inflation_frame():
    return pd.DataFrame(
        [
            {"Pais": "United States", "Ano": 2022, "inflation_pct": 8.0},
            {"Pais": "United States", "Ano": 2023, "inflation_pct": 4.1},
            {"Pais": "India", "Ano": 2022, "inflation_pct": 6.7},
            {"Pais": "India", "Ano": 2023, "inflation_pct": 5.7},
            {"Pais": "Brazil", "Ano": 2022, "inflation_pct": 9.3},
            {"Pais": "Brazil", "Ano": 2023, "inflation_pct": 4.6},
        ]
    )


def build_macro_unemployment_frame():
    return pd.DataFrame(
        [
            {"Pais": "United States", "Ano": 2022, "unemployment_pct": 3.8},
            {"Pais": "United States", "Ano": 2023, "unemployment_pct": 3.6},
            {"Pais": "United States", "Ano": 2025, "unemployment_pct": 4.0},
            {"Pais": "India", "Ano": 2022, "unemployment_pct": 4.5},
            {"Pais": "India", "Ano": 2023, "unemployment_pct": 4.2},
            {"Pais": "India", "Ano": 2025, "unemployment_pct": 4.4},
            {"Pais": "Brazil", "Ano": 2022, "unemployment_pct": 9.3},
            {"Pais": "Brazil", "Ano": 2023, "unemployment_pct": 7.8},
            {"Pais": "Brazil", "Ano": 2025, "unemployment_pct": 7.5},
        ]
    )


def build_macro_current_account_frame():
    return pd.DataFrame(
        [
            {"Pais": "United States", "Ano": 2022, "current_account_usd": -950_000_000_000},
            {"Pais": "United States", "Ano": 2023, "current_account_usd": -820_000_000_000},
            {"Pais": "India", "Ano": 2022, "current_account_usd": -67_000_000_000},
            {"Pais": "India", "Ano": 2023, "current_account_usd": -23_000_000_000},
            {"Pais": "Brazil", "Ano": 2022, "current_account_usd": -56_000_000_000},
            {"Pais": "Brazil", "Ano": 2023, "current_account_usd": -31_000_000_000},
        ]
    )


def build_macro_exports_frame():
    return pd.DataFrame(
        [
            {"Pais": "United States", "Ano": 2022, "exports_usd": 3_050_000_000_000},
            {"Pais": "United States", "Ano": 2023, "exports_usd": 3_180_000_000_000},
            {"Pais": "India", "Ano": 2022, "exports_usd": 770_000_000_000},
            {"Pais": "India", "Ano": 2023, "exports_usd": 820_000_000_000},
            {"Pais": "Brazil", "Ano": 2022, "exports_usd": 335_000_000_000},
            {"Pais": "Brazil", "Ano": 2023, "exports_usd": 360_000_000_000},
        ]
    )


def build_macro_real_interest_frame():
    return pd.DataFrame(
        [
            {"Pais": "United States", "Ano": 2022, "real_interest_pct": 1.5},
            {"Pais": "United States", "Ano": 2023, "real_interest_pct": 2.1},
            {"Pais": "India", "Ano": 2022, "real_interest_pct": 3.2},
            {"Pais": "India", "Ano": 2023, "real_interest_pct": 3.5},
            {"Pais": "Brazil", "Ano": 2022, "real_interest_pct": 6.4},
            {"Pais": "Brazil", "Ano": 2023, "real_interest_pct": 5.8},
        ]
    )


def build_macro_metadata_frame():
    return pd.DataFrame(
        [
            {"country_code": "WLD", "Pais": "World", "region": "Aggregates", "is_aggregate": True},
            {"country_code": "USA", "Pais": "United States", "region": "North America", "is_aggregate": False},
            {"country_code": "IND", "Pais": "India", "region": "South Asia", "is_aggregate": False},
            {"country_code": "BRA", "Pais": "Brazil", "region": "Latin America & Caribbean", "is_aggregate": False},
        ]
    )


def empty_country_locale_frame():
    return pd.DataFrame(columns=["country_code", "name_en", "name_pt", "name_en_key"])


def build_currency_catalog_frame():
    return pd.DataFrame(
        [
            {"code": "BRL", "name": "Brazilian Real", "symbol": "R$"},
            {"code": "USD", "name": "US Dollar", "symbol": "$"},
            {"code": "EUR", "name": "Euro", "symbol": "€"},
            {"code": "GBP", "name": "Pound Sterling", "symbol": "£"},
            {"code": "JPY", "name": "Japanese Yen", "symbol": "¥"},
        ]
    )


def build_currency_rates_frame():
    return pd.DataFrame(
        [
            {"date": "2026-03-27", "base": "BRL", "quote": "USD", "rate": 0.1765},
            {"date": "2026-03-27", "base": "BRL", "quote": "EUR", "rate": 0.1632},
            {"date": "2026-03-27", "base": "BRL", "quote": "GBP", "rate": 0.1398},
            {"date": "2026-03-27", "base": "BRL", "quote": "JPY", "rate": 26.5100},
        ]
    )


def build_crypto_markets_frame():
    return pd.DataFrame(
        [
            {"id": "bitcoin", "symbol": "BTC", "name": "Bitcoin", "current_price_usd": 65949.0, "price_change_pct_24h": -4.19, "market_cap_rank": 1},
            {"id": "ethereum", "symbol": "ETH", "name": "Ethereum", "current_price_usd": 1981.46, "price_change_pct_24h": -4.01, "market_cap_rank": 2},
            {"id": "solana", "symbol": "SOL", "name": "Solana", "current_price_usd": 154.10, "price_change_pct_24h": 3.25, "market_cap_rank": 5},
        ]
    )


def build_global_stocks_frame():
    return pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "company": "Apple Inc.",
                "market_key": "us",
                "market_label": "Estados Unidos",
                "market_short": "EUA",
                "sector": "Tecnologia",
                "currency": "USD",
                "exchange": "NasdaqGS",
                "price_local": 250.00,
                "previous_close": 245.00,
                "day_high": 252.40,
                "day_low": 247.10,
                "volume": 47000000,
                "regular_market_time": 1774641602,
                "market_state": "CLOSED",
            },
            {
                "symbol": "SAP.DE",
                "company": "SAP SE",
                "market_key": "europe",
                "market_label": "Europa",
                "market_short": "Europa",
                "sector": "Software empresarial",
                "currency": "EUR",
                "exchange": "Xetra",
                "price_local": 145.00,
                "previous_close": 144.00,
                "day_high": 146.20,
                "day_low": 142.50,
                "volume": 3500000,
                "regular_market_time": 1774598400,
                "market_state": "CLOSED",
            },
            {
                "symbol": "7203.T",
                "company": "Toyota Motor Corporation",
                "market_key": "asia",
                "market_label": "Asia-Pacifico",
                "market_short": "Asia",
                "sector": "Automotivo",
                "currency": "JPY",
                "exchange": "Tokyo",
                "price_local": 3408.00,
                "previous_close": 3388.00,
                "day_high": 3415.00,
                "day_low": 3348.00,
                "volume": 16107200,
                "regular_market_time": 1774569600,
                "market_state": "CLOSED",
            },
            {
                "symbol": "VALE3.SA",
                "company": "Vale S.A.",
                "market_key": "latam",
                "market_label": "America Latina",
                "market_short": "LatAm",
                "sector": "Mineracao",
                "currency": "BRL",
                "exchange": "Sao Paulo",
                "price_local": 79.00,
                "previous_close": 78.91,
                "day_high": 79.40,
                "day_low": 77.90,
                "volume": 25200000,
                "regular_market_time": 1774616400,
                "market_state": "CLOSED",
            },
        ]
    )


class IDHMundialViewTests(SimpleTestCase):
    @patch("dashboard.views.load_hdi_time_series_data")
    @patch("dashboard.views.load_hdi_data")
    def test_idh_page_renders(self, mocked_loader, mocked_history_loader):
        mocked_loader.return_value = build_hdi_frame()
        mocked_history_loader.return_value = build_hdi_history_frame()

        response = self.client.get("/idh-mundial/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ranking Mundial de IDH")
        self.assertContains(response, "0,972")
        self.assertContains(response, "Muito alto desenvolvimento humano")
        self.assertContains(response, "Leitura de longo prazo")
        self.assertContains(response, "1990-2023")
        self.assertContains(response, "HDR 2025 / PNUD / ONU")
        self.assertContains(response, "Os arquivos oficiais atualmente carregados sao do HDR 2025")
        self.assertContains(response, "data-auto-submit-dropdowns")

    @patch("dashboard.views.load_hdi_time_series_data")
    @patch("dashboard.views.load_hdi_data")
    def test_idh_page_filters_by_group_and_query(self, mocked_loader, mocked_history_loader):
        mocked_loader.return_value = build_hdi_frame()
        mocked_history_loader.return_value = build_hdi_history_frame()

        response = self.client.get("/idh-mundial/", {"group": "high", "q": "uruguay", "top": 10}, secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "0,830")
        self.assertNotContains(response, "0,972")
        self.assertContains(response, "1 de 1 paises exibidos")
        self.assertContains(response, "S&eacute;rie hist&oacute;rica de")
        self.assertContains(response, "0,692")

    @patch("dashboard.views.load_hdi_time_series_data")
    @patch("dashboard.views.load_hdi_data")
    def test_idh_page_filters_by_year(self, mocked_loader, mocked_history_loader):
        mocked_loader.return_value = build_hdi_frame()
        mocked_history_loader.return_value = build_hdi_history_frame()

        response = self.client.get("/idh-mundial/", {"year": 1990, "q": "iceland"}, secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ano do ranking:")
        self.assertContains(response, "0,799")
        self.assertContains(response, "IDH 0,799 em 1990")
        self.assertContains(response, "Evolu&ccedil;&atilde;o 1990-1990")


class PanoramaMacroeconomicoViewTests(SimpleTestCase):
    def test_macro_page_renders(self):
        with patch("dashboard.views.load_gdp_data", return_value=build_macro_gdp_frame()), patch(
            "dashboard.views.load_gdp_per_capita_data", return_value=build_macro_per_capita_frame()
        ), patch("dashboard.views.load_gdp_growth_real_data", return_value=build_macro_growth_frame()), patch(
            "dashboard.views.load_inflation_data", return_value=build_macro_inflation_frame()
        ), patch(
            "dashboard.views.load_unemployment_data", return_value=build_macro_unemployment_frame()
        ), patch(
            "dashboard.views.load_current_account_data", return_value=build_macro_current_account_frame()
        ), patch(
            "dashboard.views.load_exports_data", return_value=build_macro_exports_frame()
        ), patch(
            "dashboard.views.load_real_interest_rate_data", return_value=build_macro_real_interest_frame()
        ), patch(
            "dashboard.views.load_world_bank_country_metadata", return_value=build_macro_metadata_frame()
        ), patch(
            "dashboard.views.load_country_locale_names", return_value=empty_country_locale_frame()
        ):
            response = self.client.get("/panorama-macroeconomico/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Panorama Macroecon")
        self.assertContains(response, "PIB nominal")
        self.assertContains(response, "3 de 3 paises exibidos")
        self.assertContains(response, "ano 2023")
        self.assertContains(response, "United States")
        self.assertContains(response, "Conta corrente")
        self.assertContains(response, "Exporta")
        self.assertContains(response, "Juros reais")
        self.assertNotContains(response, "World")

    def test_macro_page_filters_and_opens_history_for_one_country(self):
        with patch("dashboard.views.load_gdp_data", return_value=build_macro_gdp_frame()), patch(
            "dashboard.views.load_gdp_per_capita_data", return_value=build_macro_per_capita_frame()
        ), patch("dashboard.views.load_gdp_growth_real_data", return_value=build_macro_growth_frame()), patch(
            "dashboard.views.load_inflation_data", return_value=build_macro_inflation_frame()
        ), patch(
            "dashboard.views.load_unemployment_data", return_value=build_macro_unemployment_frame()
        ), patch(
            "dashboard.views.load_current_account_data", return_value=build_macro_current_account_frame()
        ), patch(
            "dashboard.views.load_exports_data", return_value=build_macro_exports_frame()
        ), patch(
            "dashboard.views.load_real_interest_rate_data", return_value=build_macro_real_interest_frame()
        ), patch(
            "dashboard.views.load_world_bank_country_metadata", return_value=build_macro_metadata_frame()
        ), patch(
            "dashboard.views.load_country_locale_names", return_value=empty_country_locale_frame()
        ):
            response = self.client.get(
                "/panorama-macroeconomico/",
                {"metric": "inflation", "q": "brazil", "year": 2023},
                secure=True,
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "1 de 1 paises exibidos")
        self.assertContains(response, "Historico de Brazil")
        self.assertContains(response, "4,6%")
        self.assertContains(response, "9,3%")
        self.assertContains(response, "360,00 bi")
        self.assertContains(response, "5,8%")
        self.assertNotContains(response, "United States")


class HomeShortcutTests(SimpleTestCase):
    def test_home_contains_macro_shortcut(self):
        response = self.client.get("/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/empresarial/")
        self.assertContains(response, "/impostometro/")
        self.assertContains(response, "/panorama-macroeconomico/")
        self.assertContains(response, "Abrir Panorama Macro")
        self.assertContains(response, "Abrir Impost")
        self.assertContains(response, "Abrir Empresarial")
        self.assertContains(response, "/cotacao-moedas/")
        self.assertContains(response, "Abrir Cota")
        self.assertContains(response, "/acoes-mercado-mundial/")
        self.assertContains(response, "Abrir A&ccedil;&otilde;es Globais")
        self.assertContains(response, "sete frentes")


class EmpresarialViewTests(SimpleTestCase):
    def test_empresarial_page_renders(self):
        response = self.client.get("/empresarial/", secure=True)
        content = response.content.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Empresarial")
        self.assertContains(response, 'role="tablist"')
        self.assertContains(response, 'data-tab-button="diagnostico"')
        self.assertContains(response, 'data-tab-button="charts"')
        self.assertContains(response, 'data-tab-button="indicators"')
        self.assertContains(response, 'data-tab-button="dre"')
        self.assertContains(response, 'data-tab-button="opex"')
        self.assertContains(response, 'data-tab-button="capex"')
        self.assertContains(response, 'data-tab-button="balance"')
        self.assertContains(response, 'data-tab-button="dfc"')
        self.assertContains(response, "Resumo Executivo")
        self.assertContains(response, "DRE - Demostrativo de Resultado do Exercicio")
        self.assertContains(response, "EBITDA")
        self.assertContains(response, "Receita liquida")
        self.assertContains(response, "Lucro liquido")
        self.assertContains(response, "R$ 227,0 mi")
        self.assertContains(response, "Balanco Patrimonial")
        self.assertContains(response, "Patrimonio liquido")
        self.assertContains(response, "R$ 975,0 mi")
        self.assertContains(response, "DFC - Demonstrativo de Fluxo de Caixa")
        self.assertContains(response, "Fluxo de caixa operacional")
        self.assertContains(response, "R$ 193,0 mi")
        self.assertContains(response, "Painel mestre de indicadores")
        self.assertContains(response, "ROIC")
        self.assertContains(response, "PMR")
        self.assertContains(response, "Grafico de colunas do OPEX")
        self.assertContains(response, "Grafico de pizza do CAPEX")
        self.assertContains(response, "Grafico de Pareto das despesas")
        self.assertContains(response, "Graficos")
        self.assertContains(response, "Automacao da linha industrial")
        self.assertContains(response, "Principais leituras do OPEX")
        self.assertContains(response, "Principais leituras do CAPEX")
        self.assertContains(response, "OPEX - Despesas Operacionais")
        self.assertContains(response, "R$ 203,0 mi")
        self.assertContains(response, "CAPEX - Investimentos de Capital")
        self.assertContains(response, "R$ 109,0 mi")
        self.assertLess(content.index('data-tab-button="diagnostico"'), content.index('data-tab-button="charts"'))
        self.assertLess(content.index('data-tab-button="charts"'), content.index('data-tab-button="dfc"'))
        self.assertLess(content.index('data-tab-button="diagnostico"'), content.index('data-tab-button="dfc"'))
        self.assertLess(content.index('data-tab-button="dfc"'), content.index('data-tab-button="dre"'))
        self.assertLess(content.index('data-tab-button="dre"'), content.index('data-tab-button="balance"'))
        self.assertLess(content.index('data-tab-button="balance"'), content.index('data-tab-button="indicators"'))
        self.assertLess(content.index('data-tab-button="indicators"'), content.index('data-tab-button="opex"'))
        self.assertLess(content.index('data-tab-button="opex"'), content.index('data-tab-button="capex"'))


class CotacaoMoedasViewTests(SimpleTestCase):
    @patch("dashboard.views.load_crypto_markets")
    @patch("dashboard.views.load_currency_rates")
    @patch("dashboard.views.load_currency_catalog")
    def test_currency_page_renders(self, mocked_catalog, mocked_rates, mocked_crypto):
        mocked_catalog.return_value = build_currency_catalog_frame()
        mocked_rates.return_value = build_currency_rates_frame()
        mocked_crypto.return_value = build_crypto_markets_frame()

        response = self.client.get("/cotacao-moedas/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cota")
        self.assertContains(response, "BRL")
        self.assertContains(response, "2026-03-27")
        self.assertContains(response, "0,176500")
        self.assertContains(response, "100,00 BRL = 17,65 USD")
        self.assertContains(response, "Bitcoin")
        self.assertContains(response, "BTC")

    @patch("dashboard.views.load_crypto_markets")
    @patch("dashboard.views.load_currency_rates")
    @patch("dashboard.views.load_currency_catalog")
    def test_currency_page_filters_results(self, mocked_catalog, mocked_rates, mocked_crypto):
        mocked_catalog.return_value = build_currency_catalog_frame()
        mocked_rates.return_value = build_currency_rates_frame()
        mocked_crypto.return_value = build_crypto_markets_frame()

        response = self.client.get("/cotacao-moedas/", {"q": "yen", "amount": 250}, secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "1 moedas exibidas")
        self.assertContains(response, "Japanese Yen")
        self.assertContains(response, "6.627,50 JPY")
        self.assertNotContains(response, "17,65 USD")

    @patch("dashboard.views.load_crypto_markets")
    @patch("dashboard.views.load_currency_rates")
    @patch("dashboard.views.load_currency_catalog")
    def test_currency_page_filters_crypto_results(self, mocked_catalog, mocked_rates, mocked_crypto):
        mocked_catalog.return_value = build_currency_catalog_frame()
        mocked_rates.return_value = build_currency_rates_frame()
        mocked_crypto.return_value = build_crypto_markets_frame()

        response = self.client.get("/cotacao-moedas/", {"q": "bitcoin", "amount": 1000}, secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "1 criptomoedas exibidas")
        self.assertContains(response, "Bitcoin")
        self.assertContains(response, "BTC")
        self.assertNotContains(response, "Ethereum")


class AcoesMercadoMundialViewTests(SimpleTestCase):
    @patch("dashboard.views.load_global_stock_quotes")
    @patch("dashboard.views.load_currency_rates")
    @patch("dashboard.views.load_currency_catalog")
    def test_stock_page_renders(self, mocked_catalog, mocked_rates, mocked_stocks):
        mocked_catalog.return_value = build_currency_catalog_frame()
        mocked_rates.return_value = build_currency_rates_frame()
        mocked_stocks.return_value = build_global_stocks_frame()

        response = self.client.get("/acoes-mercado-mundial/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "A&ccedil;&otilde;es do Mercado Mundial")
        self.assertContains(response, "Apple Inc.")
        self.assertContains(response, "Toyota Motor Corporation")
        self.assertContains(response, "4 acoes exibidas")
        self.assertContains(response, "Mercado ativo")
        self.assertContains(response, "1.416,43 BRL")
        self.assertContains(response, "47,00 mi")

    @patch("dashboard.views.load_global_stock_quotes")
    @patch("dashboard.views.load_currency_rates")
    @patch("dashboard.views.load_currency_catalog")
    def test_stock_page_filters_by_market_and_query(self, mocked_catalog, mocked_rates, mocked_stocks):
        mocked_catalog.return_value = build_currency_catalog_frame()
        mocked_rates.return_value = build_currency_rates_frame()
        mocked_stocks.return_value = build_global_stocks_frame()

        response = self.client.get(
            "/acoes-mercado-mundial/",
            {"market": "asia", "q": "toyota", "capital": 5000},
            secure=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "1 acoes exibidas")
        self.assertContains(response, "Toyota Motor Corporation")
        self.assertContains(response, "Asia-Pacifico")
        self.assertNotContains(response, "Apple Inc.")


class ImpostometroViewTests(SimpleTestCase):
    @patch("dashboard.views.fetch_impostometro_year_total")
    @patch("dashboard.views.fetch_impostometro_live")
    def test_impostometro_page_renders_with_live_api_payload(self, mocked_live, mocked_year_total):
        mocked_live.return_value = {
            "value": 1234.5,
            "increment": 10.25,
            "data": "2026-04-09T21:04:44.4740000",
            "midnight": "2026-04-09T00:00:00.0000000",
        }
        mocked_year_total.side_effect = lambda year: {"value": float(year) * 1000.0}

        response = self.client.get("/impostometro/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tributos acumulados no Brasil")
        self.assertContains(response, "Fonte: API do Impost")
        self.assertContains(response, "const accumulated = 1234.5;")
        self.assertContains(response, "const ratePerSec = 10.25;")
        self.assertNotContains(response, "const accumulated = 1234,5;")
        self.assertNotContains(response, "const ratePerSec = 10,25;")

    @patch("dashboard.views.fetch_impostometro_year_total")
    @patch("dashboard.views.fetch_impostometro_live")
    def test_impostometro_page_renders_with_fallback_when_api_is_unavailable(self, mocked_live, mocked_year_total):
        mocked_live.return_value = None
        mocked_year_total.side_effect = lambda year: {"value": 3_000_000_000_000.0}

        response = self.client.get("/impostometro/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Estimado")
        self.assertContains(response, "Fallback local com base no")
        self.assertContains(response, "Tributos no Brasil por ano")

    @patch("dashboard.views.fetch_impostometro_year_total")
    @patch("dashboard.views.fetch_impostometro_live")
    def test_impostometro_page_renders_with_disk_snapshot_badge(self, mocked_live, mocked_year_total):
        mocked_live.return_value = {
            "value": 1500.0,
            "increment": 12.5,
            "data": "2026-04-09T21:04:44.4740000",
            "midnight": "2026-04-09T00:00:00.0000000",
            "derived_from_snapshot": True,
        }
        mocked_year_total.side_effect = lambda year: {"value": 3_000_000_000_000.0}

        response = self.client.get("/impostometro/", secure=True)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Snapshot")
        self.assertContains(response, "Fonte: snapshot local persistido")

    @patch("dashboard.views.requests.get", side_effect=Exception("offline"))
    @patch("dashboard.views.load_impostometro_persisted_cache")
    def test_fetch_impostometro_range_uses_persisted_cache_when_request_fails(self, mocked_load_cache, mocked_get):
        from dashboard import views

        mocked_load_cache.return_value = {
            "ranges": {
                "01/01/2026|09/04/2026": {
                    "start_date": "01/01/2026",
                    "end_date": "09/04/2026",
                    "value": 111.0,
                    "increment": 2.5,
                    "data": "2026-04-09T21:04:44.4740000",
                    "midnight": "2026-04-09T00:00:00.0000000",
                }
            }
        }
        views.IMPOSTOMETRO_RANGE_CACHE = {}
        views.IMPOSTOMETRO_RANGE_CACHE_TS = {}

        result = views.fetch_impostometro_range("01/01/2026", "09/04/2026")

        self.assertEqual(result["value"], 111.0)
        self.assertEqual(result["increment"], 2.5)
        self.assertTrue(mocked_get.called)

    @patch("dashboard.views.fetch_impostometro_range", return_value=None)
    def test_fetch_impostometro_year_total_uses_seed_when_api_is_unavailable(self, mocked_fetch_range):
        from dashboard import views

        result = views.fetch_impostometro_year_total(2025)

        self.assertEqual(result["value"], float(views.IMPOSTOMETRO_ANNUAL_SEED_BRL[2025]))
        self.assertEqual(result["increment"], 0.0)
