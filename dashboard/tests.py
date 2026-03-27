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
