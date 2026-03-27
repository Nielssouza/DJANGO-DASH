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


def fmt_percent(value, signed=False):
    if value is None or pd.isna(value):
        return "--"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value:.1f}%".replace(".", ",")


def fmt_decimal(value, digits=1):
    if value is None or pd.isna(value):
        return "--"
    return f"{value:.{digits}f}".replace(".", ",")


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
