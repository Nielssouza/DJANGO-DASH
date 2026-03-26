import os

import pandas as pd
import plotly.graph_objects as go
import requests
from dash import Input, Output, ctx, dash_table, dcc, html
from django_plotly_dash import DjangoDash

BG = "#000000"
SURFACE = "rgba(8, 8, 8, 0.94)"
BORDER = "rgba(255, 255, 255, 0.08)"
BORDER_STRONG = "rgba(255, 255, 255, 0.16)"
TXT_PRIMARY = "#f8fbff"
TXT_SECONDARY = "rgba(248, 251, 255, 0.76)"
ACCENT = "#a1a1aa"
CACHE_FILE = "gdp_data_cache.csv"
DEFAULT_COUNTRIES = ["World", "United States", "China", "India", "Brazil"]


def normalize_columns(dataframe):
    if dataframe.empty:
        return dataframe

    return dataframe.rename(
        columns={
            "PaÃ­s": "Pais",
            "País": "Pais",
            "Country": "Pais",
            "Year": "Ano",
            "GDP": "gdp",
        }
    )


def load_data(force=False):
    if not force and os.path.exists(CACHE_FILE):
        return normalize_columns(pd.read_csv(CACHE_FILE))

    url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json&per_page=20000"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
        rows = [
            {"Pais": item["country"]["value"], "Ano": int(item["date"]), "gdp": item["value"]}
            for item in payload[1]
            if item["value"] is not None
        ]
        dataframe = pd.DataFrame(rows).sort_values(["Pais", "Ano"])
        dataframe.to_csv(CACHE_FILE, index=False)
        return dataframe
    except Exception:
        if os.path.exists(CACHE_FILE):
            return normalize_columns(pd.read_csv(CACHE_FILE))
        return pd.DataFrame(columns=["Pais", "Ano", "gdp"])


def value_for(dataframe, country_name):
    if dataframe.empty:
        return None

    country_data = dataframe[dataframe["Pais"] == country_name]
    if country_data.empty:
        return None
    return country_data["gdp"].iloc[0]


def fmt(value):
    if value is None or pd.isna(value):
        return "--"
    if value >= 1e12:
        return f"${value / 1e12:.2f}T"
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    return f"${value / 1e6:.2f}M"


def empty_figure(message):
    figure = go.Figure()
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "font": {"family": "Manrope", "size": 16, "color": TXT_SECONDARY},
            }
        ],
    )
    return figure


def empty_table_notice(message):
    return html.Div(message, className="empty-state")


def metric_card(title, value, subtitle=None, value_id=None):
    value_kwargs = {"className": "metric-value"}
    if value_id:
        value_kwargs["id"] = value_id

    return html.Div(
        className="metric-card panel-surface",
        children=[
            html.Div(title, className="metric-label"),
            html.Div(value, **value_kwargs),
            html.Div(subtitle, className="metric-subtitle") if subtitle else None,
        ],
    )


DATAFRAME = load_data()
COUNTRIES = sorted(DATAFRAME["Pais"].unique()) if not DATAFRAME.empty else []
DEFAULT_SELECTION = [country for country in DEFAULT_COUNTRIES if country in COUNTRIES] or COUNTRIES[:5]
MAX_YEAR = int(DATAFRAME["Ano"].max()) if not DATAFRAME.empty else 2024
MIN_YEAR = max(1960, int(DATAFRAME["Ano"].min())) if not DATAFRAME.empty else 1960
YEAR_OPTIONS = [{"label": str(year), "value": year} for year in range(MAX_YEAR, MIN_YEAR - 1, -1)]


def get_runtime_dataframe(force=False):
    global DATAFRAME

    if force:
        refreshed = load_data(force=True)
        if not refreshed.empty:
            DATAFRAME = refreshed
        return DATAFRAME

    if DATAFRAME.empty:
        DATAFRAME = load_data(force=False)

    return DATAFRAME

app = DjangoDash(
    "PIBMundial",
    serve_locally=True,
    external_stylesheets=["/static/dashboard/pib_mundial_dash.css"],
)

app.layout = html.Div(
    className="dash-shell",
    style={"backgroundColor": BG, "minHeight": "100vh", "padding": "6px"},
    children=[
        html.Div(
            className="dash-hero panel-surface",
            children=[
                html.Div(
                    className="dash-hero-grid",
                    children=[
                        html.Div(
                            children=[
                                html.Div("Serie historica de pib", className="hero-kicker"),
                                html.H2("PIB mundial e por pais de 1960 ate o ano atual.", className="hero-title"),
                                html.P(
                                    "Este painel foi reorganizado para listar a serie historica completa desde 1960, "
                                    "comparar paises em barras por ano e resumir tudo em cards de dashboard.",
                                    className="hero-copy",
                                ),
                                html.Div(
                                    className="hero-pill-grid",
                                    children=[
                                        html.Div(className="hero-pill", children=[html.Strong("1960 ate hoje"), html.Span("Cobertura historica fixa ate o ultimo ano disponivel.")]),
                                        html.Div(className="hero-pill", children=[html.Strong("Filtro por pais"), html.Span("Compare economias especificas com selecao multipla.")]),
                                        html.Div(className="hero-pill", children=[html.Strong("Cards + barras"), html.Span("Leitura rapida do ano escolhido e listagem completa abaixo.")]),
                                    ],
                                ),
                            ]
                        ),
                        html.Div(
                            className="hero-side",
                            children=[
                                html.Div(
                                    className="hero-side-card",
                                    children=[
                                        html.Div("Cobertura", className="hero-kicker"),
                                        html.Strong(f"{MIN_YEAR} -> {MAX_YEAR}"),
                                        html.P("Serie historica consolidada do Banco Mundial para o indicador de PIB."),
                                    ],
                                ),
                                html.Div(
                                    className="hero-side-card",
                                    children=[
                                        html.Div("Visao atual", className="hero-kicker"),
                                        html.Strong("Comparativo por ano"),
                                        html.P("Escolha um ano para o grafico de barras e acompanhe a lista historica completa."),
                                    ],
                                ),
                                html.Button("RELOAD DATA", id="refresh-btn", className="btn-primary"),
                            ],
                        ),
                    ],
                )
            ],
        ),
        html.Div(
            className="dash-kpi-grid",
            children=[
                metric_card("PIB mundial", "Loading...", "Valor do agregado World para o ano selecionado.", value_id="world-val"),
                metric_card("Maior economia do ano", "Loading...", "Lider entre os paises filtrados no ano do grafico.", value_id="top-unit"),
            ],
        ),
        html.Div(id="bench-container", className="dash-bench-grid"),
        html.Div(
            className="core-grid",
            children=[
                html.Div(
                    className="control-surface panel-surface",
                    children=[
                        html.Div("Control stack", className="panel-kicker"),
                        html.H3("Filtros do painel", className="panel-title", style={"marginBottom": "10px"}),
                        html.P(
                            "A serie fica fixa entre 1960 e o ano mais recente. Aqui voce escolhe os paises e o ano usado no grafico de barras.",
                            className="panel-copy",
                            style={"marginBottom": "24px"},
                        ),
                        html.Div(
                            className="field-group",
                            children=[
                                html.Div("Paises", className="field-label"),
                                html.P("Escolha quais economias entram no comparativo.", className="field-copy"),
                                dcc.Dropdown(
                                    id="country-drop",
                                    options=[{"label": country, "value": country} for country in COUNTRIES],
                                    value=DEFAULT_SELECTION,
                                    multi=True,
                                ),
                            ],
                        ),
                        html.Div(
                            className="field-group",
                            children=[
                                html.Div("Ano do grafico", className="field-label"),
                                html.P(f"Snapshot em barras dentro da serie {MIN_YEAR}-{MAX_YEAR}.", className="field-copy"),
                                dcc.Dropdown(
                                    id="year-drop",
                                    options=YEAR_OPTIONS,
                                    value=MAX_YEAR,
                                    clearable=False,
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    children=[
                        html.Div(
                            className="chart-surface panel-surface",
                            children=[
                                html.Div(
                                    className="panel-head",
                                    children=[
                                        html.Div(children=[html.Div("Bar comparison", className="panel-kicker"), html.H3("PIB por pais no ano selecionado", className="panel-title")]),
                                        html.P("Grafico de barras comparando os paises filtrados no ano escolhido no seletor lateral.", className="panel-copy"),
                                    ],
                                ),
                                dcc.Graph(id="main-graph", className="dash-graph", config={"displayModeBar": False}),
                            ],
                        ),
                        html.Div(
                            className="table-surface panel-surface",
                            children=[
                                html.Div(
                                    className="panel-head",
                                    children=[
                                        html.Div(children=[html.Div("Year list", className="panel-kicker"), html.H3("Lista do ano selecionado", className="panel-title")]),
                                        html.P("O carregamento inicial traz apenas o ano mais recente disponivel. Use o seletor para navegar pela serie de 1960 ate 2024.", className="panel-copy"),
                                    ],
                                ),
                                html.Div(id="table-container"),
                            ],
                        ),
                    ]
                ),
            ],
        ),
    ],
)


@app.callback(
    [
        Output("main-graph", "figure"),
        Output("world-val", "children"),
        Output("top-unit", "children"),
        Output("table-container", "children"),
        Output("bench-container", "children"),
    ],
    [
        Input("country-drop", "value"),
        Input("year-drop", "value"),
        Input("refresh-btn", "n_clicks"),
    ],
)
def update(selected_countries, selected_year, _n_clicks):
    try:
        forced_reload = ctx.triggered_id == "refresh-btn"
    except Exception:
        forced_reload = False
    dataframe = get_runtime_dataframe(force=forced_reload)

    if dataframe.empty or not selected_countries:
        return (
            empty_figure("Sem dados disponiveis para exibir."),
            "--",
            "--",
            empty_table_notice("Nenhum dado foi carregado para o painel."),
            [],
        )

    selected_year = int(selected_year or MAX_YEAR)
    selected_year = min(max(selected_year, MIN_YEAR), MAX_YEAR)

    filtered = dataframe[
        (dataframe["Pais"].isin(selected_countries))
        & (dataframe["Ano"] >= MIN_YEAR)
        & (dataframe["Ano"] <= MAX_YEAR)
    ]
    if filtered.empty:
        return (
            empty_figure("Nenhum resultado encontrado para os paises selecionados."),
            "--",
            "--",
            empty_table_notice("Ajuste os paises filtrados para preencher a serie historica."),
            [],
        )

    year_frame = filtered[filtered["Ano"] == selected_year].sort_values("gdp", ascending=False)
    world_value = value_for(dataframe[dataframe["Ano"] == selected_year], "World")

    if year_frame.empty:
        top_value = "--"
        figure = empty_figure(f"Nenhum dado encontrado para {selected_year} nos paises filtrados.")
    else:
        top_country = year_frame.iloc[0]["Pais"]
        top_country_value = fmt(year_frame.iloc[0]["gdp"])
        top_value = html.Div(
            className="metric-stack",
            children=[html.Span(top_country), html.Small(top_country_value, className="metric-stack-note")],
        )

        colors = []
        for index, country_name in enumerate(year_frame["Pais"]):
            if country_name == "World":
                colors.append("#ffffff")
            elif index < 3:
                colors.append("#d4d4d8")
            else:
                colors.append(ACCENT)

        figure = go.Figure(
            data=[
                go.Bar(
                    x=year_frame["Pais"],
                    y=year_frame["gdp"],
                    marker={"color": colors, "line": {"color": "rgba(255,255,255,0.08)", "width": 1.2}},
                    hovertemplate="<b>%{x}</b><br>PIB: %{y:$,.0f}<extra></extra>",
                )
            ]
        )
        figure.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin={"l": 0, "r": 0, "t": 10, "b": 0},
            font={"family": "Manrope", "color": TXT_SECONDARY, "size": 12},
            xaxis={"title": "", "tickangle": -18, "gridcolor": "rgba(0,0,0,0)", "tickfont": {"size": 11}},
            yaxis={"title": "", "gridcolor": "rgba(255,255,255,0.06)", "zerolinecolor": "rgba(255,255,255,0.08)", "tickprefix": "$", "tickformat": ".2s"},
            bargap=0.28,
            hoverlabel={"bgcolor": SURFACE, "bordercolor": BORDER_STRONG, "font": {"family": "Manrope", "size": 13, "color": TXT_PRIMARY}},
        )

    benchmark_cards = [
        metric_card("Cobertura historica", f"{MIN_YEAR}-{MAX_YEAR}", "Serie disponivel no painel."),
        metric_card("Ano do grafico", str(selected_year), "Base usada para o comparativo em barras."),
        metric_card("Paises filtrados", str(len(selected_countries)), "Economias selecionadas no filtro."),
        metric_card("Registros listados", f"{len(filtered):,}".replace(",", "."), "Linhas historicas desde 1960."),
    ]

    table_frame = filtered[filtered["Ano"] == selected_year].sort_values(["gdp", "Pais"], ascending=[False, True]).copy()
    table_frame["Ano"] = table_frame["Ano"].astype(int)
    table_frame["PIB"] = table_frame["gdp"].apply(fmt)
    table_frame = table_frame[["Ano", "Pais", "PIB"]]

    table = dash_table.DataTable(
        data=table_frame.to_dict("records"),
        columns=[{"name": column, "id": column} for column in table_frame.columns],
        style_header={
            "backgroundColor": "rgba(255, 255, 255, 0.04)",
            "color": TXT_PRIMARY,
            "fontWeight": "700",
            "borderBottom": f"1px solid {BORDER}",
            "border": "none",
            "padding": "16px 18px",
            "fontFamily": "JetBrains Mono",
            "fontSize": "11px",
            "textTransform": "uppercase",
            "letterSpacing": "0.12em",
        },
        style_cell={
            "backgroundColor": "transparent",
            "color": TXT_SECONDARY,
            "textAlign": "left",
            "borderBottom": f"1px solid {BORDER}",
            "border": "none",
            "padding": "16px 18px",
            "fontFamily": "Manrope",
            "fontSize": "14px",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "rgba(255, 255, 255, 0.018)"},
            {"if": {"state": "hover"}, "backgroundColor": "rgba(255, 255, 255, 0.05)", "border": f"1px solid {BORDER_STRONG}"},
        ],
        style_table={"borderRadius": "20px", "overflow": "hidden", "overflowX": "auto"},
        sort_action="native",
        page_action="native",
        page_size=15,
    )

    return (
        figure,
        fmt(world_value),
        top_value,
        table,
        benchmark_cards,
    )
