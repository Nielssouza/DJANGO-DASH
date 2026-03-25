import os
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dash import Input, Output, dcc, html, dash_table, ctx
from django_plotly_dash import DjangoDash

# Premium Dark Theme Palette
BG = "#000000"
SURFACE = "#0a0a0c"
BORDER = "rgba(255, 255, 255, 0.1)"
TXT_PRIMARY = "#ffffff"
TXT_SECONDARY = "rgba(255, 255, 255, 0.55)"
TXT_TERTIARY = "rgba(255, 255, 255, 0.3)"
ACCENT_BLUE = "#3b82f6"
CACHE_FILE = "gdp_data_cache.csv"

def load_data(force=False):
    if not force and os.path.exists(CACHE_FILE):
        return pd.read_csv(CACHE_FILE)
    url = "http://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json&per_page=20000"
    try:
        r = requests.get(url, timeout=15)
        data = r.json()[1]
        rows = [{"País": i["country"]["value"], "Ano": int(i["date"]), "gdp": i["value"]} for i in data if i["value"] is not None]
        df = pd.DataFrame(rows).sort_values(["País", "Ano"])
        df.to_csv(CACHE_FILE, index=False)
        return df
    except:
        return pd.read_csv(CACHE_FILE) if os.path.exists(CACHE_FILE) else pd.DataFrame()

df_init = load_data()
paises = sorted(df_init["País"].unique()) if not df_init.empty else []
max_y = int(df_init["Ano"].max()) if not df_init.empty else 2023

app = DjangoDash("PIBMundial")

# Injecting Custom CSS for Animations and Premium Feel
custom_css = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@400;500;600;700;800&display=swap');

.dash-app-wrapper { font-family: 'Inter', sans-serif; }
.metric-card {
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    background: linear-gradient(145deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.01) 100%);
    box-shadow: 0 4px 24px -8px rgba(0,0,0,0.5);
    backdrop-filter: blur(10px);
}
.metric-card:hover {
    transform: translateY(-4px);
    border-color: rgba(255,255,255,0.2) !important;
    box-shadow: 0 12px 32px -8px rgba(0,0,0,0.8);
}
.btn-primary {
    background: linear-gradient(135deg, #ffffff 0%, #d4d4d4 100%);
    color: #000;
    border: none;
    border-radius: 6px;
    padding: 10px 24px;
    font-size: 0.75rem;
    font-weight: 700;
    font-family: 'Outfit', sans-serif;
    letter-spacing: 0.05em;
    cursor: pointer;
    transition: all 0.3s ease;
    box-shadow: 0 0 15px rgba(255,255,255,0.1);
}
.btn-primary:hover {
    transform: translateY(-2px);
    box-shadow: 0 0 25px rgba(255,255,255,0.3);
}
.input-field {
    background: rgba(0,0,0,0.5) !important;
    color: #fff !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
    padding: 10px 12px;
    border-radius: 6px;
    width: 100%;
    font-size: 0.85rem;
    transition: border-color 0.3s ease;
}
.input-field:focus {
    outline: none;
    border-color: rgba(255, 255, 255, 0.3) !important;
}
.text-gradient {
    background: linear-gradient(90deg, #ffffff, #888888);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
/* Estilizando o Dropdown do Dash */
.Select-control {
    background-color: #000 !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    color: #fff !important;
    border-radius: 8px !important;
    padding: 4px !important;
    transition: border-color 0.3s ease;
}
.Select-control:hover { border-color: rgba(255,255,255,0.3) !important; }
.Select-menu-outer {
    background-color: #0a0a0c !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    border-radius: 8px !important;
    box-shadow: 0 10px 25px rgba(0,0,0,0.8) !important;
    color: #fff !important;
}
.Select-option:hover { background-color: rgba(255,255,255,0.1) !important; }
.Select-value-label { color: #fff !important; }
.dash-spreadsheet-container .dash-spreadsheet-inner * {
    font-family: 'Inter', sans-serif !important;
}
"""

def metric_card(title, value, subtitle=None, **kwargs):
    return html.Div(className="metric-card", style={
        "padding": "32px", "borderRadius": "12px", 
        "border": f"1px solid {BORDER}", "flex": "1",
        "position": "relative", "overflow": "hidden"
    }, children=[
        html.Div(title, style={"fontSize": "0.7rem", "fontWeight": "600", "color": TXT_SECONDARY, "textTransform": "uppercase", "letterSpacing": "0.15em", "marginBottom": "16px"}),
        html.Div(value, className="text-gradient", style={"fontSize": "2.5rem", "fontWeight": "800", "fontFamily": "Outfit", "letterSpacing": "-0.03em", "lineHeight": "1"}),
        html.Div(subtitle, style={"fontSize": "0.75rem", "color": TXT_TERTIARY, "marginTop": "12px", "fontWeight": "500"}) if subtitle else None
    ], **kwargs)

app.layout = html.Div(className="dash-app-wrapper", style={"backgroundColor": BG, "minHeight": "100vh", "padding": "20px"}, children=[
    # Injetando CSS via dcc.Markdown (Fallback seguro quando html.Style não existe)
    dcc.Markdown(f"<style>{custom_css}</style>", dangerously_allow_html=True),
    
    # Header Section
    html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start", "borderBottom": f"1px solid {BORDER}", "paddingBottom": "32px", "marginBottom": "48px"}, children=[
        html.Div([
            html.H2("Global Economic Intelligence", style={"fontSize": "2.2rem", "fontWeight": "800", "fontFamily": "Outfit", "margin": "0", "letterSpacing": "-0.02em", "color": TXT_PRIMARY}),
            html.P("Plataforma de monitoramento macroeconômico em tempo real", style={"color": TXT_SECONDARY, "fontSize": "0.95rem", "fontWeight": "400", "margin": "12px 0 0 0"})
        ]),
        html.Button("RELOAD DATA", id="refresh-btn", className="btn-primary")
    ]),

    # Main KPI Bar
    html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "32px", "marginBottom": "32px"}, children=[
        metric_card("Total Aggregate GDP", id="total-val", value="Loading..."),
        metric_card("Dominant Economy", id="top-unit", value="Loading...")
    ]),
    
    # Secondary KPIs
    html.Div(id="bench-container", style={"display": "flex", "gap": "32px", "marginBottom": "56px"}),

    # Analytics Core
    html.Div(style={"display": "grid", "gridTemplateColumns": "320px 1fr", "gap": "40px"}, children=[
        # Controls Column
        html.Div(children=[
            html.Div(style={"background": SURFACE, "padding": "32px", "borderRadius": "12px", "border": f"1px solid {BORDER}"}, children=[
                html.Label("Filtro de Países", style={"fontSize": "0.75rem", "fontWeight": "700", "color": TXT_SECONDARY, "textTransform": "uppercase", "display": "block", "marginBottom": "16px", "letterSpacing": "0.05em"}),
                dcc.Dropdown(id="country-drop", options=[{"label": c, "value": c} for c in paises], value=["Brazil", "United States", "China", "India"], multi=True, className="custom-dropdown"),
                
                html.Hr(style={"border": "0", "borderTop": f"1px solid {BORDER}", "margin": "32px 0"}),
                
                html.Label("Intervalo de Tempo", style={"fontSize": "0.75rem", "fontWeight": "700", "color": TXT_SECONDARY, "textTransform": "uppercase", "display": "block", "marginBottom": "16px", "letterSpacing": "0.05em"}),
                html.Div(style={"display": "flex", "gap": "12px", "alignItems": "center"}, children=[
                    dcc.Input(id="y-start", type="number", value=max_y-10, className="input-field"),
                    html.Span("—", style={"color": TXT_TERTIARY}),
                    dcc.Input(id="y-end", type="number", value=max_y, className="input-field"),
                ])
            ])
        ]),

        # Content Column
        html.Div(children=[
            # Graph
            html.Div(className="metric-card", style={"padding": "32px", "borderRadius": "12px", "border": f"1px solid {BORDER}", "marginBottom": "40px"}, children=[
                html.Div("GDP Relative Ranking", style={"fontSize": "0.75rem", "fontWeight": "700", "color": TXT_SECONDARY, "textTransform": "uppercase", "letterSpacing": "0.05em", "marginBottom": "24px"}),
                dcc.Graph(id="main-graph", config={"displayModeBar": False})
            ]),
            # Table
            html.Div(style={"background": SURFACE, "padding": "32px", "borderRadius": "12px", "border": f"1px solid {BORDER}"}, children=[
                html.Div("Data Matrix Intelligence", style={"fontSize": "0.75rem", "fontWeight": "700", "color": TXT_SECONDARY, "textTransform": "uppercase", "letterSpacing": "0.05em", "marginBottom": "24px"}),
                html.Div(id="table-container")
            ])
        ])
    ])
])

def fmt(v):
    if not v: return "-"
    if v >= 1e12: return f"${v/1e12:.2f}T"
    if v >= 1e9: return f"${v/1e9:.2f}B"
    return f"${v/1e6:.2f}M"

@app.callback(
    [Output("main-graph", "figure"), Output("total-val", "children"), Output("top-unit", "children"), 
     Output("table-container", "children"), Output("bench-container", "children")],
    [Input("country-drop", "value"), Input("y-start", "value"), Input("y-end", "value"), Input("refresh-btn", "n_clicks")]
)
def update(sel, ys, ye, n):
    try: forced = ctx.triggered_id == "refresh-btn"
    except: forced = False
    
    df = load_data(force=forced)
    if df.empty or not sel: return go.Figure(), [], [], "", []
    ys, ye = ys or df["Ano"].min(), ye or df["Ano"].max()
    
    # Benchmarks
    df_b = df[df["Ano"] == ye]
    v_w = df_b[df_b["País"] == "World"]["gdp"].iloc[0] if "World" in df_b["País"].values else 0
    v_e = df_b[df_b["País"] == "European Union"]["gdp"].iloc[0] if "European Union" in df_b["País"].values else 0
    bench = [metric_card("Index World", fmt(v_w), f"FY {ye}"), metric_card("UE Region", fmt(v_e), f"FY {ye}")]

    # Process
    df_f = df[(df["País"].isin(sel)) & (df["Ano"] >= ys) & (df["Ano"] <= ye)]
    if df_f.empty: return go.Figure(), [], [], "", bench
    df_l = df_f[df_f["Ano"] == df_f["Ano"].max()].sort_values("gdp", ascending=False)
    
    # KPIs
    tot = [html.Div("Total Group Vol", style={"fontSize": "0.65rem", "fontWeight": "600", "color": TXT_SECONDARY, "textTransform": "uppercase", "marginBottom": "12px", "letterSpacing": "0.1em"}),
           html.Div(fmt(df_l["gdp"].sum()), className="text-gradient", style={"fontSize": "2.2rem", "fontWeight": "700", "fontFamily": "Outfit"})]
    top = [html.Div(f"Leader: {df_l.iloc[0]['País']}", style={"fontSize": "0.65rem", "fontWeight": "600", "color": TXT_SECONDARY, "textTransform": "uppercase", "marginBottom": "12px", "letterSpacing": "0.1em"}),
           html.Div(fmt(df_l.iloc[0]["gdp"]), className="text-gradient", style={"fontSize": "2.2rem", "fontWeight": "700", "fontFamily": "Outfit"})]

    # Graph
    fig = px.bar(df_l, x="País", y="gdp", template="plotly_dark", color="gdp", color_continuous_scale=["#3b82f6", "#60a5fa", "#ffffff"])
    fig.update_coloraxes(showscale=False)
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", 
        font={"family": "Inter", "color": TXT_SECONDARY, "size": 11},
        yaxis={"gridcolor": "rgba(255,255,255,0.05)", "tickprefix": "$", "tickformat": ".2s"},
        xaxis={"gridcolor": "rgba(0,0,0,0)", "title": ""}, 
        margin={"l":0,"r":0,"t":10,"b":0},
        bargap=0.3,
        hoverlabel={"bgcolor": "#0a0a0c", "font_size": 13, "font_family": "Inter", "bordercolor": "rgba(255,255,255,0.2)"}
    )
    fig.update_traces(marker_line_width=0, opacity=0.9, hovertemplate="<b>%{x}</b><br>GDP: %{y:$,.0f}<extra></extra>")

    # Table
    df_p = df_f[df_f["Ano"] >= ye-5].pivot(index="País", columns="Ano", values="gdp").reset_index()
    for col in df_p.columns[1:]: df_p[col] = df_p[col].apply(fmt)
    
    table = dash_table.DataTable(
        data=df_p.to_dict('records'),
        columns=[{"name": str(i), "id": str(i)} for i in df_p.columns],
        style_header={
            'backgroundColor': 'rgba(255, 255, 255, 0.03)', 'color': "#fff", 'fontWeight': '600', 
            'borderBottom': f'1px solid {BORDER}', 'border': 'none', 'padding': '16px', 
            'fontFamily': 'Inter', 'fontSize': '0.75rem', 'textTransform': 'uppercase', 'letterSpacing': '0.05em'
        },
        style_cell={
            'backgroundColor': 'transparent', 'color': TXT_SECONDARY, 'textAlign': 'left', 
            'borderBottom': f'1px solid {BORDER}', 'border': 'none', 'padding': '16px 20px', 
            'fontFamily': 'Inter', 'fontSize': '0.85rem'
        },
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(255,255,255,0.01)'},
            {'if': {'state': 'hover'}, 'backgroundColor': 'rgba(255,255,255,0.05)', 'border': '1px solid rgba(255,255,255,0.1)'}
        ],
        style_table={'borderRadius': '8px', 'overflow': 'hidden'}
    )

    return fig, tot, top, table, bench
