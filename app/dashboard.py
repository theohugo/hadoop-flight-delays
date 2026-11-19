"""Dashboard Streamlit — retards des vols américains (extrait 2015).

L'application lit uniquement les tables Delta de la couche Gold (/lakehouse/gold/...).
Elle ne charge ni Silver ni les CSV : toutes les agrégations lourdes sont faites dans
les notebooks J3 (tables d'analyse) et J4 (segmentation K-Means).
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Retards des vols 2015", layout="wide")

GOLD = "/lakehouse/gold"
TABLES = {
    "pareto": "airport_delay_pareto",
    "daily": "daily_delay_trend",
    "airlines": "airline_monthly_performance",
    "hourly": "hourly_delay_profile",
    "segments": "airport_segments",
}
MONTHS = {1: "Janvier", 4: "Avril", 7: "Juillet", 11: "Novembre"}
# Couleurs catégorielles en ordre fixe (attachées à l'entité, jamais au rang).
MONTH_COLORS = {"Janvier": "#2a78d6", "Avril": "#eb6834", "Juillet": "#1baf7a", "Novembre": "#eda100"}
SEGMENT_SLOTS = ["#2a78d6", "#eb6834", "#1baf7a"]
NEUTRAL = "#8a8985"
HIGHLIGHT = "#2a78d6"

INDICATORS_SQL = """    COUNT(*)                                   AS flights,
    SUM(cancelled)                             AS cancelled_flights,
    COUNT(departure_delay_15)                  AS observed_flights,
    SUM(departure_delay_15)                    AS delayed_15_flights,
    TRY_DIVIDE(SUM(cancelled), COUNT(*))       AS cancellation_rate,
    TRY_DIVIDE(SUM(departure_delay_15), COUNT(departure_delay_15)) AS delay_rate_15,
    PERCENTILE_APPROX(IF(cancelled = 0, departure_delay, NULL), 0.25) AS p25_delay,
    PERCENTILE_APPROX(IF(cancelled = 0, departure_delay, NULL), 0.50) AS median_delay,
    PERCENTILE_APPROX(IF(cancelled = 0, departure_delay, NULL), 0.75) AS p75_delay,
    PERCENTILE_APPROX(IF(cancelled = 0, departure_delay, NULL), 0.90) AS p90_delay,
    ROUND(AVG(IF(cancelled = 0, departure_delay, NULL)), 2)          AS mean_delay,
    SUM(positive_departure_delay)              AS total_delay_minutes"""

SQL = {
    "pareto": f"""-- Notebook J3, table Gold airport_delay_pareto (v_silver = Silver projetée)
WITH by_airport AS (
    SELECT origin_airport AS airport, origin_airport_name AS airport_name,
           origin_city AS city, origin_state AS state,
           origin_latitude AS latitude, origin_longitude AS longitude,
{INDICATORS_SQL}
    FROM v_silver
    GROUP BY origin_airport, origin_airport_name, origin_city, origin_state,
             origin_latitude, origin_longitude
)
SELECT *,
       p75_delay - p25_delay AS iqr_delay,
       DENSE_RANK() OVER (ORDER BY total_delay_minutes DESC) AS rank_total_minutes,
       SUM(total_delay_minutes) OVER (ORDER BY total_delay_minutes DESC, airport
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
         / SUM(total_delay_minutes) OVER () AS cumulative_share_minutes,
       ROW_NUMBER() OVER (ORDER BY total_delay_minutes DESC, airport)
         / COUNT(*) OVER () AS cumulative_share_airports,
       IF(flights >= 5000,
          DENSE_RANK() OVER (PARTITION BY flights >= 5000 ORDER BY delay_rate_15 DESC),
          NULL) AS rank_delay_rate
FROM by_airport
-- Puis (PySpark) : pareto_class = 'A' tant que la part cumulée AVANT l'aéroport est < 80 %
-- (l'aéroport qui franchit 80 % est inclus), 'B' jusqu'à 95 %, 'C' ensuite.""",
    "daily": f"""-- Notebook J3, table Gold daily_delay_trend
WITH by_day AS (
    SELECT flight_date, month, day_of_week,
{INDICATORS_SQL}
    FROM v_silver
    GROUP BY flight_date, month, day_of_week
)
SELECT *,
       LAG(delay_rate_15) OVER w                           AS prev_day_delay_rate,
       100.0 * (delay_rate_15 - LAG(delay_rate_15) OVER w) AS delay_rate_change_pts,
       100.0 * TRY_DIVIDE(total_delay_minutes - LAG(total_delay_minutes) OVER w,
                          LAG(total_delay_minutes) OVER w) AS total_minutes_change_pct,
       AVG(delay_rate_15) OVER (PARTITION BY month ORDER BY flight_date
                                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS delay_rate_7d_avg
FROM by_day
WINDOW w AS (PARTITION BY month ORDER BY flight_date)""",
    "airlines": f"""-- Notebook J3, table Gold airline_monthly_performance
WITH by_airline_month AS (
    SELECT month, airline, airline_name,
{INDICATORS_SQL}
    FROM v_silver
    GROUP BY month, airline, airline_name
)
SELECT *,
       p75_delay - p25_delay AS iqr_delay,
       ROW_NUMBER() OVER (PARTITION BY month ORDER BY delay_rate_15 DESC, airline) AS rank_in_month
FROM by_airline_month

-- Réagrégation faite par le dashboard pour plusieurs mois (pondérée, jamais une moyenne de taux) :
SELECT airline, airline_name,
       SUM(flights) AS flights,
       SUM(delayed_15_flights) / SUM(observed_flights) AS delay_rate_15,
       SUM(cancelled_flights) / SUM(flights)          AS cancellation_rate
FROM airline_monthly_performance
WHERE month IN (:mois_sélectionnés)
GROUP BY airline, airline_name""",
    "hourly": f"""-- Notebook J3, table Gold hourly_delay_profile
SELECT month, scheduled_hour, departure_slot,
{INDICATORS_SQL}
FROM v_silver
GROUP BY month, scheduled_hour, departure_slot""",
    "segments": """# Notebook J4, table Gold airport_segments (agrégation Spark, puis scikit-learn)
entities = (silver.groupBy('origin_airport', 'origin_airport_name', 'origin_city', 'origin_state',
                           'origin_latitude', 'origin_longitude')
  .agg(F.count('*').alias('flights'), F.count('departure_delay_15').alias('observed_flights'),
       F.sum('cancelled').alias('cancelled_flights'),
       F.sum('departure_delay_15').alias('delayed_15_flights'),
       F.sum('positive_departure_delay').alias('total_delay_minutes'))
  .withColumn('delay_rate_15', F.expr('try_divide(delayed_15_flights, observed_flights)'))
  .withColumn('cancellation_rate', F.expr('try_divide(cancelled_flights, flights)'))
  .withColumn('mean_positive_delay', F.expr('try_divide(total_delay_minutes, observed_flights)')))
# Aéroports d'au moins 100 vols observés : log1p(flights), log1p(mean_positive_delay),
# StandardScaler sur les 4 variables, KMeans K = 2..6 (n_init=30, random_state=42),
# K retenu = meilleure silhouette avec des clusters d'au moins 10 aéroports.
# Les autres aéroports : cluster_id = -1, segment « Données limitées ».""",
}


# --------------------------------------------------------------------------- Lecture Gold
@st.cache_resource
def get_spark():
    spark = (
        SparkSession.builder
        .appName("StreamlitGoldReader")
        .master("local[2]")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # Tables Gold de quelques centaines de lignes : inutile de répartir la reconstruction de l'état
    # Delta (50 partitions par défaut) ni les shuffles (200) sur de nombreuses tâches.
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "1")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@st.cache_data(ttl=600, show_spinner="Lecture des tables Gold sur HDFS…")
def load_gold_table(name: str) -> pd.DataFrame:
    return get_spark().read.format("delta").load(f"{GOLD}/{name}").toPandas()


def load_all():
    data, missing = {}, []
    for key, name in TABLES.items():
        try:
            data[key] = load_gold_table(name)
        except Exception as exc:  # noqa: BLE001 — table absente ou HDFS indisponible : message affiché
            missing.append((name, str(exc).splitlines()[0][:200]))
    return data, missing


# --------------------------------------------------------------------------- Utilitaires
def pct(x, digits=1):
    return "—" if pd.isna(x) else f"{100 * x:.{digits}f} %".replace(".", ",")


def num(x):
    return "—" if pd.isna(x) else f"{int(x):,}".replace(",", " ")


def weighted(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Réagrège des taux à partir des numérateurs et dénominateurs (pondération par les effectifs)."""
    g = df.groupby(by, as_index=False)[
        ["flights", "cancelled_flights", "observed_flights", "delayed_15_flights", "total_delay_minutes"]
    ].sum()
    g["delay_rate_15"] = g.delayed_15_flights / g.observed_flights.where(g.observed_flights > 0)
    g["cancellation_rate"] = g.cancelled_flights / g.flights.where(g.flights > 0)
    return g


def explain(key: str, text: str):
    with st.expander("Requête SQL & Justification statistique"):
        st.code(SQL[key], language="python" if key == "segments" else "sql")
        st.markdown(text)


def style(fig, height=420):
    # Titre en haut de la marge, légende juste au-dessus de la zone de tracé : pas de chevauchement.
    fig.update_layout(height=height, margin={"l": 10, "r": 10, "t": 95, "b": 10},
                      title={"y": 0.99, "yanchor": "top", "x": 0, "xanchor": "left", "pad": {"l": 4}},
                      legend={"orientation": "h", "yanchor": "bottom", "y": 1.04, "x": 0, "title": None},
                      hoverlabel={"namelength": -1})
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.18)", zeroline=False)
    return fig


def segment_colors(segments: pd.Series) -> dict:
    colors, slot = {}, 0
    for seg in sorted(segments.unique()):
        if seg.startswith("Profil") and slot < len(SEGMENT_SLOTS):
            colors[seg] = SEGMENT_SLOTS[slot]
            slot += 1
        else:
            colors[seg] = NEUTRAL
    return colors


# --------------------------------------------------------------------------- Données
st.title("Retards des vols américains — janvier, avril, juillet et novembre 2015")
st.caption("Quels aéroports, compagnies et créneaux concentrent les retards ? "
           "Source : couche Gold du Lakehouse (HDFS + Delta Lake), calculée depuis 1 943 809 vols.")

data, missing = load_all()
if missing:
    st.error("Tables Gold indisponibles : exécuter les notebooks J3 puis J4 avant d'ouvrir le dashboard.")
    for name, err in missing:
        st.markdown(f"- `{GOLD}/{name}` : {err}")
    st.stop()

pareto, daily, airlines, hourly, segments = (data[k] for k in TABLES)
for df in (daily, airlines, hourly):
    df["month_name"] = df["month"].map(MONTHS)
daily["flight_date"] = pd.to_datetime(daily["flight_date"])
pareto = pareto.merge(
    segments[["origin_airport", "cluster_id", "segment", "mean_positive_delay"]],
    left_on="airport", right_on="origin_airport", how="left",
).drop(columns="origin_airport")
seg_colors = segment_colors(segments["segment"])

# --------------------------------------------------------------------------- Filtres
f1, f2, f3 = st.columns([1.2, 1.6, 1.2])
month_sel = f1.multiselect("Mois", list(MONTHS.values()), default=list(MONTHS.values()),
                           help="S'applique aux indicateurs clés, aux compagnies, à l'évolution "
                                "quotidienne et au profil horaire.")
airline_names = (airlines[["airline", "airline_name"]].drop_duplicates().sort_values("airline_name"))
airline_labels = {f"{r.airline_name} ({r.airline})": r.airline for r in airline_names.itertuples()}
airline_sel_labels = f2.multiselect("Compagnies mises en évidence", list(airline_labels),
                                    placeholder="Toutes les compagnies",
                                    help="Seule la table des compagnies conserve cette dimension : "
                                         "le filtre n'agit que sur les indicateurs clés et le classement.")
airline_sel = [airline_labels[x] for x in airline_sel_labels]
airport_opts = ["Tous les aéroports"] + [
    f"{r.airport} — {r.city}" for r in pareto.sort_values("flights", ascending=False).itertuples()
]
airport_choice = f3.selectbox("Aéroport", airport_opts,
                              help="Met en évidence l'aéroport sur la carte, le Pareto et les profils "
                                   "(indicateurs sur les quatre mois regroupés).")
airport_sel = None if airport_choice == airport_opts[0] else airport_choice.split(" — ")[0]

if not month_sel:
    st.warning("Aucun mois sélectionné : choisissez au moins un mois pour afficher les indicateurs mensuels.")
    st.stop()
month_ids = [m for m, name in MONTHS.items() if name in month_sel]

# --------------------------------------------------------------------------- Indicateurs clés
scope = airlines[airlines.month.isin(month_ids)]
if airline_sel:
    scope = scope[scope.airline.isin(airline_sel)]
tot = scope[["flights", "cancelled_flights", "observed_flights", "delayed_15_flights"]].sum()
k1, k2, k3, k4 = st.columns(4)
k1.metric("Vols", num(tot.flights))
k2.metric("Taux de retard ≥ 15 min", pct(tot.delayed_15_flights / tot.observed_flights)
          if tot.observed_flights else "—")
k3.metric("Taux d'annulation", pct(tot.cancelled_flights / tot.flights) if tot.flights else "—")
k4.metric("Vols observés (non annulés, retard connu)", num(tot.observed_flights))
st.caption(f"Périmètre : {', '.join(month_sel)}"
           + (f" ; compagnies : {', '.join(airline_sel)}" if airline_sel else " ; toutes compagnies")
           + ". Taux recalculés par somme des numérateurs / somme des dénominateurs.")

tab_map, tab_air, tab_time, tab_prof, tab_lim = st.tabs(
    ["Carte des aéroports", "Compagnies", "Évolution des retards", "Profils d'aéroports", "Méthode et limites"]
)

# --------------------------------------------------------------------------- 1. Carte
with tab_map:
    st.info("Carte calculée sur les quatre mois regroupés : elle ne réagit pas au filtre « Mois ».")
    color_mode = st.radio("Couleur des points", ["Profil K-Means", "Taux de retard ≥ 15 min"], horizontal=True)
    geo = pareto.dropna(subset=["latitude", "longitude"]).copy()
    no_coords = pareto[pareto.latitude.isna() | pareto.longitude.isna()]
    geo["Taux de retard"] = geo.delay_rate_15.map(pct)
    hover = {"latitude": False, "longitude": False, "city": True, "flights": ":,",
             "Taux de retard": True, "p90_delay": True, "segment": True}
    if color_mode == "Profil K-Means":
        fig = px.scatter_geo(geo, lat="latitude", lon="longitude", size="flights", size_max=28,
                             color="segment", color_discrete_map=seg_colors, hover_name="airport_name",
                             hover_data=hover, scope="usa",
                             category_orders={"segment": sorted(seg_colors)})
    else:
        fig = px.scatter_geo(geo, lat="latitude", lon="longitude", size="flights", size_max=28,
                             color="delay_rate_15", color_continuous_scale="Blues",
                             range_color=(0, geo.delay_rate_15.quantile(0.98)),
                             hover_name="airport_name", hover_data=hover, scope="usa")
        fig.update_coloraxes(colorbar={"title": "Taux ≥ 15 min", "tickformat": ".0%"})
    fig.update_traces(marker={"line": {"width": 0.6, "color": "white"}, "opacity": 0.85, "sizemin": 4})
    if airport_sel and airport_sel in set(geo.airport):
        a = geo[geo.airport == airport_sel]
        fig.add_trace(go.Scattergeo(lat=a.latitude, lon=a.longitude, mode="markers+text",
                                    text=a.airport, textposition="top center", showlegend=False,
                                    hoverinfo="skip",
                                    marker={"size": 22, "color": "rgba(0,0,0,0)",
                                                "line": {"width": 3, "color": "#e34948"}}))
    fig.update_geos(showland=True, landcolor="rgba(128,128,128,0.10)", showlakes=False,
                    bgcolor="rgba(0,0,0,0)", subunitcolor="rgba(128,128,128,0.4)",
                    fitbounds="locations")
    st.plotly_chart(style(fig, 640), width="stretch", config={"scrollZoom": False})
    st.caption(f"Taille = nombre de vols au départ. {len(no_coords)} aéroport(s) sans coordonnées, "
               f"absent(s) de la carte mais conservé(s) dans les classements : "
               + ", ".join(f"{r.airport} ({num(r.flights)} vols)" for r in no_coords.itertuples()) + ".")

    if airport_sel:
        r = pareto[pareto.airport == airport_sel].iloc[0]
        st.subheader(f"{r.airport} — {r.airport_name} ({r.city}, {r.state})")
        c = st.columns(6)
        c[0].metric("Vols", num(r.flights))
        c[1].metric("Taux ≥ 15 min", pct(r.delay_rate_15))
        c[2].metric("Médiane / P90", f"{r.median_delay:.0f} / {r.p90_delay:.0f} min")
        c[3].metric("Annulations", pct(r.cancellation_rate))
        c[4].metric("Rang (minutes cumulées)", f"{int(r.rank_total_minutes)} / {len(pareto)}")
        c[5].metric("Classe Pareto", r.pareto_class)
        st.markdown(f"Profil K-Means : **{r.segment}**")

    explain("pareto", """
**Indicateurs.** Le retard au départ est très asymétrique (skewness 7,4 ; moyenne ≈ 9 min mais médiane −2 min).
La carte montre donc le **taux de retard ≥ 15 min** (seuil de ponctualité du DOT), dont le dénominateur est
explicite : vols non annulés dont le retard est renseigné. La taille des points rappelle l'effectif, car un
taux calculé sur quelques centaines de vols est moins stable. Le **P90** (au survol) décrit la queue de
distribution ; la médiane, proche de zéro presque partout, discrimine peu.

**Lecture.** Un grand aéroport peut cumuler beaucoup de minutes de retard sans avoir un taux élevé :
il faut lire la couleur et la taille ensemble.

**Test associé (Jour 4).** Spearman entre la latitude (référentiel des aéroports) et le taux de retard,
sur 302 aéroports d'au moins 100 vols : ρ = −0,22 (p corrigée = 1,7 × 10⁻⁴). Les aéroports du sud ont
en moyenne un taux un peu plus élevé, mais le lien est faible et non significatif sur les seuls grands
aéroports (65 aéroports ≥ 5 000 vols) : association descriptive, pas une cause.
""")

# --------------------------------------------------------------------------- 2. Compagnies
with tab_air:
    sub = airlines[airlines.month.isin(month_ids)]
    agg = weighted(sub, ["airline", "airline_name"]).sort_values("delay_rate_15")
    agg["label"] = agg.airline_name + " (" + agg.airline + ")"
    agg["Taux"] = agg.delay_rate_15.map(pct)
    agg["Annulations"] = agg.cancellation_rate.map(pct)
    agg["mise_en_evidence"] = agg.airline.isin(airline_sel) if airline_sel else True
    colors = [HIGHLIGHT if h else NEUTRAL for h in agg.mise_en_evidence]
    fig = go.Figure(go.Bar(
        x=agg.delay_rate_15, y=agg.label, orientation="h", marker_color=colors,
        text=agg.Taux, textposition="outside", cliponaxis=False,
        customdata=agg[["flights", "Annulations"]],
        hovertemplate="<b>%{y}</b><br>Taux ≥ 15 min : %{text}<br>Vols : %{customdata[0]:,}"
                      "<br>Annulations : %{customdata[1]}<extra></extra>",
    ))
    overall = sub.delayed_15_flights.sum() / sub.observed_flights.sum()
    fig.add_vline(x=overall, line_dash="dot", line_color=NEUTRAL,
                  annotation_text=f"Ensemble : {pct(overall)}", annotation_position="top")
    fig.update_xaxes(tickformat=".0%", title="Taux de retard ≥ 15 min (pondéré par les vols)")
    fig.update_yaxes(title=None)
    fig.update_layout(title=f"Classement des compagnies — {', '.join(month_sel)}", showlegend=False,
                      bargap=0.35)
    st.plotly_chart(style(fig, 520), width="stretch")
    st.caption("Barres triées par taux. Les effectifs varient de quelques milliers à plus de "
               "100 000 vols par mois selon la compagnie : voir le survol et le tableau.")

    detail = sub if not airline_sel else sub[sub.airline.isin(airline_sel)]
    detail = detail.sort_values(["month", "rank_in_month"])
    st.markdown("**Détail mensuel** (médiane, IQR et P90 à leur granularité d'origine, non réagrégés)")
    st.dataframe(
        detail[["month_name", "rank_in_month", "airline", "airline_name", "flights", "delay_rate_15",
                "median_delay", "iqr_delay", "p90_delay", "cancellation_rate"]],
        hide_index=True, width="stretch",
        column_config={
            "month_name": "Mois", "rank_in_month": "Rang", "airline": "Code",
            "airline_name": "Compagnie", "flights": st.column_config.NumberColumn("Vols", format="localized"),
            "delay_rate_15": st.column_config.NumberColumn("Taux ≥ 15 min", format="percent"),
            "median_delay": st.column_config.NumberColumn("Médiane (min)", format="localized"),
            "iqr_delay": st.column_config.NumberColumn("IQR (min)", format="localized"),
            "p90_delay": st.column_config.NumberColumn("P90 (min)", format="localized"),
            "cancellation_rate": st.column_config.NumberColumn("Annulations", format="percent"),
        },
    )
    explain("airlines", """
**Pourquoi un taux pondéré ?** Pour plusieurs mois, le taux est recalculé par
`somme(vols retardés) / somme(vols observés)`. Faire la moyenne des taux mensuels donnerait le même poids
à un mois de 7 000 vols et à un mois de 46 000 vols.

**Pourquoi pas de médiane réagrégée ?** Une médiane ou un P90 ne s'additionnent pas : ils ne sont affichés
qu'au niveau compagnie × mois, tel que calculé par `PERCENTILE_APPROX` dans Spark.

**Prudence.** La compagnie au plus fort taux change selon le mois (MQ, F9, UA) : le classement décrit
cet extrait de 2015, sans désigner une compagnie structurellement moins ponctuelle. Les compagnies
desservent des aéroports et des créneaux différents.
""")

# --------------------------------------------------------------------------- 3. Évolution
with tab_time:
    d = daily[daily.month.isin(month_ids)].sort_values("flight_date")
    long = d.melt(id_vars=["flight_date", "month_name", "flights", "delay_rate_change_pts"],
                  value_vars=["delay_rate_15", "delay_rate_7d_avg"], var_name="Mesure", value_name="taux")
    long["Mesure"] = long.Mesure.map({"delay_rate_15": "Taux du jour",
                                      "delay_rate_7d_avg": "Moyenne glissante 7 jours (dans le mois)"})
    fig = px.line(long, x="flight_date", y="taux", color="Mesure", facet_col="month_name",
                  facet_col_wrap=2, facet_col_spacing=0.06, facet_row_spacing=0.12,
                  category_orders={"month_name": month_sel},
                  color_discrete_sequence=["#9cc0ec", "#2a78d6"],
                  custom_data=["flights", "delay_rate_change_pts"])
    fig.update_traces(line_width=2)
    fig.update_traces(selector={"name": "Taux du jour"}, mode="lines+markers", marker_size=4)
    fig.update_traces(hovertemplate="%{x|%d/%m/%Y}<br>%{y:.1%}<br>Vols : %{customdata[0]:,}"
                                    "<br>Variation vs veille : %{customdata[1]:+.1f} pts<extra></extra>")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_xaxes(matches=None, showticklabels=True, title=None, tickformat="%d/%m")
    fig.update_yaxes(tickformat=".0%", title=None)
    fig.update_layout(title="Taux de retard ≥ 15 min par jour — un panneau par mois (extrait discontinu)")
    rows = (len(month_sel) + 1) // 2
    st.plotly_chart(style(fig, 300 * rows + 80), width="stretch")
    peak = d.loc[d.delay_rate_15.idxmax()]
    jump = d.loc[d.delay_rate_change_pts.idxmax()] if d.delay_rate_change_pts.notna().any() else None
    st.caption(f"Jour le plus difficile du périmètre : {peak.flight_date:%d/%m/%Y} ({pct(peak.delay_rate_15)})."
               + (f" Plus forte hausse d'un jour sur l'autre : {jump.flight_date:%d/%m/%Y} "
                  f"({jump.delay_rate_change_pts:+.1f} points).".replace(".", ",", 1)
                  if jump is not None else ""))
    explain("daily", """
**Fenêtres par mois.** Les quatre mois ne sont pas contigus : `LAG` et la moyenne glissante sont calculés
avec `PARTITION BY month`, et chaque mois a son panneau. Relier le 31 janvier au 1er avril suggérerait une
continuité qui n'existe pas. Le premier jour de chaque mois n'a donc pas de variation.

**Unités.** `delay_rate_change_pts` est une différence de taux en **points de pourcentage**, pas une
variation relative. La moyenne glissante sur 7 jours lisse l'effet du jour de la semaine.

**Limite.** Ces pics sont des constats ; les tables agrégées ne permettent pas d'en attribuer la cause
(météo, panne, trafic).
""")

    st.divider()
    h = hourly[hourly.month.isin(month_ids)]
    hw = weighted(h, ["month_name", "scheduled_hour"])
    hw = hw[hw.observed_flights >= 30]  # heures quasi vides (nuit) : taux instable
    fig = px.line(hw, x="scheduled_hour", y="delay_rate_15", color="month_name", markers=True,
                  color_discrete_map=MONTH_COLORS, category_orders={"month_name": month_sel},
                  custom_data=["observed_flights"])
    fig.update_traces(line_width=2, marker_size=8,
                      hovertemplate="%{x} h — %{y:.1%}<br>Vols observés : %{customdata[0]:,}<extra></extra>")
    fig.update_xaxes(title="Heure de départ prévue (locale)", dtick=2, range=[-0.5, 23.5])
    fig.update_yaxes(tickformat=".0%", title="Taux de retard ≥ 15 min", rangemode="tozero")
    fig.update_layout(title="Profil horaire : le taux de retard augmente au fil de la journée")
    st.plotly_chart(style(fig, 420), width="stretch")
    slots = weighted(h, ["departure_slot"]).sort_values("delay_rate_15")
    st.caption("Par créneau, mois sélectionnés : " + " ; ".join(
        f"{r.departure_slot} {pct(r.delay_rate_15)}" for r in slots.itertuples())
        + ". Heures avec moins de 30 vols observés masquées.")
    explain("hourly", """
**Test associé (Jour 4).** Mann–Whitney sur 80 000 vols échantillonnés : la distribution du retard du soir
(18–23 h) diffère de celle du matin (06–11 h) ; delta de Cliff 0,22, H0 rejetée après correction de Holm.
Le bootstrap des 122 jours donne un écart moyen soir − matin de 8,7 min [7,4 ; 9,9].

**Limite.** Les créneaux ne regroupent pas les mêmes aéroports ni les mêmes compagnies, et les vols d'un
même jour partagent les mêmes perturbations : association, pas causalité. Le taux est réagrégé de façon
pondérée quand on combine des mois.
""")

# --------------------------------------------------------------------------- 4. Profils
with tab_prof:
    st.info("Profils et Pareto calculés sur les quatre mois regroupés : ils ne réagissent pas au filtre « Mois ».")
    left, right = st.columns(2)

    sc = pareto[pareto.observed_flights > 0].copy()
    fig = px.scatter(sc, x="flights", y="delay_rate_15", color="segment", log_x=True,
                     color_discrete_map=seg_colors, category_orders={"segment": sorted(seg_colors)},
                     hover_name="airport", custom_data=["city", "cancellation_rate", "mean_positive_delay"])
    fig.update_traces(marker={"size": 9, "opacity": 0.8, "line": {"width": 1, "color": "white"}},
                      hovertemplate="<b>%{hovertext}</b> — %{customdata[0]}<br>Vols : %{x:,}"
                                    "<br>Taux ≥ 15 min : %{y:.1%}<br>Annulations : %{customdata[1]:.1%}"
                                    "<br>Minutes de retard / vol : %{customdata[2]:.1f}<extra></extra>")
    if airport_sel:
        a = sc[sc.airport == airport_sel]
        fig.add_trace(go.Scatter(x=a.flights, y=a.delay_rate_15, mode="markers+text", text=a.airport,
                                 textposition="top center", showlegend=False, hoverinfo="skip",
                                 marker={"size": 18, "color": "rgba(0,0,0,0)", "line": {"width": 3, "color": "#e34948"}}))
    fig.update_xaxes(title="Vols au départ (échelle log)")
    fig.update_yaxes(tickformat=".0%", title="Taux de retard ≥ 15 min")
    fig.update_layout(title="Segments K-Means : volume et taux de retard",
                      legend={"orientation": "v", "y": -0.25, "yanchor": "top"})
    left.plotly_chart(style(fig, 520), width="stretch")

    p = pareto.sort_values("cumulative_share_airports")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=p.cumulative_share_airports, y=p.cumulative_share_minutes, mode="lines",
                             line={"color": HIGHLIGHT, "width": 2}, name="Part cumulée des minutes",
                             customdata=p[["airport", "rank_total_minutes", "pareto_class"]],
                             hovertemplate="%{customdata[0]} (rang %{customdata[1]}, classe %{customdata[2]})"
                                           "<br>%{x:.1%} des aéroports → %{y:.1%} des minutes<extra></extra>"))
    fig.add_hline(y=0.8, line_dash="dot", line_color=NEUTRAL)
    n_a = int((pareto.pareto_class == "A").sum())
    share_a = pareto.loc[pareto.pareto_class == "A", "total_delay_minutes"].sum() / pareto.total_delay_minutes.sum()
    fig.add_annotation(x=n_a / len(pareto), y=share_a, text=f"Classe A : {n_a} aéroports, {pct(share_a)}",
                       showarrow=True, arrowhead=0, ax=60, ay=40)
    if airport_sel:
        a = p[p.airport == airport_sel]
        fig.add_trace(go.Scatter(x=a.cumulative_share_airports, y=a.cumulative_share_minutes,
                                 mode="markers+text", text=a.airport, textposition="bottom right",
                                 marker={"size": 12, "color": "#e34948"}, showlegend=False, hoverinfo="skip"))
    fig.update_xaxes(tickformat=".0%", title="Part cumulée des aéroports (triés par minutes de retard)")
    fig.update_yaxes(tickformat=".0%", title="Part cumulée des minutes de retard", range=[0, 1.02])
    fig.update_layout(title="Pareto des minutes de retard au départ", showlegend=False)
    right.plotly_chart(style(fig, 520), width="stretch")

    prof = (segments.groupby("segment")
            .agg(aeroports=("origin_airport", "count"), vols_total=("flights", "sum"),
                 vols_median=("flights", "median"), taux_median=("delay_rate_15", "median"),
                 minutes_median=("mean_positive_delay", "median"), annul_median=("cancellation_rate", "median"))
            .reset_index().round({"minutes_median": 1}))
    st.markdown("**Profils** (médianes entre aéroports du groupe, non pondérées par le trafic)")
    st.dataframe(prof, hide_index=True, width="stretch", column_config={
        "segment": "Segment", "aeroports": "Aéroports",
        "vols_total": st.column_config.NumberColumn("Vols (total)", format="localized"),
        "vols_median": st.column_config.NumberColumn("Vols (médiane)", format="localized"),
        "taux_median": st.column_config.NumberColumn("Taux ≥ 15 min (médiane)", format="percent"),
        "minutes_median": st.column_config.NumberColumn("Minutes de retard / vol (médiane)", format="localized"),
        "annul_median": st.column_config.NumberColumn("Annulations (médiane)", format="percent"),
    })
    explain("segments", """
**Segmentation.** Quatre variables par aéroport : volume, taux de retard ≥ 15 min, minutes positives de
retard par vol observé et taux d'annulation. `log1p` est appliqué seulement aux deux mesures non négatives
asymétriques (volume, minutes), puis `StandardScaler` pour qu'aucune échelle ne domine la distance
euclidienne. K = 3 retenu (silhouette 0,30, à peine devant K = 5) : la séparation est **modérée**, les
segments sont une aide descriptive, pas des catégories naturelles. Les aéroports de moins de 100 vols
observés ne sont pas entraînés (« Données limitées », `cluster_id = -1`).

**Pareto** (requête J3 ci-dessus pour `airport_delay_pareto`, colonnes `cumulative_share_*` et
`pareto_class`). Les aéroports de classe A concentrent un peu plus de 80 % des minutes, mais aussi environ
78 % des vols : la concentration reflète d'abord celle du trafic. D'où le nuage de gauche, qui sépare
volume et taux.
""")

# --------------------------------------------------------------------------- 5. Limites
with tab_lim:
    st.markdown("""
### Architecture
Le dashboard lit uniquement cinq tables Delta de `/lakehouse/gold/` (≤ 322 lignes chacune), produites par
les notebooks J3 et J4 à partir de Silver. Aucun CSV ni aucune table Silver n'est chargé ici : les
agrégations lourdes restent dans Spark, l'application ne fait que filtrer et réagréger des compteurs.

### Indicateurs et dénominateurs
- **Taux de retard ≥ 15 min** = vols retardés d'au moins 15 min / vols non annulés dont le retard est renseigné.
- **Taux d'annulation** = vols annulés / tous les vols.
- **Médiane, IQR, P90** : préférés à la moyenne, tirée vers le haut par une longue traîne (skewness 7,4).
  Ils ne sont pas réagrégés entre groupes.
- Les retards négatifs (avances) sont conservés ; une valeur manquante n'est jamais remplacée par zéro.

### Portée des filtres
| Filtre | Agit sur | N'agit pas sur |
|---|---|---|
| Mois | indicateurs clés, compagnies, évolution quotidienne, profil horaire | carte, Pareto, segments (4 mois regroupés) |
| Compagnies | indicateurs clés, mise en évidence du classement, détail mensuel | autres vues (dimension absente des tables) |
| Aéroport | mise en évidence sur la carte, le nuage et le Pareto ; fiche aéroport | tables mensuelles (pas de dimension aéroport) |

### Limites
- Quatre mois de 2015 seulement (novembre inclut Thanksgiving) : ni une année entière, ni la situation actuelle.
- Associations statistiques, pas de démonstration causale ; les vols partagent dates, aéroports et perturbations.
- Les segments K-Means décrivent des profils relatifs à cet extrait, pas un niveau de qualité universel.
- Trois aéroports (ECP, PBG, UST) n'ont pas de coordonnées et n'apparaissent pas sur la carte.
""")
