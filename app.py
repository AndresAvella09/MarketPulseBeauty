import streamlit as st
import pandas as pd
from pathlib import Path
from nltk.sentiment import SentimentIntensityAnalyzer

st.set_page_config(page_title="MarketPulse Beauty", layout="wide")

P_PRODUCTS = Path("data/local/gold/products.parquet")
P_REVIEWS = Path("data/local/gold/reviews.parquet")
P_TRENDS = Path("data/local/gold/trends.parquet")

@st.cache_data(show_spinner=False)
def read_parquet_safe(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)

def stop_if_missing_or_empty(df: pd.DataFrame, label: str):
    if df is None or df.empty:
        st.error(f"{label}: archivo no existe o está vacío.")
        st.stop()

def to_dt(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    try:
        return dt.dt.tz_convert(None)
    except Exception:
        return dt

def week_start(dt: pd.Series) -> pd.Series:
    return dt.dt.to_period("W-MON").apply(lambda p: p.start_time)

def infer_keyword_from_name(name: str) -> str | None:
    s = str(name).lower()
    if "niacin" in s:
        return "niacinamida"
    if "hialur" in s or "hyalur" in s:
        return "acido_hialuronico"
    if "sulfat" in s or "sulfate" in s or "shampoo" in s:
        return "shampoo_sin_sulfatos"
    return None

@st.cache_data(show_spinner=False)
def add_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "sentiment_label" in out.columns and "sentiment_score" in out.columns:
        return out

    if "ReviewText" not in out.columns:
        out["sentiment_score"] = 0.0
        out["sentiment_label"] = "neutral"
        return out

    sia = SentimentIntensityAnalyzer()
    texts = out["ReviewText"].fillna("").astype(str)

    scores = texts.apply(lambda t: sia.polarity_scores(t)["compound"])
    labels = scores.apply(lambda x: "positive" if x >= 0.05 else ("negative" if x <= -0.05 else "neutral"))

    out["sentiment_score"] = scores
    out["sentiment_label"] = labels
    return out

def health_score_week(g: pd.DataFrame) -> float:
    rating_part = float(g["Rating"].mean()) / 5.0 if g["Rating"].notna().any() else 0.0

    rec_part = 0.5
    if "IsRecommended" in g.columns and g["IsRecommended"].notna().any():
        rec_bool = g["IsRecommended"].astype(str).str.lower().map(
            {"true": True, "false": False}
        )
        if rec_bool.notna().any():
            rec_part = float(rec_bool.mean())

    score = 100.0 * (0.70 * rating_part + 0.30 * rec_part)
    return max(0.0, min(100.0, score))

@st.cache_data(show_spinner=False)
def build_weekly_metrics(reviews: pd.DataFrame) -> pd.DataFrame:
    r = reviews.dropna(subset=["event_time"]).copy()
    r["week"] = week_start(r["event_time"])

    rows = []
    for (pid, w), g in r.groupby(["ProductID", "week"]):
        rec_rate = None
        if "IsRecommended" in g.columns and g["IsRecommended"].notna().any():
            rec_bool = g["IsRecommended"].astype(str).str.lower().map(
                {"true": True, "false": False}
            )
            if rec_bool.notna().any():
                rec_rate = float(rec_bool.mean())

        rows.append({
            "ProductID": pid,
            "week": w,
            "n_reviews": int(len(g)),
            "rating_avg": float(g["Rating"].mean()) if g["Rating"].notna().any() else None,
            "rec_rate": rec_rate,
            "health_score": health_score_week(g),
        })
    return pd.DataFrame(rows).sort_values(["ProductID", "week"])

@st.cache_data(show_spinner=False)
def build_alerts(weekly: pd.DataFrame, threshold: float) -> pd.DataFrame:
    if weekly.empty:
        return weekly
    w = weekly.sort_values(["ProductID", "week"]).copy()
    w["prev_health"] = w.groupby("ProductID")["health_score"].shift(1)
    w["drop"] = w["prev_health"] - w["health_score"]
    return w[w["drop"] >= threshold].copy().sort_values(["drop", "week"], ascending=[False, False])

def build_context(product_name, prod_df, prod_week, latest_health, delta):
    lines = []
    tone = "info"

    rating_avg = float(prod_df["Rating"].mean()) if prod_df["Rating"].notna().any() else None
    total_reviews = len(prod_df)

    dominant_sentiment = (
        prod_df["sentiment_label"].mode().iloc[0]
        if not prod_df.empty and prod_df["sentiment_label"].notna().any()
        else "neutral"
    )

    polarization = None
    if "polarization_score" in prod_df.columns and prod_df["polarization_score"].notna().any():
        polarization = float(prod_df["polarization_score"].dropna().iloc[0])

    if latest_health >= 80:
        lines.append(f"**{product_name}** muestra un estado general fuerte, con un Health Score alto.")
        tone = "success"
    elif latest_health >= 60:
        lines.append(f"**{product_name}** muestra un desempeño estable, aunque con margen de mejora.")
        tone = "info"
    else:
        lines.append(f"**{product_name}** presenta señales de riesgo y requiere revisión prioritaria.")
        tone = "warning"

    if delta is not None:
        if delta > 3:
            lines.append(f"En la última comparación semanal, el Health Score subió **{delta:.1f} puntos**, lo que sugiere una mejora reciente.")
        elif delta < -3:
            lines.append(f"En la última comparación semanal, el Health Score cayó **{abs(delta):.1f} puntos**, lo que indica deterioro reciente.")
        else:
            lines.append("El Health Score se mantuvo relativamente estable entre las últimas semanas.")

    if dominant_sentiment == "positive":
        lines.append("El sentimiento dominante en las reseñas del rango actual es **positivo**, lo que respalda la percepción favorable del producto.")
    elif dominant_sentiment == "negative":
        lines.append("El sentimiento dominante en las reseñas del rango actual es **negativo**, lo que puede explicar señales de deterioro.")
    else:
        lines.append("El sentimiento dominante en las reseñas del rango actual es **neutral**, sin una inclinación fuerte.")

    if rating_avg is not None:
        if rating_avg >= 4.2:
            lines.append(f"El rating promedio actual es **{rating_avg:.2f}**, lo que sugiere una recepción sólida por parte de los usuarios.")
        elif rating_avg >= 3.5:
            lines.append(f"El rating promedio actual es **{rating_avg:.2f}**, indicando una experiencia aceptable pero no sobresaliente.")
        else:
            lines.append(f"El rating promedio actual es **{rating_avg:.2f}**, señalando una experiencia más problemática.")

    if polarization is not None:
        if polarization >= 0.75:
            lines.append(f"La polarización del producto es **alta ({polarization:.3f})**, por lo que conviene revisar si existen opiniones muy divididas.")
        elif polarization >= 0.5:
            lines.append(f"La polarización del producto es **media ({polarization:.3f})**, con cierta dispersión en la experiencia del usuario.")
        else:
            lines.append(f"La polarización del producto es **baja ({polarization:.3f})**, lo que indica una percepción más consistente.")

    if total_reviews >= 500:
        lines.append(f"El análisis se apoya en un volumen alto de reseñas (**{total_reviews}**), por lo que la lectura actual es más confiable.")
    elif total_reviews >= 100:
        lines.append(f"El análisis se apoya en un volumen moderado de reseñas (**{total_reviews}**).")
    else:
        lines.append(f"El volumen de reseñas dentro del rango actual es reducido (**{total_reviews}**), así que la interpretación debe tomarse con cautela.")

    return tone, lines

st.title("MarketPulse Beauty Dashboard")

with st.sidebar:
    st.header("Datos locales")
    products_path = st.text_input("products.parquet", str(P_PRODUCTS))
    reviews_path = st.text_input("reviews.parquet", str(P_REVIEWS))
    trends_path = st.text_input("trends.parquet", str(P_TRENDS))

products = read_parquet_safe(products_path)
reviews = read_parquet_safe(reviews_path)
trends = read_parquet_safe(trends_path)

stop_if_missing_or_empty(products, "products.parquet")
stop_if_missing_or_empty(reviews, "reviews.parquet")
stop_if_missing_or_empty(trends, "trends.parquet")

if "ProductID" not in products.columns or "ProductID" not in reviews.columns:
    st.error("Se requiere ProductID en products y reviews.")
    st.stop()

if "Rating" not in reviews.columns:
    st.error("Se requiere Rating en reviews.")
    st.stop()

time_col = "SubmissionTime" if "SubmissionTime" in reviews.columns else ("LastModTime" if "LastModTime" in reviews.columns else None)
if time_col is None:
    st.error("No se encontró SubmissionTime ni LastModTime en reviews.")
    st.stop()

reviews = reviews.copy()
reviews["event_time"] = to_dt(reviews[time_col])
reviews = add_sentiment(reviews)

prod_keep = [c for c in ["ProductID", "ProductName", "Brand", "AvgRating", "TotalReviewCount", "polarization_score"] if c in products.columns]
df = reviews.merge(products[prod_keep], on="ProductID", how="left")

min_dt = df["event_time"].min()
max_dt = df["event_time"].max()

with st.sidebar:
    st.header("Filtro global de fechas")
    date_start, date_end = st.date_input(
        "Rango",
        value=(min_dt.date(), max_dt.date()),
        min_value=min_dt.date(),
        max_value=max_dt.date()
    )

mask = (df["event_time"].dt.date >= date_start) & (df["event_time"].dt.date <= date_end)
df_f = df[mask].copy()
if df_f.empty:
    st.warning("Con este rango no hay reseñas.")
    st.stop()

if not {"keyword", "week", "interest"}.issubset(set(trends.columns)):
    st.error("trends.parquet debe tener columnas keyword, week, interest.")
    st.stop()

tr = trends.copy()
tr["week"] = pd.to_datetime(tr["week"], errors="coerce")
tr = tr.dropna(subset=["week"])
tr = tr[(tr["week"].dt.date >= date_start) & (tr["week"].dt.date <= date_end)]

with st.sidebar:
    st.header("Vista")
    view = st.radio("Selecciona", ["Producto", "Comparativa"])

weekly = build_weekly_metrics(df_f)

with st.sidebar:
    st.header("Alertas")
    threshold = st.slider("Umbral caída (pts)", min_value=1, max_value=50, value=10, step=1)

alerts = build_alerts(weekly, threshold)

if view == "Producto":
    prod_list = (
        df_f[["ProductID", "ProductName"]]
        .dropna()
        .drop_duplicates()
        .sort_values("ProductName")
    )
    prod_map = dict(zip(prod_list["ProductName"], prod_list["ProductID"]))

    with st.sidebar:
        st.header("Producto")
        selected_name = st.selectbox("Selecciona producto", list(prod_map.keys()))
        selected_id = prod_map[selected_name]

    prod_df = df_f[df_f["ProductID"] == selected_id].copy()
    prod_week = weekly[weekly["ProductID"] == selected_id].sort_values("week")

    latest_health = float(prod_week["health_score"].iloc[-1]) if not prod_week.empty else 0.0
    prev_health = float(prod_week["health_score"].iloc[-2]) if len(prod_week) >= 2 else None
    delta = latest_health - prev_health if prev_health is not None else None

    st.subheader("Resumen ejecutivo")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rating promedio", f"{prod_df['Rating'].mean():.2f}" if prod_df["Rating"].notna().any() else "N/A")
    c2.metric("AvgRating producto", f"{prod_df['AvgRating'].dropna().iloc[0]:.2f}" if "AvgRating" in prod_df.columns and prod_df["AvgRating"].notna().any() else "N/A")
    c3.metric("Health Score", f"{latest_health:.1f}", f"{delta:+.1f}" if delta is not None else None)
    c4.metric("# reseñas", f"{len(prod_df)}")

    tone, context_lines = build_context(selected_name, prod_df, prod_week, latest_health, delta)
    if tone == "success":
        st.success(context_lines[0])
    elif tone == "warning":
        st.warning(context_lines[0])
    else:
        st.info(context_lines[0])

    st.divider()

    st.subheader("Qué cambió recientemente")

    col1, col2 = st.columns(2)
    with col1:
        if not prod_week.empty:
            st.caption("Rating promedio semanal")
            st.line_chart(prod_week.set_index("week")[["rating_avg"]], height=260)
        else:
            st.info("No hay semanas suficientes para graficar el rating.")

    with col2:
        if not prod_week.empty:
            st.caption("Health Score semanal")
            st.line_chart(prod_week.set_index("week")[["health_score"]], height=260)
        else:
            st.info("No hay semanas suficientes para graficar el Health Score.")

    st.divider()

    st.subheader("Qué explica el estado actual del producto")

    ctx_col1, ctx_col2 = st.columns([1.2, 1])
    with ctx_col1:
        st.markdown("\n".join(context_lines[1:] if len(context_lines) > 1 else context_lines))

    with ctx_col2:
        st.caption("Distribución de sentimiento")
        sent_counts = prod_df["sentiment_label"].value_counts()
        st.bar_chart(sent_counts, height=240)

    st.divider()

    st.subheader("Contexto de tendencia")

    kw_list = sorted(tr["keyword"].unique().tolist()) if not tr.empty else []
    default_kw = infer_keyword_from_name(selected_name)
    if kw_list:
        kw_index = kw_list.index(default_kw) if default_kw in kw_list else 0
        selected_kw = st.selectbox("Keyword asociada al producto", kw_list, index=kw_index)
        tr_kw = tr[tr["keyword"] == selected_kw].sort_values("week")
        st.line_chart(tr_kw.set_index("week")[["interest"]], height=260)
    else:
        st.info("No hay trends en el rango seleccionado.")

    st.divider()

    st.subheader("Riesgos y alertas")

    if alerts.empty:
        st.info("No hay alertas con el umbral actual.")
    else:
        name_map = products.set_index("ProductID")["ProductName"].to_dict()
        alerts_show = alerts.copy()
        alerts_show["ProductName"] = alerts_show["ProductID"].map(name_map).fillna(alerts_show["ProductID"].astype(str))
        alerts_show = alerts_show[["week", "ProductName", "prev_health", "health_score", "drop", "n_reviews"]]
        alerts_show = alerts_show.rename(columns={
            "prev_health": "Health semana anterior",
            "health_score": "Health semana actual",
            "drop": "Caída (pts)",
            "n_reviews": "#Reviews semana"
        })
        st.dataframe(alerts_show, use_container_width=True)

    st.divider()

    st.subheader("Detalle de reseñas")
    detail_cols = [c for c in ["ReviewID", "Rating", "Title", "ReviewText", "SubmissionTime", "sentiment_label", "sentiment_score"] if c in prod_df.columns]
    st.dataframe(prod_df[detail_cols].head(100), use_container_width=True)

else:
    prod_list = (
        df_f[["ProductID", "ProductName"]]
        .dropna()
        .drop_duplicates()
        .sort_values("ProductName")
    )
    all_names = prod_list["ProductName"].tolist()

    with st.sidebar:
        st.header("Comparativa")
        selected_names = st.multiselect("Selecciona 2 o más productos", all_names, default=all_names[:2])

    if len(selected_names) < 2:
        st.info("Selecciona al menos 2 productos para comparar.")
        st.stop()

    compare_ids = prod_list[prod_list["ProductName"].isin(selected_names)]["ProductID"].tolist()
    compare_df = df_f[df_f["ProductID"].isin(compare_ids)].copy()
    compare_week = weekly[weekly["ProductID"].isin(compare_ids)].copy()

    name_map = prod_list.set_index("ProductID")["ProductName"].to_dict()
    compare_week["ProductName"] = compare_week["ProductID"].map(name_map)

    st.subheader("Health Score comparado")
    hs_pivot = compare_week.pivot(index="week", columns="ProductName", values="health_score")
    st.line_chart(hs_pivot, height=280)

    st.subheader("Volumen de reseñas por semana")
    vol_pivot = compare_week.pivot(index="week", columns="ProductName", values="n_reviews")
    st.line_chart(vol_pivot, height=280)

    st.subheader("Distribución de sentimiento")
    sent_cmp = (
        compare_df.groupby(["ProductName", "sentiment_label"])
        .size()
        .reset_index(name="count")
        .pivot(index="sentiment_label", columns="ProductName", values="count")
        .fillna(0)
    )
    st.bar_chart(sent_cmp, height=280)

    st.subheader("Google Trends comparado")
    if tr.empty:
        st.info("No hay trends en el rango seleccionado.")
    else:
        cols = st.columns(len(selected_names))
        keyword_assignments = {}
        kw_list = sorted(tr["keyword"].unique().tolist())

        for i, product_name in enumerate(selected_names):
            inferred = infer_keyword_from_name(product_name)
            idx = kw_list.index(inferred) if inferred in kw_list else 0
            keyword_assignments[product_name] = cols[i].selectbox(
                f"Keyword para {product_name}",
                kw_list,
                index=idx,
                key=f"kw_{product_name}"
            )

        frames = []
        for product_name, kw in keyword_assignments.items():
            sub = tr[tr["keyword"] == kw].copy()
            sub["ProductName"] = product_name
            frames.append(sub)

        if frames:
            tc = pd.concat(frames, ignore_index=True)
            tc_pivot = tc.pivot(index="week", columns="ProductName", values="interest")
            st.line_chart(tc_pivot, height=280)