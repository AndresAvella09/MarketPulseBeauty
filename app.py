import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="MarketPulse Beauty", layout="wide")

DEFAULT_PRODUCTS = Path("data/local/silver/products.parquet")
DEFAULT_REVIEWS = Path("data/local/silver/reviews.parquet")

@st.cache_data(show_spinner=False)
def load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)

def to_datetime_safe(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    try:
        return dt.dt.tz_convert(None)
    except Exception:
        return dt

def compute_health_score(prod_reviews: pd.DataFrame, prod_row: pd.Series | None) -> float:
    """
    Health score (0..100) simple y defendible con tus columnas:
    - 70% rating promedio por reseña (Rating)
    - 30% % recomendación (IsRecommended) si existe
    """
    rating_part = 0.0
    if "Rating" in prod_reviews.columns and prod_reviews["Rating"].notna().any():
        rating_part = float(prod_reviews["Rating"].mean()) / 5.0  # 0..1

    rec_part = 0.5
    if "IsRecommended" in prod_reviews.columns and prod_reviews["IsRecommended"].notna().any():
        # viene booleano → promedio = % true
        rec_part = float(prod_reviews["IsRecommended"].astype(bool).mean())

    score = 100.0 * (0.70 * rating_part + 0.30 * rec_part)
    return max(0.0, min(100.0, score))

st.title("MarketPulse Beauty — Streamlit Prototype (Issue #29)")

with st.sidebar:
    st.header("Datos (local)")
    st.caption("Lee silver local en data/local/silver/*.parquet (no se sube al repo).")
    products_path = Path(st.text_input("products.parquet", str(DEFAULT_PRODUCTS)))
    reviews_path = Path(st.text_input("reviews.parquet", str(DEFAULT_REVIEWS)))

if not products_path.exists() or not reviews_path.exists():
    st.error("No encuentro los parquet. Verifica rutas en el panel izquierdo.")
    st.stop()

try:
    products = load_parquet(products_path)
    reviews = load_parquet(reviews_path)
except Exception as e:
    st.error("No pude leer parquet. Asegúrate de tener `pyarrow` instalado.")
    st.exception(e)
    st.stop()

# Join FIXO con tus columnas reales
if "ProductID" not in products.columns or "ProductID" not in reviews.columns:
    st.error("No existe ProductID en products o reviews. No puedo unir.")
    st.stop()

# Normalizar columnas clave para dashboard
products = products.copy()
reviews = reviews.copy()

# Asegurar datetime
if "SubmissionTime" in reviews.columns:
    reviews["SubmissionTime"] = to_datetime_safe(reviews["SubmissionTime"])

# Merge: reviews + products
df = reviews.merge(
    products[["ProductID", "ProductName", "Brand", "AvgRating", "TotalReviewCount", "CategoryId", "ProductPageUrl"]],
    on="ProductID",
    how="left"
)

# Selector producto
with st.sidebar:
    st.header("Filtros")
    product_list = (
        df[["ProductID", "ProductName"]]
        .dropna()
        .drop_duplicates()
        .sort_values("ProductName")
    )
    product_map = dict(zip(product_list["ProductName"], product_list["ProductID"]))
    selected_name = st.selectbox("Producto", list(product_map.keys()))
    selected_id = product_map[selected_name]

prod_reviews = df[df["ProductID"] == selected_id].copy()
prod_row = products[products["ProductID"] == selected_id].head(1)
prod_row = prod_row.iloc[0] if len(prod_row) else None

# KPIs
c1, c2, c3, c4 = st.columns(4)

rating_avg_reviews = float(prod_reviews["Rating"].mean()) if prod_reviews["Rating"].notna().any() else None
health = compute_health_score(prod_reviews, prod_row)
n_reviews = int(len(prod_reviews))
avg_rating_product = float(prod_row["AvgRating"]) if prod_row is not None and pd.notna(prod_row.get("AvgRating")) else None

c1.metric("Rating promedio (reseñas)", f"{rating_avg_reviews:.2f}" if rating_avg_reviews is not None else "N/A")
c2.metric("AvgRating (producto)", f"{avg_rating_product:.2f}" if avg_rating_product is not None else "N/A")
c3.metric("Health Score (0-100)", f"{health:.1f}")
c4.metric("N° reseñas", f"{n_reviews}")

st.divider()

# Evolución temporal (rating + volumen semanal)
st.subheader("Evolución temporal")
if "SubmissionTime" in prod_reviews.columns and prod_reviews["SubmissionTime"].notna().any():
    tmp = prod_reviews.dropna(subset=["SubmissionTime"]).sort_values("SubmissionTime").set_index("SubmissionTime")

    colA, colB = st.columns(2)
    with colA:
        if "Rating" in tmp.columns and tmp["Rating"].notna().any():
            st.caption("Rating promedio semanal")
            st.line_chart(tmp["Rating"].resample("W").mean(), height=260)
        else:
            st.info("No hay Rating para graficar.")

    with colB:
        st.caption("Volumen de reseñas semanal")
        st.line_chart(tmp.resample("W").size(), height=260)
else:
    st.info("No hay SubmissionTime para graficar evolución temporal.")

st.divider()

# Distribución de sentimiento (no existe aún en tus silver)
st.subheader("Distribución de sentimiento")
st.info("Aún no hay columnas de sentimiento en los parquets silver (sentiment_label/sentiment_score). "
        "Cuando estén disponibles en la capa silver/gold, se puede agregar la distribución aquí.")

st.divider()

# Distribución de ratings
st.subheader("Distribución de ratings")
rating_counts = prod_reviews["Rating"].value_counts().sort_index()
st.bar_chart(rating_counts, height=240)

# Tabla de muestra
with st.expander("Ver muestra de reseñas"):
    show_cols = [c for c in ["ReviewID", "Rating", "Title", "ReviewText", "SubmissionTime", "IsRecommended", "HelpfulCount"] if c in prod_reviews.columns]
    st.dataframe(prod_reviews[show_cols].head(25))