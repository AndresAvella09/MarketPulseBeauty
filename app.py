import streamlit as st
from pathlib import Path

from src.visualization.config import PRODUCTS_PATH, REVIEWS_PATH
from src.visualization.data_loader import (
    load_parquet,
    prepare_dashboard_data,
    validate_inputs,
)
from src.visualization.views import (
    render_kpis,
    render_rating_distribution,
    render_reviews_table,
    render_sidebar,
    render_time_series,
)

st.set_page_config(page_title="MarketPulse Beauty", layout="wide")

st.title("MarketPulse Beauty — Streamlit Prototype")

with st.sidebar:
    st.header("Datos (local)")
    st.caption("Lee silver local en data/local/silver/*.parquet (no se sube al repo).")
    products_path = Path(st.text_input("products.parquet", str(PRODUCTS_PATH)))
    reviews_path = Path(st.text_input("reviews.parquet", str(REVIEWS_PATH)))

try:
    validate_inputs(products_path, reviews_path)
    products = load_parquet(products_path)
    reviews = load_parquet(reviews_path)
    products, df = prepare_dashboard_data(products, reviews)
except Exception as e:
    st.error("No pude cargar los datos del dashboard.")
    st.exception(e)
    st.stop()

selected_name, selected_id = render_sidebar(df)

prod_reviews = df[df["ProductID"] == selected_id].copy()
prod_row = products[products["ProductID"] == selected_id].head(1)
prod_row = prod_row.iloc[0] if len(prod_row) else None

render_kpis(prod_reviews, prod_row)
st.divider()
render_time_series(prod_reviews)
st.divider()
render_rating_distribution(prod_reviews)
render_reviews_table(prod_reviews)