from __future__ import annotations

import streamlit as st

from src.dashboard.data_loader import load_gold_data
from src.dashboard.sections import (
    render_business_questions,
    render_overview,
    render_products_section,
)
from src.dashboard.utils import pretty_keyword


st.set_page_config(
    page_title="MarketPulseBeauty",
    page_icon="💄",
    layout="wide",
)


@st.cache_data(show_spinner=False)
def cached_load_data():
    return load_gold_data()


def apply_filters(products, reviews):
    st.sidebar.header("Filtros globales")

    keyword_options = sorted(products["focus_keyword"].dropna().unique().tolist())
    selected_keywords = st.sidebar.multiselect(
        "Familias objetivo",
        options=keyword_options,
        default=keyword_options,
        format_func=pretty_keyword,
    )

    filtered_products = products.loc[products["focus_keyword"].isin(selected_keywords)].copy()
    filtered_reviews = reviews.loc[reviews["focus_keyword"].isin(selected_keywords)].copy()

    if "Brand" in filtered_products.columns:
        brand_options = sorted(filtered_products["Brand"].dropna().unique().tolist())
        selected_brands = st.sidebar.multiselect(
            "Marca",
            options=brand_options,
            default=brand_options,
        )
        filtered_products = filtered_products.loc[filtered_products["Brand"].isin(selected_brands)].copy()
        filtered_reviews = filtered_reviews.loc[filtered_reviews["Brand"].isin(selected_brands)].copy()

    product_options = sorted(filtered_products["ProductName"].dropna().unique().tolist())
    selected_products = st.sidebar.multiselect(
        "Productos",
        options=product_options,
        default=product_options,
    )
    filtered_products = filtered_products.loc[filtered_products["ProductName"].isin(selected_products)].copy()
    filtered_reviews = filtered_reviews.loc[filtered_reviews["ProductName"].isin(selected_products)].copy()

    if "SubmissionDate" in filtered_reviews.columns and filtered_reviews["SubmissionDate"].notna().any():
        min_date = filtered_reviews["SubmissionDate"].min().date()
        max_date = filtered_reviews["SubmissionDate"].max().date()

        selected_range = st.sidebar.date_input(
            "Rango de fechas",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
            filtered_reviews = filtered_reviews.loc[
                filtered_reviews["SubmissionDate"].dt.date.between(start_date, end_date)
            ].copy()

    return filtered_products, filtered_reviews


def main():
    st.title("MarketPulseBeauty Dashboard")
    st.caption("Dashboard enfocado únicamente en niacinamida, ácido hialurónico y shampoo sin sulfatos.")

    try:
        products, reviews = cached_load_data()
    except Exception as exc:
        st.error(f"Error cargando datos: {exc}")
        st.stop()

    filtered_products, filtered_reviews = apply_filters(products, reviews)

    if filtered_products.empty or filtered_reviews.empty:
        st.warning("Con los filtros actuales no hay datos para mostrar.")
        st.stop()

    tab1, tab2, tab3 = st.tabs(
        ["Resumen", "Productos", "Preguntas de negocio"]
    )

    with tab1:
        render_overview(filtered_products, filtered_reviews)

    with tab2:
        render_products_section(filtered_products, filtered_reviews)

    with tab3:
        render_business_questions(filtered_products, filtered_reviews)


if __name__ == "__main__":
    main()