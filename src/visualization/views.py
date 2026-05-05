import streamlit as st
import pandas as pd

from src.visualization.metrics import compute_health_score


def render_sidebar(df: pd.DataFrame):
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
    return selected_name, selected_id


def render_kpis(prod_reviews: pd.DataFrame, prod_row: pd.Series | None):
    c1, c2, c3, c4 = st.columns(4)

    rating_avg_reviews = (
        float(prod_reviews["Rating"].mean()) if prod_reviews["Rating"].notna().any() else None
    )
    health = compute_health_score(prod_reviews, prod_row)
    n_reviews = int(len(prod_reviews))
    avg_rating_product = (
        float(prod_row["AvgRating"])
        if prod_row is not None and pd.notna(prod_row.get("AvgRating"))
        else None
    )

    c1.metric(
        "Rating promedio (reseñas)",
        f"{rating_avg_reviews:.2f}" if rating_avg_reviews is not None else "N/A",
    )
    c2.metric(
        "AvgRating (producto)",
        f"{avg_rating_product:.2f}" if avg_rating_product is not None else "N/A",
    )
    c3.metric("Health Score (0-100)", f"{health:.1f}")
    c4.metric("N° reseñas", f"{n_reviews}")


def render_time_series(prod_reviews: pd.DataFrame):
    st.subheader("Evolución temporal")

    if "SubmissionTime" in prod_reviews.columns and prod_reviews["SubmissionTime"].notna().any():
        tmp = (
            prod_reviews.dropna(subset=["SubmissionTime"])
            .sort_values("SubmissionTime")
            .set_index("SubmissionTime")
        )

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


def render_rating_distribution(prod_reviews: pd.DataFrame):
    st.subheader("Distribución de ratings")
    rating_counts = prod_reviews["Rating"].value_counts().sort_index()
    st.bar_chart(rating_counts, height=240)


def render_reviews_table(prod_reviews: pd.DataFrame):
    with st.expander("Ver muestra de reseñas"):
        show_cols = [
            c for c in [
                "ReviewID",
                "Rating",
                "Title",
                "ReviewText",
                "SubmissionTime",
                "IsRecommended",
                "HelpfulCount",
            ]
            if c in prod_reviews.columns
        ]
        st.dataframe(prod_reviews[show_cols].head(25), width="stretch")