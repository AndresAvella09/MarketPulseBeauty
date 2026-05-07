from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard.utils import pretty_keyword


def render_kpis(products: pd.DataFrame, reviews: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Productos foco", int(products["ProductID"].nunique()) if not products.empty else 0)
    c2.metric("Reviews foco", int(len(reviews)))
    c3.metric(
        "Marcas",
        int(products["Brand"].nunique()) if "Brand" in products.columns and not products.empty else 0,
    )
    c4.metric(
        "Rating promedio",
        round(float(reviews["Rating"].mean()), 2) if "Rating" in reviews.columns and reviews["Rating"].notna().any() else 0,
    )


def render_overview(products: pd.DataFrame, reviews: pd.DataFrame) -> None:
    st.subheader("Resumen general")
    render_kpis(products, reviews)

    st.markdown("### Reviews por familia de producto")
    reviews_by_kw = (
        reviews.groupby("focus_keyword")
        .size()
        .reset_index(name="reviews")
        .sort_values("reviews", ascending=False)
    )
    if not reviews_by_kw.empty:
        reviews_by_kw["focus_keyword"] = reviews_by_kw["focus_keyword"].apply(pretty_keyword)
        st.bar_chart(reviews_by_kw.set_index("focus_keyword"))
    else:
        st.info("No hay datos para mostrar reviews por familia.")

    st.markdown("### Rating promedio por familia")
    if "Rating" in reviews.columns and reviews["Rating"].notna().any():
        rating_by_kw = (
            reviews.groupby("focus_keyword", as_index=False)["Rating"]
            .mean()
            .sort_values("Rating", ascending=False)
        )
        rating_by_kw["focus_keyword"] = rating_by_kw["focus_keyword"].apply(pretty_keyword)
        st.bar_chart(rating_by_kw.set_index("focus_keyword"))
    else:
        st.info("No hay rating disponible.")


def render_products_section(products: pd.DataFrame, reviews: pd.DataFrame) -> None:
    st.subheader("Detalle por producto")

    product_options = sorted(products["ProductName"].dropna().unique().tolist())
    if not product_options:
        st.warning("No hay productos foco para mostrar.")
        return

    selected_product = st.selectbox("Selecciona un producto", product_options)

    product_row = products.loc[products["ProductName"] == selected_product].head(1)
    product_reviews = reviews.loc[reviews["ProductName"] == selected_product].copy()

    if not product_row.empty:
        row = product_row.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Marca", row["Brand"] if "Brand" in row else "N/A")
        c2.metric("AvgRating", round(float(row["AvgRating"]), 2) if pd.notna(row.get("AvgRating")) else "N/A")
        c3.metric("TotalReviewCount", int(row["TotalReviewCount"]) if pd.notna(row.get("TotalReviewCount")) else 0)

        st.caption(f"Familia objetivo: {pretty_keyword(row['focus_keyword'])}")

    st.markdown("### Distribución de ratings del producto")
    if "Rating" in product_reviews.columns and product_reviews["Rating"].notna().any():
        rating_dist = (
            product_reviews["Rating"]
            .value_counts(dropna=True)
            .sort_index()
            .rename_axis("Rating")
            .reset_index(name="count")
        )
        st.bar_chart(rating_dist.set_index("Rating"))
    else:
        st.info("No hay ratings para este producto.")

    st.markdown("### Últimas reviews")
    cols = [c for c in ["SubmissionDate", "Rating", "Title", "ReviewText", "IsRecommended"] if c in product_reviews.columns]
    if cols:
        st.dataframe(
            product_reviews[cols].sort_values("SubmissionDate", ascending=False).head(20),
            width="stretch",
        )
    else:
        st.info("No hay columnas suficientes para mostrar reviews.")


def render_business_questions(products: pd.DataFrame, reviews: pd.DataFrame) -> None:
    st.subheader("Preguntas de negocio")

    st.markdown("### 1. ¿Qué productos combinan mejor rating y volumen?")
    if {"ProductName", "Rating"}.issubset(reviews.columns):
        ranking = (
            reviews.groupby(["ProductName", "Brand", "focus_keyword"], as_index=False)
            .agg(
                avg_rating=("Rating", "mean"),
                n_reviews=("ProductID", "size"),
            )
            .sort_values(["avg_rating", "n_reviews"], ascending=[False, False])
        )
        ranking["focus_keyword"] = ranking["focus_keyword"].apply(pretty_keyword)
        st.dataframe(ranking.head(15), width="stretch")
        st.caption("Esto ayuda a priorizar productos fuertes en reputación y volumen de conversación.")
    else:
        st.info("No hay datos suficientes para construir el ranking.")

    st.markdown("### 2. ¿Qué familias concentran más recomendación?")
    if "IsRecommended" in reviews.columns:
        rec = (
            reviews.groupby("focus_keyword", as_index=False)["IsRecommended"]
            .mean()
            .rename(columns={"IsRecommended": "recommendation_rate"})
            .sort_values("recommendation_rate", ascending=False)
        )
        rec["focus_keyword"] = rec["focus_keyword"].apply(pretty_keyword)
        st.bar_chart(rec.set_index("focus_keyword"))
        st.caption("Sirve para identificar qué línea tiene mejor percepción general.")
    else:
        st.info("No existe la columna IsRecommended en reviews.")

    st.markdown("### 3. ¿Qué atributos aparecen más en reviews negativas?")
    if "Rating" in reviews.columns:
        negatives = reviews.loc[reviews["Rating"] <= 2].copy()
        attr_candidates = ["skinType", "skinConcerns", "hairType", "hairConcerns", "ageRange"]
        selected_attr = next((c for c in attr_candidates if c in negatives.columns), None)

        if selected_attr and not negatives.empty:
            attr_dist = (
                negatives[selected_attr]
                .fillna("No informado")
                .value_counts()
                .head(10)
                .rename_axis(selected_attr)
                .reset_index(name="count")
            )
            st.bar_chart(attr_dist.set_index(selected_attr))
            st.caption(f"En este caso se está usando la variable: {selected_attr}.")
        else:
            st.info("No hay suficientes reviews negativas o atributos disponibles.")
    else:
        st.info("No hay columna Rating.")

    st.markdown("### 4. ¿Cómo evoluciona el volumen de reviews en el tiempo?")
    if "SubmissionDate" in reviews.columns and reviews["SubmissionDate"].notna().any():
        ts = reviews.dropna(subset=["SubmissionDate"]).copy()
        ts["month"] = ts["SubmissionDate"].dt.to_period("M").astype(str)
        monthly = (
            ts.groupby(["month", "focus_keyword"], as_index=False)
            .size()
            .rename(columns={"size": "reviews"})
        )
        pivot_ts = monthly.pivot(index="month", columns="focus_keyword", values="reviews").fillna(0)
        pivot_ts = pivot_ts.rename(columns={c: pretty_keyword(c) for c in pivot_ts.columns})
        st.line_chart(pivot_ts)
        st.caption("Esto muestra la dinámica temporal de conversación por familia objetivo.")
    else:
        st.info("No hay SubmissionDate para analizar evolución temporal.")