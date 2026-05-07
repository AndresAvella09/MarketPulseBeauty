"""MarketPulse Beauty — Streamlit dashboard entry point.

Reads from the `gold.*` schema in Postgres (via PG_CONN). Run inside docker:
    docker compose up streamlit

Or locally with port 5433 exposed by the postgres service:
    streamlit run app.py
"""
from __future__ import annotations

import streamlit as st

from src.dashboard import theme as th
from src.dashboard.components import render_sidebar, top_bar
from src.dashboard.data_loader import load_dashboard_data
from src.dashboard.pages import (
    overview,
    family,
    product,
    brand,
    review,
    demographics,
    search,
    pipeline,
)


st.set_page_config(
    page_title="MarketPulse Beauty",
    page_icon="✦",
    layout="wide",
    initial_sidebar_state="expanded",
)

th.inject_css()

NAV_GROUPS = [
    {"title": "Analytics", "items": [
        {"key": "overview",     "label": "Overview",         "icon": "🏠"},
        {"key": "family",       "label": "Family Explorer",  "icon": "🧪"},
        {"key": "product",      "label": "Product Detail",   "icon": "📦"},
        {"key": "brand",        "label": "Brand Benchmark",  "icon": "🏷️"},
    ]},
    {"title": "Discovery", "items": [
        {"key": "review",       "label": "Review Explorer",  "icon": "💬"},
        {"key": "demographics", "label": "Demographics",     "icon": "👥"},
        {"key": "search",       "label": "Search Intel",     "icon": "🔍"},
    ]},
    {"title": "System", "items": [
        {"key": "pipeline",     "label": "Pipeline Health",  "icon": "📊"},
    ]},
]

PAGES = {
    "overview": overview.render,
    "family": family.render,
    "product": product.render,
    "brand": brand.render,
    "review": review.render,
    "demographics": demographics.render,
    "search": search.render,
    "pipeline": pipeline.render,
}


def main() -> None:
    if "active_page" not in st.session_state:
        st.session_state.active_page = "overview"

    active = render_sidebar(
        active_key=st.session_state.active_page,
        nav_groups=NAV_GROUPS,
        user=("EM", "Elena Marín", "Category Lead"),
    )
    st.session_state.active_page = active

    try:
        data = load_dashboard_data()
    except Exception as e:  # noqa: BLE001 — surface DB errors at the top of the page
        st.error(
            "Could not connect to the gold-layer Postgres. "
            "Check that the `postgres` container is running and that "
            "PG_CONN points to it (default in docker-compose: "
            "`postgresql+psycopg2://postgres:***@postgres:5432/marketpulse`)."
        )
        st.exception(e)
        st.stop()
        return

    if data.last_run is not None:
        finished = data.last_run.get("finished_at")
        when = finished.strftime("%Y-%m-%d %H:%M UTC") if finished is not None else "n/a"
        refresh = f"Synced · {data.last_run['dag_name']} · {when}"
    else:
        refresh = f"source: {data.source}"

    top_bar(refresh=refresh, date_range="Postgres · gold.*")

    PAGES[active](data)


main()
