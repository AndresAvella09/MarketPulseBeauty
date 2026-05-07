"""Page 7 — Search Intelligence.

NOTE: gold.search_trends and gold.search_spikes are populated by a separate
Google Trends DAG that hasn't run yet. Per stakeholder request, this page
renders illustrative fixtures so the layout is reviewable. Once Trends data
lands, swap the fixtures for real queries.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import charts, components as ui, mock_data


def render(data) -> None:  # noqa: ANN001
    ui.page_header(
        kicker="Search intelligence",
        title="External demand signal",
        sub="Google Trends 0–100 daily index · independent of reviews · "
            "MOCK until gold.search_trends is populated",
        chips=[("", "niacinamida"), ("", "ácido hialurónico"),
               ("", "shampoo sin sulfatos"), ("geo:", "US, MX, AR, CO")],
    )

    st.warning(
        "📡 gold.search_trends and gold.search_spikes are currently empty. "
        "The visuals below are mocked. Schedule the Google Trends ingestion DAG "
        "to swap them for live data automatically."
    )

    series = mock_data.search_trends()
    spike = {"start": 70, "end": 78, "label": "+184%"}

    with ui.card("Search interest timeline", sub="90-day daily index · spike windows shaded"):
        st.plotly_chart(charts.search_trends(series, spike=spike, height=320),
                        use_container_width=True, key="search_main_tl")

    st.markdown("&nbsp;", unsafe_allow_html=True)

    col_lead, col_grid = st.columns([7, 5])

    with col_lead:
        with ui.card("Spike leaderboard", actions="↓ CSV"):
            spikes_df = pd.DataFrame(mock_data.SPIKES)
            spikes_df["pct"] = spikes_df["pct"].apply(lambda v: f"+{v}%")
            spikes_df.columns = ["Keyword", "Geo", "Start", "End", "% change"]
            st.dataframe(spikes_df, use_container_width=True, hide_index=True)

    with col_grid:
        with ui.card("Cross-family · demand vs supply", sub="click mini → Family Explorer"):
            small_cols = st.columns(2)
            for i, fam in enumerate(mock_data.FAMILIES[:4]):
                with small_cols[i % 2]:
                    st.markdown(
                        f'<div style="font-size:11.5px;font-weight:600;margin-bottom:4px;color:var(--t1);">{fam["label"]}</div>',
                        unsafe_allow_html=True,
                    )
                    st.plotly_chart(
                        charts.demand_vs_supply(
                            months=mock_data.MONTHS_12[-8:],
                            search_line=mock_data.SEARCH_LINE[-8:],
                            review_bars=mock_data.REVIEW_BARS[-8:],
                            height=160,
                        ),
                        use_container_width=True,
                        key=f"search_mini_{fam['k']}",
                    )
