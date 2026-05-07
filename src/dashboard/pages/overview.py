"""Page 1 — Executive Overview."""
from __future__ import annotations

import streamlit as st

from src.dashboard import charts, components as ui, data_loader as dl


def render(data: dl.DashboardData) -> None:
    kpis = dl.overview_kpis()
    last_run = data.last_run
    last_label = last_run["finished_at"].strftime("%Y-%m-%d %H:%M UTC") \
        if last_run and last_run.get("finished_at") is not None else "n/a"

    ui.page_header(
        kicker="Executive overview",
        title="Portfolio at a glance",
        sub=f"{kpis['total_products']} products · {kpis['total_reviews']:,} reviews · "
            f"last gold build {last_label}",
    )

    ui.kpi_strip([
        {"label": "TOTAL PRODUCTS", "value": dl.kpi_format_number(kpis["total_products"])},
        {"label": "TOTAL REVIEWS", "value": dl.kpi_format_number(kpis["total_reviews"])},
        {"label": "MEAN RATING", "value": f"{kpis['mean_rating']:.2f}" if kpis["mean_rating"] else "—"},
        {"label": "MEAN HEALTH", "value": f"{kpis['mean_health']:.1f}" if kpis["mean_health"] else "—"},
        {"label": "% RECOMMENDED", "value": f"{kpis['pct_recommended']:.1f}%" if kpis["pct_recommended"] else "—"},
    ])

    st.markdown("&nbsp;", unsafe_allow_html=True)

    col_left, col_right = st.columns([8, 4])

    with col_left:
        scores = data.products["health_score"].dropna().tolist()
        median = float(data.products["health_score"].median()) if scores else 0
        with ui.card("Health score distribution",
                     sub=f"{len(scores)} products · median {median:.1f}"):
            st.plotly_chart(charts.histogram(scores, median=median, bins=20, height=240),
                            use_container_width=True, key="ov_hist")

    with col_right:
        mix = dl.sentiment_mix()
        labels = ["Positive", "Neutral", "Negative"]
        values = [mix.get("positive", 0), mix.get("neutral", 0), mix.get("negative", 0)]
        with ui.card("Sentiment mix",
                     sub=f"{sum(values):,} reviews · gold.reviews.sentiment_label"):
            st.plotly_chart(charts.donut(labels, values, height=220),
                            use_container_width=True, key="ov_donut")

    st.markdown("&nbsp;", unsafe_allow_html=True)

    top_df, bot_df = dl.top_bottom_products(10)

    def to_items(df):
        return [{
            "brand": r["brand"] or "—",
            "name": (r["product_name"] or "")[:34],
            "v": float(r["health_score"]),
            "fam": r["focus_keyword"],
        } for _, r in df.iterrows()]

    col_top, col_bot = st.columns(2)
    with col_top:
        with ui.card("Top 10 by health score", sub="color = focus_keyword family · ≥30 reviews"):
            st.plotly_chart(charts.hbars(to_items(top_df), height=320),
                            use_container_width=True, key="ov_top")

    with col_bot:
        with ui.card("Bottom 10 — needs attention", sub="ascending health · ≥30 reviews"):
            st.plotly_chart(charts.hbars(to_items(bot_df), height=320, color_by_family=False),
                            use_container_width=True, key="ov_bot")

    st.markdown("&nbsp;", unsafe_allow_html=True)

    vel = dl.velocity_movers(min_total=30)
    # Drop rows where both velocities are zero — they cluster at origin and add noise.
    vel = vel[(vel["prior_30d"] > 0) | (vel["last_30d"] > 0)]
    with ui.card("Velocity movers",
                 sub="prior 30d vs last 30d · dot size = total reviews · 45° = parity · "
                     f"{len(vel)} active products"):
        st.plotly_chart(
            charts.velocity_scatter(
                prior=vel["prior_30d"].tolist(),
                last=vel["last_30d"].tolist(),
                sizes=vel["total_review_count"].clip(lower=20).tolist(),
                names=[f"{r.brand} · {(r.product_name or '')[:32]}" for r in vel.itertuples()],
                height=400,
            ),
            use_container_width=True, key="ov_vel",
        )
