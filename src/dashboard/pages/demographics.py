"""Page 6 — Demographics & Audience."""
from __future__ import annotations

import numpy as np
import streamlit as st

from src.dashboard import charts, components as ui, data_loader as dl
from src.dashboard.utils import pretty_keyword


def render(data: dl.DashboardData) -> None:
    fam_options = sorted(data.families["focus_keyword"].dropna().tolist()) \
        if not data.families.empty else []
    if not fam_options:
        st.warning("No families in gold.product_families yet.")
        return

    col_a, col_b = st.columns(2)
    with col_a:
        family = st.selectbox("Family", fam_options, key="demo_family")
    dims = dl.demographic_dimensions(family)
    with col_b:
        if not dims:
            st.warning(f"No demographic insights for {family} yet.")
            return
        dim = st.selectbox("Dimension", dims, key="demo_dim")

    ui.page_header(
        kicker="Audience",
        title="Who likes what",
        sub=f"gold.demographic_insights · family = {pretty_keyword(family)} · "
            f"dimension = {dim}",
        chips=[("family:", pretty_keyword(family)), ("dimension:", dim)],
    )

    df = dl.demographic_insights(family, dim)
    if df.empty:
        st.info("No rows for this family × dimension.")
        return

    rows = df["demographic_value"].fillna("(unknown)").tolist()
    cols = ["rating", "sentiment", "%rec", "%pos"]

    # Absolute values per (row, col) — what we display in the cells
    abs_values = []
    for _, r in df.iterrows():
        abs_values.append([
            float(r["avg_rating"] or 0),
            float(r["avg_sentiment"] or 0),
            float(r["pct_recommended"] or 0) * 100,
            float(r["pct_positive"] or 0) * 100,
        ])
    abs_arr = np.asarray(abs_values, dtype=float)

    # Per-column min-max normalisation so small spreads use the full color range
    col_min = abs_arr.min(axis=0)
    col_max = abs_arr.max(axis=0)
    col_range = np.where(col_max - col_min < 1e-9, 1.0, col_max - col_min)
    color_values = ((abs_arr - col_min) / col_range).tolist()

    text_values = [
        [f"{row[0]:.2f}", f"{row[1]:+.2f}", f"{row[2]:.0f}%", f"{row[3]:.0f}%"]
        for row in abs_values
    ]

    col_heat, col_vol = st.columns([7, 5])

    with col_heat:
        with ui.card("Performance heatmap",
                     sub="per-column min-max · cells show absolute values · "
                         "teal = best in column, coral = worst"):
            st.plotly_chart(
                charts.heatmap(rows, cols, color_values,
                               text_values=text_values,
                               height=max(280, 38 * len(rows))),
                use_container_width=True, key="demo_heat",
            )

    with col_vol:
        with ui.card("Audience volume", sub="reviewer count per segment"):
            max_v = int(df["reviews_count"].max() or 1)
            html = ""
            for r in df.itertuples():
                count = int(r.reviews_count or 0)
                pct = count / max_v * 100
                low = ui.low_n(count) if count < 50 else ""
                html += f"""
<div style="display:flex;align-items:center;gap:10px;font-size:12.5px;margin-bottom:8px;color:var(--t1);">
  <span style="width:120px;">{r.demographic_value or '(unknown)'}</span>
  <div style="flex:1;height:12px;background:var(--bg);border-radius:4px;">
    <div style="width:{pct:.0f}%;height:100%;background:var(--c5-sky);border-radius:4px;"></div>
  </div>
  <span class="mono" style="width:60px;text-align:right;font-weight:600;">{count:,}</span>
  {low}
</div>"""
            st.markdown(html, unsafe_allow_html=True)

    st.markdown("&nbsp;", unsafe_allow_html=True)

    with ui.card("Detailed metrics", sub="raw values from gold.demographic_insights"):
        st.dataframe(
            df.assign(pct_recommended=(df["pct_recommended"] * 100).round(1),
                      pct_positive=(df["pct_positive"] * 100).round(1),
                      pct_negative=(df["pct_negative"] * 100).round(1),
                      avg_rating=df["avg_rating"].round(2),
                      avg_sentiment=df["avg_sentiment"].round(2)),
            use_container_width=True, hide_index=True,
        )
