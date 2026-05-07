"""Page 4 — Brand Benchmark, on real Postgres data."""
from __future__ import annotations

import streamlit as st

from src.dashboard import charts, components as ui, data_loader as dl, theme as th
from src.dashboard.utils import pretty_keyword


# Stable color per family across the page
_FAMILY_COLOR = {
    "niacinamida":           th.C2_TEAL,
    "acido_hialuronico":     th.C1_CORAL,
    "shampoo_sin_sulfatos":  th.C3_LAVENDER,
    "retinol":               th.C4_AMBER,
    "vitamina_c":            th.C5_SKY,
    "unclassified":          "#9CA3AF",
}


def _color_for(family: str) -> str:
    return _FAMILY_COLOR.get(family, "#9CA3AF")


def render(data: dl.DashboardData) -> None:
    brands_df = data.brands
    if brands_df.empty:
        st.warning("gold.brands is empty.")
        return

    all_brands = brands_df["brand"].tolist()
    default_pick = brands_df.head(5)["brand"].tolist()

    selection = st.multiselect("Brands (up to 5)", all_brands,
                               default=default_pick, max_selections=5,
                               key="brand_select")

    ui.page_header(
        kicker="Brand benchmark",
        title="Head-to-head",
        sub=f"{len(brands_df)} brands in catalogue · select up to 5 for comparison",
        chips=[("", b) for b in selection],
    )

    if not selection:
        st.info("Pick at least one brand.")
        return

    sub = brands_df[brands_df["brand"].isin(selection)].copy()

    col_table, col_radar = st.columns([8, 4])

    with col_table:
        with ui.card("Comparison table", sub="all metrics from gold.brands",
                     actions="↓ Export CSV"):
            display = sub.rename(columns={
                "brand": "Brand", "products_count": "Products", "total_reviews": "Reviews",
                "avg_rating": "Avg rating", "pct_recommended": "% Rec",
                "avg_sentiment": "Sentiment", "avg_health_score": "Health",
                "polarization_score": "Polariz.",
            })[["Brand", "Products", "Reviews", "Avg rating", "% Rec",
                "Sentiment", "Health", "Polariz."]].copy()
            display["% Rec"] = display["% Rec"] * 100
            st.dataframe(
                display.style.format({
                    "Avg rating": "{:.2f}", "% Rec": "{:.1f}%",
                    "Sentiment": "{:+.2f}", "Health": "{:.1f}", "Polariz.": "{:.2f}",
                    "Reviews": "{:,}", "Products": "{:,}",
                }),
                use_container_width=True, hide_index=True,
            )

    with col_radar:
        with ui.card("Profile radar",
                     sub="per-axis relative · differences amplified across selected brands"):
            palette = [th.C1_CORAL, th.C2_TEAL, th.C3_LAVENDER, th.C4_AMBER, th.C5_SKY]
            axes = ["Rating", "Recommended", "Sentiment", "Health", "Stability"]

            # Collect raw values per metric
            raw = {ax: [] for ax in axes}
            for _, r in sub.iterrows():
                raw["Rating"].append(float(r["avg_rating"] or 0))
                raw["Recommended"].append(float(r["pct_recommended"] or 0))
                raw["Sentiment"].append(float(r["avg_sentiment"] or 0))
                raw["Health"].append(float(r["avg_health_score"] or 0))
                raw["Stability"].append(1.0 - float(r["polarization_score"] or 0))

            # Per-axis min/max with a small padding so the best brand isn't
            # pinned at the edge and the worst isn't at the center.
            ranges: dict[str, tuple[float, float]] = {}
            for ax, vals in raw.items():
                lo, hi = min(vals), max(vals)
                span = hi - lo
                if span < 1e-9:
                    ranges[ax] = (lo - 0.05, lo + 0.05)
                else:
                    pad = span * 0.15
                    ranges[ax] = (lo - pad, hi + pad)

            def _scale(ax: str, val: float) -> float:
                lo, hi = ranges[ax]
                return max(0.0, min(1.0, (val - lo) / (hi - lo)))

            def _abs_str(ax: str, val: float) -> str:
                if ax == "Rating":      return f"{val:.2f}"
                if ax == "Recommended": return f"{val * 100:.1f}%"
                if ax == "Sentiment":   return f"{val:+.2f}"
                if ax == "Health":      return f"{val:.1f}"
                if ax == "Stability":   return f"{val:.2f}"
                return f"{val}"

            series = []
            for i, (_, r) in enumerate(sub.iterrows()):
                rating = float(r["avg_rating"] or 0)
                rec = float(r["pct_recommended"] or 0)
                sent = float(r["avg_sentiment"] or 0)
                health = float(r["avg_health_score"] or 0)
                stability = 1.0 - float(r["polarization_score"] or 0)
                series.append({
                    "name": r["brand"],
                    "color": palette[i % 5],
                    "values": [
                        _scale("Rating", rating),
                        _scale("Recommended", rec),
                        _scale("Sentiment", sent),
                        _scale("Health", health),
                        _scale("Stability", stability),
                    ],
                    "absolute": [
                        _abs_str("Rating", rating),
                        _abs_str("Recommended", rec),
                        _abs_str("Sentiment", sent),
                        _abs_str("Health", health),
                        _abs_str("Stability", stability),
                    ],
                })
            if series:
                st.plotly_chart(
                    charts.radar(axes, series, height=360),
                    use_container_width=True, key="brand_radar",
                )
                # Show the actual axis ranges so analysts can read absolute
                # context, not just relative shape.
                rng_html = " · ".join(
                    f'<span style="color:var(--t2)">{ax}</span> '
                    f'<span class="mono" style="color:var(--t1)">'
                    f'{_abs_str(ax, ranges[ax][0])} → {_abs_str(ax, ranges[ax][1])}</span>'
                    for ax in axes
                )
                st.markdown(
                    f'<div style="margin-top:8px;font-size:11px;line-height:1.6;">'
                    f'<b style="color:var(--t2)">scale</b><br>{rng_html}</div>',
                    unsafe_allow_html=True,
                )

    st.markdown("&nbsp;", unsafe_allow_html=True)

    col_share, col_rank = st.columns(2)

    # ── Family share by brand — real distribution from gold.products ────────
    with col_share:
        with ui.card("Family share by brand",
                     sub="real distribution · review-weighted across all focus_keywords"):
            dist = dl.brand_family_distribution(tuple(selection))

            if dist.empty:
                st.info("No reviews aggregated for the selected brands.")
            else:
                families_present = (
                    dist.groupby("focus_keyword")["reviews"].sum()
                    .sort_values(ascending=False).index.tolist()
                )

                for brand in selection:
                    rows = dist[dist["brand"] == brand].set_index("focus_keyword")
                    total_reviews = int(rows["reviews"].sum())
                    if total_reviews == 0:
                        continue
                    bar_segments = []
                    for fk in families_present:
                        share = float(rows.loc[fk, "share"]) if fk in rows.index else 0.0
                        if share <= 0:
                            continue
                        bar_segments.append((fk, share))

                    seg_html = "".join(
                        f'<div style="flex:{share};background:{_color_for(fk)};display:grid;'
                        f'place-items:center;color:#fff;font-size:11px;font-family:var(--mono);" '
                        f'title="{pretty_keyword(fk)} · {share*100:.0f}%">'
                        f'{share*100:.0f}%</div>'
                        for fk, share in bar_segments
                    )
                    st.markdown(
                        f"""<div style="margin-bottom:14px;color:var(--t1);">
                          <div style="display:flex;justify-content:space-between;
                                      font-size:12.5px;font-weight:600;margin-bottom:6px;">
                            <span>{brand}</span>
                            <span style="color:var(--t2);font-weight:500;font-family:var(--mono);">
                              {total_reviews:,} reviews
                            </span>
                          </div>
                          <div style="display:flex;gap:4px;height:24px;border-radius:4px;
                                      overflow:hidden;">{seg_html}</div>
                        </div>""", unsafe_allow_html=True)

                # Legend showing only families present in the data
                legend_html = " ".join(
                    f'<span style="display:inline-flex;align-items:center;gap:4px;'
                    f'font-size:11.5px;color:var(--t2);margin-right:14px;">'
                    f'<span style="width:8px;height:8px;background:{_color_for(fk)};"></span>'
                    f'{pretty_keyword(fk)}</span>'
                    for fk in families_present
                )
                st.markdown(f'<div style="margin-top:6px;">{legend_html}</div>',
                            unsafe_allow_html=True)

    with col_rank:
        with ui.card("Brand ranking · health score",
                     sub=f"top 15 of {len(brands_df)} brands · selected highlighted"):
            top15 = brands_df.head(15)
            items = [{"brand": r["brand"], "health": float(r["avg_health_score"] or 0)}
                     for _, r in top15.iterrows()]
            st.plotly_chart(charts.ranking_bars(items, selected=set(selection), height=420),
                            use_container_width=True, key="brand_rank")
