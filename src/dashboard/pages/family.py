"""Page 2 — Family Explorer."""
from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from src.dashboard import charts, components as ui, data_loader as dl, theme as th
from src.dashboard.utils import pretty_keyword


def _bar_only_volume(months, bars):
    """Single-axis bar chart for monthly review volume (used when search is NULL)."""
    fig = go.Figure(go.Bar(
        x=months, y=bars, marker=dict(color=th.C2_TEAL, opacity=0.85),
        hovertemplate="%{x}<br>reviews <b>%{y:,}</b><extra></extra>",
    ))
    fig.update_layout(
        height=320,
        font=dict(family="DM Sans, system-ui, sans-serif", color=th.T1, size=12),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40, r=20, t=20, b=36),
        xaxis=dict(gridcolor=th.LINE,
                   tickfont=dict(family="JetBrains Mono", size=11, color=th.T2)),
        yaxis=dict(title="reviews per month", gridcolor=th.LINE,
                   tickfont=dict(family="JetBrains Mono", size=11, color=th.T2)),
        showlegend=False, bargap=0.25,
    )
    return fig


def render(data: dl.DashboardData) -> None:
    fam_df = data.families
    if fam_df.empty:
        st.warning("gold.product_families is empty — run the gold transform.")
        return

    fam_options = sorted(fam_df["focus_keyword"].dropna().tolist())
    selected = st.selectbox("Family", fam_options,
                            index=0, key="family_select", label_visibility="collapsed")

    fam_row = fam_df[fam_df["focus_keyword"] == selected].iloc[0]

    ui.page_header(
        kicker="Family explorer",
        title=pretty_keyword(selected),
        breadcrumb="back to Overview",
        sub=f"{int(fam_row['products_count'])} products · {int(fam_row['brands_count'])} brands · "
            f"{int(fam_row['total_reviews']):,} reviews",
    )

    ui.kpi_strip([
        {"label": "PRODUCTS IN FAMILY", "value": f"{int(fam_row['products_count']):,}"},
        {"label": "BRANDS COMPETING", "value": f"{int(fam_row['brands_count']):,}"},
        {"label": "TOTAL REVIEWS", "value": f"{int(fam_row['total_reviews']):,}"},
        {"label": "AVG RATING", "value": f"{float(fam_row['avg_rating']):.2f}"},
    ])

    st.markdown("&nbsp;", unsafe_allow_html=True)

    # ── Monthly review volume (search trends not populated yet) ──────────────
    fds = dl.family_demand_supply(selected)
    has_search = fds["search_interest_avg"].notna().any() if not fds.empty else False

    sub_text = ("search interest line vs review volume bars · monthly"
                if has_search
                else "monthly review volume · Google Trends data pending")

    with ui.card(f"Monthly activity · {pretty_keyword(selected)}", sub=sub_text):
        if not fds.empty:
            months = [m.strftime("%Y-%m") for m in fds["month"]]
            bars = fds["reviews_count"].fillna(0).astype(int).tolist()
            if has_search:
                search = fds["search_interest_avg"].fillna(0).tolist()
                st.plotly_chart(
                    charts.demand_vs_supply(months=months, search_line=search,
                                             review_bars=bars, spike_windows=None,
                                             height=320),
                    use_container_width=True, key="fam_dvs",
                )
            else:
                st.plotly_chart(_bar_only_volume(months, bars),
                                use_container_width=True, key="fam_volume")
        else:
            st.info("No monthly aggregates for this family yet.")

    st.markdown("&nbsp;", unsafe_allow_html=True)

    col_box, col_share = st.columns([4, 8])

    with col_box:
        with ui.card("Health distribution",
                     sub=f"{pretty_keyword(selected)} vs all others"):
            all_p = data.products
            fam_h = all_p.loc[all_p["focus_keyword"] == selected, "health_score"].dropna()
            other_h = all_p.loc[all_p["focus_keyword"] != selected, "health_score"].dropna()
            if not fam_h.empty and not other_h.empty:
                boxes = []
                for label, s in [(pretty_keyword(selected), fam_h), ("all others", other_h)]:
                    boxes.append({"label": label, "min": float(s.min()),
                                  "q1": float(s.quantile(.25)), "med": float(s.median()),
                                  "q3": float(s.quantile(.75)), "max": float(s.max()),
                                  "p90": float(s.quantile(.9))})
                st.plotly_chart(charts.boxplot_health(boxes, height=280),
                                use_container_width=True, key="fam_box")
            else:
                st.info("Not enough products with health_score in this family.")

    with col_share:
        with ui.card("Brand share inside family", sub="% of family review volume per brand"):
            share = dl.family_brand_share(selected).head(6)
            if not share.empty:
                colors = [th.C1_CORAL, th.C2_TEAL, th.C3_LAVENDER, th.C4_AMBER, th.C5_SKY, "#9CA3AF"]
                segments = [{"label": b, "v": int(v), "c": colors[i % len(colors)]}
                            for i, (b, v) in enumerate(zip(share["brand"], share["reviews"]))]
                st.plotly_chart(charts.stacked_hbar(segments, height=110),
                                use_container_width=True, key="fam_share")
            else:
                st.info("No brand data for this family.")

            st.markdown('<div class="caps" style="margin-top:18px;margin-bottom:10px;">'
                        'Top 8 products in family</div>', unsafe_allow_html=True)
            top = dl.family_top_products(selected, n=8)
            tile_cols = st.columns(4)
            for i, (_, r) in enumerate(top.iterrows()):
                with tile_cols[i % 4]:
                    rating = float(r["avg_rating"] or 0)
                    score = int(r["health_score"]) if r["health_score"] is not None else "—"
                    name = (r["product_name"] or "")[:38]
                    brand = r["brand"] or ""
                    st.markdown(
                        f"""<div class="mp-card" style="margin-bottom:12px;">
                          <div style="font-weight:600;font-size:13px;line-height:1.25;color:var(--t1);">{name}</div>
                          <div style="font-size:11.5px;color:var(--t2);margin-top:4px;">{brand}</div>
                          <div style="display:flex;align-items:center;gap:8px;margin-top:8px;">
                            {ui.stars(rating)}
                            <span class="mono" style="font-size:11px;color:var(--t1);">{rating:.1f}</span>
                            <span style="margin-left:auto;font-family:var(--mono);font-size:11px;
                                         padding:2px 6px;border-radius:4px;background:var(--coral-tint);
                                         color:var(--coral-dark);">{score}</span>
                          </div>
                        </div>""", unsafe_allow_html=True)
