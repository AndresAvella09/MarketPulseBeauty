"""Page 3 — Product Detail, on real Postgres data."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import charts, components as ui, data_loader as dl
from src.dashboard.utils import pretty_keyword


def render(data: dl.DashboardData) -> None:
    products = data.products
    if products.empty:
        st.warning("No products in gold.products.")
        return

    sorted_p = products.sort_values("health_score", ascending=False, na_position="last")
    options = sorted_p["product_name"].dropna().tolist()
    name = st.selectbox("Producto", options, index=0,
                        key="product_select", label_visibility="collapsed")

    row = products[products["product_name"] == name].iloc[0]
    pid = row["product_id"]

    brand = row.get("brand") or "—"
    cat = row.get("product_category") or "—"
    family = row.get("focus_keyword")

    st.markdown('<div class="mp-breadcrumb">← back to Overview</div>', unsafe_allow_html=True)
    st.markdown(
        f"""
<div style="display:flex;align-items:flex-start;gap:20px;margin-bottom:18px;color:var(--t1);">
  <div style="width:96px;height:96px;border-radius:10px;
              background:repeating-linear-gradient(135deg,#F4F1EE 0,#F4F1EE 7px,#ECE7E2 7px,#ECE7E2 14px);
              display:grid;place-items:center;color:var(--t3);font-family:var(--mono);font-size:11px;">product</div>
  <div style="flex:1;">
    <div class="caps">{brand} · {cat}</div>
    <div class="mp-h1" style="font-size:30px;margin-top:4px;">{name}</div>
    <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
      <span class="mp-chip brand">family · {pretty_keyword(family) if family else '—'}</span>
      <span class="mp-chip">PID {pid}</span>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    avg_rating = float(row["avg_rating"]) if pd.notna(row["avg_rating"]) else None
    n = int(row["total_review_count"] or 0)
    pct_rec = (float(row["pct_recommended"]) * 100) if pd.notna(row["pct_recommended"]) else None
    health = float(row["health_score"]) if pd.notna(row["health_score"]) else None
    photo = float(row["photo_coverage"]) if pd.notna(row.get("photo_coverage")) else None
    edit_rate = float(row["edit_rate"]) if pd.notna(row.get("edit_rate")) else None

    vel_last = row.get("review_velocity_30d")
    vel_prior = row.get("review_velocity_prior_30d")
    vel_delta = None
    if pd.notna(vel_last) and pd.notna(vel_prior) and vel_prior:
        vel_delta = (vel_last - vel_prior) / vel_prior * 100

    ui.kpi_strip([
        {"label": "AVG RATING", "value": f"{avg_rating:.2f}" if avg_rating else "—",
         "meta": ui.stars(avg_rating)},
        {"label": "REVIEWS", "value": f"{n:,}",
         "delta": f"{int(vel_last)} last 30d" if pd.notna(vel_last) else None,
         "delta_up": (vel_delta is not None and vel_delta >= 0) if vel_delta is not None else None},
        {"label": "% RECOMMENDED", "value": f"{pct_rec:.1f}%" if pct_rec is not None else "—"},
        {"label": "HEALTH SCORE", "value": f"{health:.0f}" if health else "—",
         "meta": "0–100 composite"},
        {"label": "PHOTO COVERAGE", "value": f"{photo*100:.1f}%" if photo is not None else "—"},
        {"label": "EDIT RATE", "value": f"{edit_rate*100:.1f}%" if edit_rate is not None else "—"},
    ])

    st.markdown("&nbsp;", unsafe_allow_html=True)

    monthly = dl.product_monthly(pid, months=24)
    rating_dist = dl.product_rating_distribution(pid)

    col_left, col_right = st.columns([8, 4])
    with col_left:
        with ui.card("Performance over time",
                     sub=f"avg_rating line & sentiment mix · {len(monthly)} months "
                         "from gold.product_insights_monthly"):
            if not monthly.empty and len(monthly) >= 2:
                months = [m.strftime("%b %y") if hasattr(m, "strftime") else str(m)
                          for m in monthly["month"]]
                pos = (monthly["pct_positive"].fillna(0) * 100).tolist()
                neu = (monthly["pct_neutral"].fillna(0) * 100).tolist()
                neg = (monthly["pct_negative"].fillna(0) * 100).tolist()
                rating = monthly["avg_rating"].ffill().tolist()
                st.plotly_chart(
                    charts.sentiment_area(months=months, pos=pos, neu=neu, neg=neg,
                                          rating_line=rating, height=300),
                    use_container_width=True, key="prod_perf",
                )
            else:
                st.info("Need at least 2 monthly snapshots for this product.")

    with col_right:
        with ui.card("Rating histogram", sub="from gold.products.rating_dist_*"):
            counts = pd.Series(rating_dist).reindex([5, 4, 3, 2, 1], fill_value=0) \
                if rating_dist else pd.Series({5: 0, 4: 0, 3: 0, 2: 0, 1: 0})
            max_v = max(counts.max(), 1)
            rows = []
            for star, v in counts.items():
                pct = v / max_v * 100
                rows.append(
                    f"""<div style="display:flex;align-items:center;gap:10px;font-size:12px;
                                    margin-bottom:8px;color:var(--t1);">
                      <span style="width:14px;font-family:var(--mono);font-weight:600;">{star}★</span>
                      <div style="flex:1;height:14px;background:var(--bg);border-radius:4px;">
                        <div style="width:{pct:.0f}%;height:100%;background:var(--coral);border-radius:4px;"></div>
                      </div>
                      <span class="mono" style="width:60px;text-align:right;font-weight:600;">{int(v):,}</span>
                    </div>"""
                )
            st.markdown("".join(rows), unsafe_allow_html=True)

            if pd.notna(vel_last) and pd.notna(vel_prior):
                arrow = "▲" if vel_last >= vel_prior else "▼"
                color = "var(--success)" if vel_last >= vel_prior else "var(--danger)"
                st.markdown(
                    f"""<div style="border-top:1px solid var(--line);margin-top:14px;padding-top:14px;color:var(--t1);">
                      <div class="caps" style="margin-bottom:8px;">Velocity · last 30d</div>
                      <div style="display:flex;align-items:flex-end;gap:14px;">
                        <div>
                          <div class="mono" style="font-size:22px;font-weight:600;color:var(--t1);">{int(vel_last)}</div>
                          <div style="font-size:11.5px;color:var(--t2);">last 30d reviews</div>
                        </div>
                        <div style="font-family:var(--mono);font-size:13px;color:{color};">{arrow} {abs(vel_delta):.1f}%</div>
                        <div style="margin-left:auto;text-align:right;">
                          <div class="mono" style="font-size:14px;color:var(--t2);">{int(vel_prior)}</div>
                          <div style="font-size:11px;color:var(--t3);">prior 30d</div>
                        </div>
                      </div>
                    </div>""", unsafe_allow_html=True)

    st.markdown("&nbsp;", unsafe_allow_html=True)

    daily = dl.product_daily(pid)
    themes = dl.product_themes(pid)

    col_a, col_b = st.columns([7, 5])
    with col_a:
        with ui.card("Last 90 days", sub="daily review counts · 7-day MA"):
            if not daily.empty:
                d = daily["reviews_count"].fillna(0).astype(int).tolist()
                ma = pd.Series(d).rolling(7, min_periods=1).mean().tolist()
                st.plotly_chart(charts.daily_bars_ma(d, ma, height=220),
                                use_container_width=True, key="prod_daily")
            else:
                st.info("No daily snapshots yet for this product.")

    with col_b:
        with ui.card("What people love · main complaints", sub="from gold.review_themes"):
            loves = themes[themes["polarity"] == "pos"].head(5) if not themes.empty else themes
            complaints = themes[themes["polarity"] == "neg"].head(5) if not themes.empty else themes
            c1, c2 = st.columns(2)
            with c1:
                st.markdown('<div class="caps">What people love</div>', unsafe_allow_html=True)
                if loves.empty:
                    st.markdown('<div style="color:var(--t2);font-size:12px;margin-top:8px;">'
                                'no positive themes yet</div>', unsafe_allow_html=True)
                else:
                    pills = "".join(
                        ui.theme_pill((r["theme_label"] or "")[:36], int(round((r["pct"] or 0) * 100)), "pos")
                        for _, r in loves.iterrows())
                    st.markdown(pills, unsafe_allow_html=True)
            with c2:
                st.markdown('<div class="caps">Main complaints</div>', unsafe_allow_html=True)
                if complaints.empty:
                    st.markdown('<div style="color:var(--t2);font-size:12px;margin-top:8px;">'
                                'no negative themes yet</div>', unsafe_allow_html=True)
                else:
                    pills = "".join(
                        ui.theme_pill((r["theme_label"] or "")[:36], int(round((r["pct"] or 0) * 100)), "neg")
                        for _, r in complaints.iterrows())
                    st.markdown(pills, unsafe_allow_html=True)

    st.markdown("&nbsp;", unsafe_allow_html=True)

    col_q, col_l = st.columns([8, 4])
    with col_q:
        with ui.card("Featured quotes", sub="from gold.products.top_quote_*"):
            quotes = []
            for pol, key in (("pos", "top_quote_positive"),
                             ("neu", "top_quote_neutral"),
                             ("neg", "top_quote_negative")):
                decoded = dl.decode_quote(row.get(key))
                if decoded:
                    txt = decoded.get("text") or decoded.get("title") or ""
                    who_parts = [decoded.get(k) for k in ("skin_type", "age_range") if decoded.get(k)]
                    quotes.append((pol, txt, " · ".join(who_parts) or "—"))
            if quotes:
                qcols = st.columns(len(quotes))
                for col, (pol, txt, who) in zip(qcols, quotes):
                    with col:
                        st.markdown(ui.quote_card(txt[:280], who, pol), unsafe_allow_html=True)
            else:
                st.info("No featured quotes precomputed for this product.")

    with col_l:
        with ui.card("Top reviewer locations", sub="top 8 by review count"):
            locs = dl.product_top_locations(pid)
            if locs:
                max_n = max(n for _, n in locs)
                html = ""
                for loc, count in locs:
                    pct = count / max_n * 100
                    html += f"""
<div style="display:flex;align-items:center;gap:10px;font-size:12.5px;margin-bottom:6px;color:var(--t1);">
  <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{loc[:36]}</span>
  <div style="width:120px;height:8px;background:var(--bg);border-radius:99px;">
    <div style="width:{pct:.0f}%;height:100%;background:var(--c5-sky);border-radius:99px;"></div>
  </div>
  <span class="mono" style="font-weight:600;width:40px;text-align:right;">{count}</span>
</div>"""
                st.markdown(html, unsafe_allow_html=True)
            else:
                st.info("No location data for reviewers of this product.")
