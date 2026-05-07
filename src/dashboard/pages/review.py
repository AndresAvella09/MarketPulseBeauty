"""Page 5 — Review Explorer."""
from __future__ import annotations

import streamlit as st

from src.dashboard import charts, components as ui, data_loader as dl


def render(data: dl.DashboardData) -> None:
    meta = dl.review_filters_meta()

    rail, body = st.columns([2, 10])

    with rail:
        with ui.card("Filters"):
            family_filter = st.selectbox("Family", ["all"] + meta["families"], key="rev_family")
            rating_range = st.slider("Rating range", 1, 5, (1, 5), key="rev_rating")
            sentiments = st.multiselect(
                "Sentiment", ["positive", "neutral", "negative"],
                default=["positive", "neutral", "negative"], key="rev_sent",
            )

    fk = None if family_filter == "all" else family_filter
    sent_tuple = tuple(sentiments) if sentiments else None

    total = dl.review_count(focus_keyword=fk, sentiment_labels=sent_tuple)

    with body:
        ui.page_header(
            kicker="Review explorer",
            title=f"{total:,} reviews",
            sub="Slice individual reviews and explore the semantic landscape",
        )

        with ui.card("Topic landscape · UMAP",
                     sub="random 3k sample · top 10 topics shown · outlier (-1) hidden"):
            umap_df = dl.umap_sample(focus_keyword=fk, sentiment_labels=sent_tuple, limit=3000)
            if not umap_df.empty:
                topic_labels = (umap_df.dropna(subset=["topic_label"])
                                .groupby("topic_id")["topic_label"].first().to_dict())
                points = [{"x": float(r.umap_x), "y": float(r.umap_y),
                           "t": int(r.topic_id) if r.topic_id is not None else 0}
                          for r in umap_df.itertuples()]
                st.plotly_chart(
                    charts.umap_scatter(
                        points,
                        topic_labels={int(k): str(v) for k, v in topic_labels.items()},
                        height=460, top_n_topics=10,
                    ),
                    use_container_width=True, key="rev_umap",
                )
            else:
                st.info("No reviews with UMAP coords match the filter.")

        st.markdown("&nbsp;", unsafe_allow_html=True)

        col_freq, col_time = st.columns([7, 5])

        with col_freq:
            with ui.card("Topic frequency", sub="top 12 BERTopic clusters · outlier excluded"):
                tf = dl.topic_frequency(focus_keyword=fk, top=12)
                if not tf.empty:
                    max_n = int(tf["n"].max())
                    colors = ["#F0647A", "#14B8A6", "#A78BFA", "#F59E0B", "#38BDF8",
                              "#F89BAB", "#5DD4C5", "#C4B0FB", "#FBC56B", "#7DD3F8",
                              "#F0647A", "#14B8A6"]
                    html = ""
                    for i, r in enumerate(tf.itertuples()):
                        pct = r.n / max_n * 100
                        label = (r.label or "—")[:42]
                        html += f"""
<div style="display:flex;align-items:center;gap:10px;font-size:12.5px;margin-bottom:6px;color:var(--t1);">
  <span style="width:200px;">{label}</span>
  <div style="flex:1;height:12px;background:var(--bg);border-radius:4px;">
    <div style="width:{pct:.0f}%;height:100%;background:{colors[i % len(colors)]};border-radius:4px;"></div>
  </div>
  <span class="mono" style="width:50px;text-align:right;font-weight:600;">{r.n:,}</span>
</div>"""
                    st.markdown(html, unsafe_allow_html=True)
                else:
                    st.info("No topics for this filter.")

        with col_time:
            with ui.card("Sentiment timeline", sub="daily mean sentiment · last year"):
                tl = dl.sentiment_timeline(focus_keyword=fk, days=365)
                if not tl.empty and len(tl) >= 2:
                    st.plotly_chart(
                        charts.line_chart([{
                            "color": "#C94060", "label": "sentiment",
                            "data": tl["sentiment"].fillna(0).tolist(),
                        }], height=200),
                        use_container_width=True, key="rev_sent_tl",
                    )
                else:
                    st.info("Not enough dated reviews for a timeline.")

                trust = dl.trust_signals(focus_keyword=fk)
                st.markdown(
                    f"""<div style="border-top:1px solid var(--line);margin-top:14px;padding-top:14px;
                                display:flex;justify-content:space-between;font-size:12px;color:var(--t1);">
                      <div><span style="color:var(--t2)">incentivized</span>
                        <span class="mono" style="font-weight:600">{trust['pct_incentivized']*100:.1f}%</span></div>
                      <div><span style="color:var(--t2)">staff</span>
                        <span class="mono" style="font-weight:600">{trust['pct_staff']*100:.1f}%</span></div>
                      <div><span style="color:var(--t2)">featured</span>
                        <span class="mono" style="font-weight:600">{trust['pct_featured']*100:.1f}%</span></div>
                    </div>""",
                    unsafe_allow_html=True,
                )

        st.markdown("&nbsp;", unsafe_allow_html=True)

        with ui.card("Reviews",
                     sub=f"latest 100 of {total:,} matching · sortable",
                     actions="↓ Export CSV"):
            table = dl.reviews_table(
                focus_keyword=fk,
                sentiment_labels=sent_tuple,
                rating_min=rating_range[0],
                rating_max=rating_range[1],
                limit=100,
            )
            if not table.empty:
                cols_show = ["title", "brand", "product_name", "rating",
                             "sentiment_label", "skin_type", "age_range",
                             "helpful_ratio", "date", "snippet"]
                cols_show = [c for c in cols_show if c in table.columns]
                st.dataframe(table[cols_show], use_container_width=True, hide_index=True)
            else:
                st.info("No reviews match.")
