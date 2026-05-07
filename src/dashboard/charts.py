"""Plotly chart helpers tuned to the MarketPulse Beauty design system.

Each helper returns a `plotly.graph_objects.Figure` ready for `st.plotly_chart`.
Colors and fonts come from `src.dashboard.theme` so charts stay in sync with the
CSS tokens.
"""
from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.dashboard import theme as th


def _base(fig: go.Figure, height: int = 280) -> go.Figure:
    fig.update_layout(height=height, **th.PLOTLY_LAYOUT)
    return fig


# ── Histogram with median line ────────────────────────────────────────────────
def histogram(values: Sequence[float], median: float | None = None,
              bins: int = 20, height: int = 220) -> go.Figure:
    fig = go.Figure()
    if len(values):
        fig.add_trace(go.Histogram(
            x=values, nbinsx=bins,
            marker=dict(color=th.CORAL, line=dict(color=th.CORAL_DARK, width=0.5)),
            hovertemplate="%{y} products<br>score %{x}<extra></extra>",
        ))
    if median is not None:
        fig.add_vline(x=median, line=dict(color=th.T1, dash="dash", width=1.2),
                      annotation_text=f"median {median:.0f}",
                      annotation_position="top right",
                      annotation_font=dict(family="JetBrains Mono", size=11, color=th.T1))
    _base(fig, height)
    fig.update_layout(bargap=0.06, showlegend=False)
    return fig


# ── Horizontal bars (Top 10 / Bottom 10) ──────────────────────────────────────
def hbars(items: list[dict], color: str | None = None, height: int = 320,
          color_by_family: bool = True) -> go.Figure:
    items = list(items)
    labels = [f"<b>{it.get('brand','')}</b> · {it.get('name','')}" for it in items]
    values = [it.get("v", 0) for it in items]
    if color_by_family:
        colors = [th.FAMILY_COLORS.get(it.get("fam"), th.CORAL) for it in items]
    else:
        colors = [color or th.CORAL_DARK] * len(items)

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color=colors),
        text=[f"{v:.0f}" for v in values], textposition="outside",
        textfont=dict(family="JetBrains Mono", size=11, color=th.T1),
        hovertemplate="%{y}<br>score <b>%{x}</b><extra></extra>",
    ))
    _base(fig, height)
    fig.update_layout(
        margin=dict(l=10, r=40, t=10, b=20),
        yaxis=dict(autorange="reversed", tickfont=dict(size=11, color=th.T1)),
        xaxis=dict(showgrid=True, gridcolor=th.LINE),
        showlegend=False,
    )
    return fig


# ── Velocity scatter ──────────────────────────────────────────────────────────
def velocity_scatter(prior: Sequence[float], last: Sequence[float],
                     sizes: Sequence[float], names: Sequence[str] | None = None,
                     height: int = 300) -> go.Figure:
    # Coerce to plain numpy floats and drop any NaN rows — Plotly silently
    # hides markers whose x/y/size is NaN, which produces an "empty" chart.
    prior = pd.to_numeric(pd.Series(prior), errors="coerce").to_numpy(dtype=float)
    last = pd.to_numeric(pd.Series(last), errors="coerce").to_numpy(dtype=float)
    sizes = pd.to_numeric(pd.Series(sizes), errors="coerce").to_numpy(dtype=float)
    names_arr = np.asarray(list(names) if names else [""] * len(prior), dtype=object)
    valid = ~(np.isnan(prior) | np.isnan(last) | np.isnan(sizes))
    prior, last, sizes, names_arr = prior[valid], last[valid], sizes[valid], names_arr[valid]

    if not len(prior):
        return _base(go.Figure(), height)
    colors = [th.C2_TEAL if l > p else th.CORAL_DARK for p, l in zip(prior, last)]
    hover_names = names_arr.tolist()
    max_v = float(max(prior.max(), last.max(), 1))
    # Compute marker pixel sizes manually (sqrt scaling, capped) and pass as a
    # plain Python list. Streamlit's plotly_chart sometimes drops markers when
    # the figure contains numpy arrays in size/color, even though Plotly's own
    # HTML writer accepts them.
    pixel_sizes = np.clip(np.sqrt(sizes) * 1.6, 5, 28).tolist()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[0.0, max_v], y=[0.0, max_v], mode="lines",
        line=dict(color=th.T3, dash="dash", width=1),
        hoverinfo="skip", showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=prior.tolist(), y=last.tolist(), mode="markers",
        marker=dict(size=pixel_sizes, color=colors, opacity=0.78,
                    line=dict(color="#fff", width=1.2)),
        text=hover_names,
        hovertemplate="<b>%{text}</b><br>prior <b>%{x:.0f}</b> → last <b>%{y:.0f}</b><extra></extra>",
        showlegend=False,
    ))
    _base(fig, height)
    fig.update_layout(
        xaxis=dict(title="prior 30d velocity →", range=[0, max_v * 1.05]),
        yaxis=dict(title="last 30d velocity", range=[0, max_v * 1.05]),
    )
    return fig


# ── Donut ─────────────────────────────────────────────────────────────────────
def donut(labels: Sequence[str], values: Sequence[float],
          colors: Sequence[str] | None = None, height: int = 220) -> go.Figure:
    colors = colors or [th.C2_TEAL, th.C4_AMBER, th.CORAL_DARK]
    fig = go.Figure(go.Pie(
        labels=list(labels), values=list(values), hole=0.62,
        marker=dict(colors=list(colors), line=dict(color="#fff", width=2)),
        textinfo="none",
        hovertemplate="%{label}<br><b>%{value}</b> (%{percent})<extra></extra>",
    ))
    total = int(sum(values))
    fig.update_layout(
        height=height,
        annotations=[dict(text=f"<b>{total:,}</b><br><span style='color:{th.T2};font-size:11px'>reviews</span>",
                          x=0.5, y=0.5, font=dict(family="JetBrains Mono", size=20, color=th.T1),
                          showarrow=False)],
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ── Demand vs Supply (line + bars + spike bands) ──────────────────────────────
def demand_vs_supply(months: Sequence[str], search_line: Sequence[float],
                     review_bars: Sequence[float],
                     spike_windows: Sequence[tuple[int, int]] | None = None,
                     height: int = 280) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=list(months), y=list(review_bars), name="reviews (supply)",
        marker=dict(color=th.C2_TEAL, opacity=0.78),
        hovertemplate="%{x}<br>reviews <b>%{y}</b><extra></extra>",
    ), secondary_y=True)
    fig.add_trace(go.Scatter(
        x=list(months), y=list(search_line), name="search interest (demand)",
        mode="lines+markers",
        line=dict(color=th.CORAL, width=2.5),
        marker=dict(size=6, color="#fff", line=dict(color=th.CORAL, width=1.5)),
        hovertemplate="%{x}<br>search <b>%{y}</b><extra></extra>",
    ), secondary_y=False)
    if spike_windows:
        for s, e in spike_windows:
            if 0 <= s < len(months) and 0 <= e < len(months):
                fig.add_vrect(x0=months[s], x1=months[e],
                              fillcolor="rgba(245,158,11,0.16)", line_width=0)
    _base(fig, height)
    fig.update_yaxes(title_text="search interest", secondary_y=False, color=th.CORAL_DARK)
    fig.update_yaxes(title_text="reviews", secondary_y=True, color="#0F766E")
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        bargap=0.45,
    )
    return fig


# ── Stacked area for sentiment over time + rating line overlay ────────────────
def sentiment_area(months: Sequence[str],
                   pos: Sequence[float], neu: Sequence[float], neg: Sequence[float],
                   rating_line: Sequence[float] | None = None,
                   height: int = 240) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=list(months), y=list(pos), name="positive",
        mode="lines", stackgroup="s",
        line=dict(color=th.C2_TEAL, width=0), fillcolor="rgba(20,184,166,0.85)",
        hovertemplate="%{x}<br>positive %{y:.0f}%<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=list(months), y=list(neu), name="neutral",
        mode="lines", stackgroup="s",
        line=dict(color=th.C4_AMBER, width=0), fillcolor="rgba(245,158,11,0.80)",
        hovertemplate="%{x}<br>neutral %{y:.0f}%<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=list(months), y=list(neg), name="negative",
        mode="lines", stackgroup="s",
        line=dict(color=th.CORAL_DARK, width=0), fillcolor="rgba(201,64,96,0.75)",
        hovertemplate="%{x}<br>negative %{y:.0f}%<extra></extra>",
    ), secondary_y=False)
    if rating_line is not None:
        fig.add_trace(go.Scatter(
            x=list(months), y=list(rating_line), name="avg rating",
            mode="lines+markers",
            line=dict(color=th.T1, width=2),
            marker=dict(size=5, color="#fff", line=dict(color=th.T1, width=1.5)),
            hovertemplate="%{x}<br>rating <b>%{y:.2f}</b><extra></extra>",
        ), secondary_y=True)
    _base(fig, height)
    fig.update_yaxes(title_text="% reviews", range=[0, 100], secondary_y=False)
    fig.update_yaxes(title_text="rating", range=[3, 5], secondary_y=True)
    fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0))
    return fig


# ── Daily review bars + 7d MA ─────────────────────────────────────────────────
def daily_bars_ma(daily: Sequence[float], ma: Sequence[float],
                  height: int = 180) -> go.Figure:
    x = list(range(len(daily)))
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=x, y=list(daily), name="daily reviews",
        marker=dict(color=th.C5_SKY, opacity=0.55),
        hovertemplate="day %{x}<br>%{y} reviews<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=x, y=list(ma), name="7d MA",
        mode="lines", line=dict(color=th.CORAL_DARK, width=2),
        hovertemplate="day %{x}<br>MA %{y:.1f}<extra></extra>",
    ))
    _base(fig, height)
    fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                      bargap=0.15)
    return fig


# ── Radar (brand profile) ─────────────────────────────────────────────────────
def radar(axes: Sequence[str], series: list[dict], height: int = 320) -> go.Figure:
    """Radar with optional `absolute` strings per axis for hover tooltips.

    Each series may include an `absolute` key (list aligned with `axes`)
    holding pre-formatted strings (e.g. "4.88", "98.6%", "+0.85"). When
    present they're shown in the hover instead of the normalized 0–1 number,
    which makes per-axis relative scaling readable for the user.
    """
    fig = go.Figure()
    closed_axes = list(axes) + [axes[0]]
    for s in series:
        vals = [float(v) for v in s["values"]]
        vals = vals + [vals[0]]
        absolute = s.get("absolute")
        if absolute:
            text = list(absolute) + [absolute[0]]
        else:
            text = [f"{v:.2f}" for v in vals]
        fig.add_trace(go.Scatterpolar(
            r=vals, theta=closed_axes, fill="toself",
            name=s.get("name", ""),
            line=dict(color=s.get("color", th.CORAL), width=2),
            opacity=0.55,
            text=text,
            hovertemplate="<b>%{theta}</b>: %{text}<extra>%{fullData.name}</extra>",
        ))
    fig.update_layout(
        height=height,
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 1], gridcolor=th.LINE,
                            tickfont=dict(family="JetBrains Mono", size=10, color=th.T2)),
            angularaxis=dict(tickfont=dict(family="DM Sans", size=11, color=th.T1)),
            bgcolor="rgba(0,0,0,0)",
        ),
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.05, x=0.5, xanchor="center",
                    font=dict(size=11)),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40, r=40, t=10, b=40),
    )
    return fig


# ── UMAP scatter ──────────────────────────────────────────────────────────────
def umap_scatter(points: list[dict], topic_labels: dict[int, str] | None = None,
                 height: int = 380, top_n_topics: int = 10) -> go.Figure:
    """Scatter of UMAP points coloured by topic.

    With BERTopic outputs that have hundreds of topics, the legend would crush
    the plot. We keep the top-N most populated topics with named colors and
    bucket the rest into a single neutral "other" series.
    """
    if not points:
        fig = go.Figure()
        _base(fig, height)
        return fig
    df = pd.DataFrame(points)
    # Drop NaN points; force numeric coords; coerce topic to int.
    df["x"] = pd.to_numeric(df["x"], errors="coerce")
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df["t"] = pd.to_numeric(df["t"], errors="coerce").fillna(-1).astype(int)
    df = df.dropna(subset=["x", "y"])
    if df.empty:
        return _base(go.Figure(), height)

    counts = df["t"].value_counts()
    top_topics = list(counts.head(top_n_topics).index)
    df["bucket"] = df["t"].where(df["t"].isin(top_topics), other=-9999)

    fig = go.Figure()
    # Plot "other" first so named topics sit on top. Pass plain Python lists.
    if (df["bucket"] == -9999).any():
        sub = df[df["bucket"] == -9999]
        fig.add_trace(go.Scatter(
            x=sub["x"].tolist(), y=sub["y"].tolist(), mode="markers",
            name=f"other ({len(sub):,})",
            marker=dict(size=4, color="#D1D5DB", opacity=0.45,
                        line=dict(color="#fff", width=0.3)),
            hovertemplate="other topic<extra></extra>",
        ))
    for i, tid in enumerate(top_topics):
        sub = df[df["bucket"] == tid]
        color = th.DATA_PALETTE[i % len(th.DATA_PALETTE)]
        raw = (topic_labels or {}).get(int(tid), f"topic {int(tid)}")
        label = (raw[:40] + "…") if len(raw) > 40 else raw
        fig.add_trace(go.Scatter(
            x=sub["x"].tolist(), y=sub["y"].tolist(), mode="markers",
            name=f"{label} ({len(sub):,})",
            marker=dict(size=6, color=color, opacity=0.85,
                        line=dict(color="#fff", width=0.5)),
            hovertemplate=f"<b>{label}</b><extra></extra>",
        ))
    _base(fig, height)
    fig.update_layout(
        xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
        yaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
        plot_bgcolor="#FBFAF8",
        legend=dict(orientation="v", yanchor="top", y=1, x=1.02,
                    font=dict(size=10.5)),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    return fig


# ── Heatmap ───────────────────────────────────────────────────────────────────
def heatmap(rows: Sequence[str], cols: Sequence[str],
            values: Sequence[Sequence[float]],
            text_values: Sequence[Sequence[str]] | None = None,
            height: int = 340) -> go.Figure:
    """Heatmap with steep color transitions and optional pre-formatted cell text.

    `values` is interpreted as colour intensity in [0, 1]. `text_values`
    overrides the displayed cell text — pass absolute formatted strings here
    when `values` has been per-column min-max normalised so the cells still
    show the real metric (e.g. "4.88" or "94.6%").
    """
    z_np = np.asarray(values, dtype=float)
    z_np = np.nan_to_num(z_np, nan=0.0)
    z = z_np.tolist()
    if text_values is None:
        text = [[f"{int(v * 100)}" for v in row] for row in z]
    else:
        text = [list(row) for row in text_values]
    # Steeper colour transitions — small numerical differences pop visually.
    colorscale = [
        [0.00, th.CORAL_DARK],
        [0.20, "#E07A8E"],
        [0.40, "#F59E0B"],
        [0.55, "#FBC56B"],
        [0.70, "#5DD4C5"],
        [1.00, th.C2_TEAL],
    ]
    fig = go.Figure(go.Heatmap(
        z=z, x=list(cols), y=list(rows),
        colorscale=colorscale,
        text=text,
        texttemplate="%{text}",
        textfont=dict(family="JetBrains Mono", size=11, color="#fff"),
        zmin=0, zmax=1, showscale=True,
        colorbar=dict(thickness=10, len=0.7,
                      tickvals=[0, 0.5, 1], ticktext=["low", "mid", "high"],
                      tickfont=dict(family="JetBrains Mono", size=10, color=th.T2)),
        hovertemplate="<b>%{y}</b> · %{x}<br><b>%{text}</b><extra></extra>",
    ))
    _base(fig, height)
    return fig


# ── Box plot ──────────────────────────────────────────────────────────────────
def boxplot_health(boxes: list[dict], height: int = 240) -> go.Figure:
    fig = go.Figure()
    for b in boxes:
        fig.add_trace(go.Box(
            name=b["label"],
            q1=[b["q1"]], median=[b["med"]], q3=[b["q3"]],
            lowerfence=[b["min"]], upperfence=[b["max"]],
            marker=dict(color=th.CORAL),
            fillcolor="rgba(255,228,233,0.7)", line=dict(color=th.CORAL_DARK),
            boxpoints=False,
        ))
    _base(fig, height)
    fig.update_layout(showlegend=False, yaxis=dict(range=[0, 100], title="health score"))
    return fig


# ── Stacked horizontal bar ────────────────────────────────────────────────────
def stacked_hbar(segments: list[dict], height: int = 80) -> go.Figure:
    total = float(sum(s["v"] for s in segments)) or 1.0
    fig = go.Figure()
    for s in segments:
        pct = s["v"] / total * 100
        fig.add_trace(go.Bar(
            x=[pct], y=["share"], orientation="h", name=s["label"],
            marker=dict(color=s.get("c", th.CORAL)),
            text=[f"{pct:.0f}%"], textposition="inside",
            insidetextfont=dict(color="#fff", family="JetBrains Mono", size=11),
            hovertemplate=f"<b>{s['label']}</b><br>%{{x:.1f}}%<extra></extra>",
        ))
    _base(fig, height)
    fig.update_layout(
        barmode="stack", showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.5, x=0, font=dict(size=11)),
        xaxis=dict(showgrid=False, showticklabels=False),
        yaxis=dict(showticklabels=False),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    return fig


# ── Brand ranking horizontal bars (with selection highlight) ──────────────────
def ranking_bars(items: list[dict], selected: set[str] | None = None,
                 height: int = 360) -> go.Figure:
    selected = selected or set()
    items = list(items)
    labels = [it["brand"] for it in items]
    values = [it["health"] for it in items]
    colors = [th.CORAL if lbl in selected else th.T3 for lbl in labels]
    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color=colors),
        text=[f"{v:.0f}" for v in values], textposition="outside",
        textfont=dict(family="JetBrains Mono", size=11, color=th.T1),
        hovertemplate="<b>%{y}</b><br>health <b>%{x}</b><extra></extra>",
    ))
    _base(fig, height)
    fig.update_layout(
        yaxis=dict(autorange="reversed"),
        xaxis=dict(range=[0, 100]),
        showlegend=False,
        margin=dict(l=10, r=40, t=10, b=20),
    )
    return fig


# ── Multi-line search trends with spike band ──────────────────────────────────
def search_trends(series: list[dict], days: int = 90,
                  spike: dict | None = None, height: int = 280) -> go.Figure:
    fig = go.Figure()
    x = list(range(days))
    for s in series:
        fig.add_trace(go.Scatter(
            x=x, y=list(s["data"]), name=s["label"], mode="lines",
            line=dict(color=s["color"], width=1.8),
            hovertemplate=f"<b>{s['label']}</b><br>day %{{x}}: %{{y:.0f}}<extra></extra>",
        ))
    if spike:
        fig.add_vrect(x0=spike["start"], x1=spike["end"],
                      fillcolor="rgba(245,158,11,0.18)", line_width=0,
                      annotation_text=spike.get("label", ""),
                      annotation_position="top left",
                      annotation_font=dict(family="JetBrains Mono", size=11, color="#92400E"))
    _base(fig, height)
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        yaxis=dict(range=[0, 100]),
    )
    return fig


# ── Pipeline run history ──────────────────────────────────────────────────────
def pipeline_runs(runs: list[dict], height: int = 200) -> go.Figure:
    x = list(range(len(runs)))
    y = [r["duration"] for r in runs]
    colors = [th.CORAL_DARK if r["status"] == "fail" else th.C2_TEAL for r in runs]
    fig = go.Figure(go.Bar(
        x=x, y=y, marker=dict(color=colors),
        hovertemplate="run %{x}<br>duration <b>%{y:.1f}m</b><extra></extra>",
    ))
    _base(fig, height)
    fig.update_layout(
        showlegend=False, bargap=0.15,
        yaxis=dict(title="duration (min)"),
        xaxis=dict(title="run index"),
    )
    return fig


# ── Simple line chart (sentiment timeline) ────────────────────────────────────
def line_chart(series: list[dict], height: int = 200) -> go.Figure:
    fig = go.Figure()
    for s in series:
        fig.add_trace(go.Scatter(
            x=list(range(len(s["data"]))), y=list(s["data"]),
            mode="lines", name=s.get("label", ""),
            line=dict(color=s.get("color", th.CORAL_DARK), width=2),
            hovertemplate="day %{x}: %{y:.2f}<extra></extra>",
        ))
    _base(fig, height)
    fig.update_layout(showlegend=False)
    return fig
