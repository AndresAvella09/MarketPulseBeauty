"""Reusable Streamlit components rendered as styled HTML.

These wrap `st.markdown(..., unsafe_allow_html=True)` so the dashboard can
match the design tokens defined in `src.dashboard.theme`.
"""
from __future__ import annotations

from html import escape
from typing import Iterable

import streamlit as st


def page_header(kicker: str, title: str, sub: str | None = None,
                breadcrumb: str | None = None,
                chips: Iterable[tuple[str, str]] | None = None) -> None:
    parts: list[str] = []
    if breadcrumb:
        parts.append(f'<div class="mp-breadcrumb">← {escape(breadcrumb)}</div>')
    parts.append(f'<div class="mp-kicker">{escape(kicker)}</div>')
    parts.append(f'<h1 class="mp-h1">{escape(title)}</h1>')
    if sub:
        parts.append(f'<div class="mp-sub">{escape(sub)}</div>')
    if chips:
        chip_html = "".join(filter_chip_html(k, v) for k, v in chips)
        parts.append(f'<div class="mp-filterstrip">{chip_html}</div>')
    st.markdown("".join(parts), unsafe_allow_html=True)


def filter_chip_html(key: str, value: str) -> str:
    k = f'<span class="k">{escape(key)}</span>' if key else ""
    return f'<span class="fchip">{k}<span class="v">{escape(value)}</span></span>'


def top_bar(refresh: str, date_range: str = "Last 90 days") -> None:
    html = f"""
<div class="mp-top">
  <div class="mp-search">🔎 <span>Search products, brands, families…</span></div>
  <div style="display:flex; gap:10px; align-items:center; margin-left:auto;">
    <div class="mp-chip dot">{escape(refresh)}</div>
    <div class="mp-chip">📅 {escape(date_range)}</div>
    <button class="mp-btn">🔗 Share view</button>
  </div>
</div>
"""
    st.markdown(html, unsafe_allow_html=True)


def kpi_card(label: str, value: str,
             delta: str | None = None, delta_up: bool | None = None,
             meta: str | None = None, active: bool = False) -> str:
    classes = "mp-kpi" + (" active" if active else "")
    delta_html = ""
    if delta:
        if delta_up is True:
            delta_html = f'<div class="delta up">▲ {escape(delta)}</div>'
        elif delta_up is False:
            delta_html = f'<div class="delta down">▼ {escape(delta)}</div>'
        else:
            delta_html = f'<div class="meta">{escape(delta)}</div>'
    meta_html = f'<div class="meta">{escape(meta)}</div>' if meta else ""
    return f"""
<div class="{classes}">
  <div class="lbl">{escape(label)}</div>
  <div class="val">{escape(value)}</div>
  {delta_html}{meta_html}
</div>
"""


def kpi_strip(items: list[dict]) -> None:
    cols = st.columns(len(items), gap="medium")
    for col, item in zip(cols, items):
        with col:
            st.markdown(kpi_card(**item), unsafe_allow_html=True)


def card(title: str, sub: str | None = None, actions: str | None = None):
    """Bordered Streamlit container with a styled header.

    Use as a context manager:
        with ui.card("Title", sub="…"):
            st.plotly_chart(fig, use_container_width=True)
    """
    container = st.container(border=True)
    sub_html = f'<div class="sub">{escape(sub)}</div>' if sub else ""
    actions_html = f'<div class="actions">{actions}</div>' if actions else ""
    container.markdown(
        f'<div class="mp-card-head"><div>'
        f'<div class="title">{escape(title)}</div>{sub_html}</div>'
        f'{actions_html}</div>',
        unsafe_allow_html=True,
    )
    return container


# Back-compat shims for any caller still using the old open/close API.
def card_open(title: str, sub: str | None = None, actions: str | None = None) -> None:
    sub_html = f'<div class="sub">{escape(sub)}</div>' if sub else ""
    actions_html = f'<div class="actions">{actions}</div>' if actions else ""
    st.markdown(
        f'<div class="mp-card"><div class="mp-card-head"><div>'
        f'<div class="title">{escape(title)}</div>{sub_html}</div>'
        f'{actions_html}</div>',
        unsafe_allow_html=True,
    )


def card_close() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def stars(rating: float | None) -> str:
    if rating is None:
        return '<span class="stars">— — — — —</span>'
    filled = int(round(rating))
    return ('<span class="stars">' +
            "".join("★" if i < filled else '<span class="off">★</span>' for i in range(5)) +
            "</span>")


def sent_chip(label: str) -> str:
    label = (label or "").lower()
    if label.startswith("pos"):
        cls, txt = "pos", "positive"
    elif label.startswith("neg"):
        cls, txt = "neg", "negative"
    else:
        cls, txt = "neu", "neutral"
    return f'<span class="sent-chip {cls}">{txt}</span>'


def theme_pill(label: str, pct: int, polarity: str = "pos") -> str:
    cls = "pos" if polarity.startswith("pos") else "neg"
    return f'<span class="theme-pill {cls}">{escape(label)}<span class="pct">{pct}%</span></span>'


def low_n(n: int) -> str:
    return f'<span class="lown">N={n}</span>'


def quote_card(text: str, who: str, polarity: str) -> str:
    cls = {"pos": "pos", "neu": "neu", "neg": "neg"}.get(polarity, "neu")
    pretty = {"pos": "positive", "neu": "neutral", "neg": "negative"}[cls]
    return (f'<div class="quote-card {cls}">'
            f'<span class="sent-chip {cls}">{pretty}</span>'
            f'<div style="margin-top:10px">{escape(text)}</div>'
            f'<div class="who">{escape(who)}</div></div>')


def render_sidebar(active_key: str, nav_groups: list[dict],
                   user: tuple[str, str, str]) -> str:
    """Render the dark sidebar nav and return the chosen page key.

    Streamlit's native radio drives selection; brand/group headers/footer are
    rendered around it via st.markdown HTML.
    """
    initials, name, role = user

    with st.sidebar:
        st.markdown(
            '<div class="mp-brand"><div class="mark"></div>'
            'Market<em>Pulse</em></div>',
            unsafe_allow_html=True,
        )

        # Build a single flat list of options with section dividers via labels.
        flat: list[tuple[str, str]] = []
        for grp in nav_groups:
            for it in grp["items"]:
                flat.append((it["key"], f"{it['icon']}  {it['label']}"))

        labels = [lbl for _, lbl in flat]
        keys = [k for k, _ in flat]
        try:
            idx = keys.index(active_key)
        except ValueError:
            idx = 0

        # Render section headers manually before each group's first item using
        # an HTML block, then a single radio for selection.
        st.markdown('<div class="mp-nav-section">Analytics</div>', unsafe_allow_html=True)
        choice = st.radio(
            label="navigation",
            options=labels,
            index=idx,
            label_visibility="collapsed",
            key="mp_nav",
        )

        st.markdown(
            f'<div class="mp-side-foot">'
            f'<div class="av">{escape(initials)}</div>'
            f'<div><div style="color:#fff;font-size:12.5px">{escape(name)}</div>'
            f'<div>{escape(role)}</div></div></div>',
            unsafe_allow_html=True,
        )

    return keys[labels.index(choice)]
