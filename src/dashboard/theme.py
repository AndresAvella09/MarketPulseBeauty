"""Design tokens and CSS injector for the MarketPulse Beauty dashboard.

Mirrors the design tokens from the Claude Design handoff
(ui_kits/dashboard_design_ref/tokens.css) so Streamlit and Plotly look the same
as the React mockup.
"""
from __future__ import annotations

import streamlit as st


# ── Colors ────────────────────────────────────────────────────────────────────
CORAL = "#F0647A"
CORAL_TINT = "#FFE4E9"
CORAL_DARK = "#C94060"

C1_CORAL = "#F0647A"
C2_TEAL = "#14B8A6"
C3_LAVENDER = "#A78BFA"
C4_AMBER = "#F59E0B"
C5_SKY = "#38BDF8"

C1_CORAL_TINT = "#F89BAB"
C2_TEAL_TINT = "#5DD4C5"
C3_LAVENDER_TINT = "#C4B0FB"
C4_AMBER_TINT = "#FBC56B"
C5_SKY_TINT = "#7DD3F8"

SUCCESS = "#22C55E"
WARNING = "#F59E0B"
DANGER = "#EF4444"
INFO = "#38BDF8"

POS = C2_TEAL
NEU = C4_AMBER
NEG = CORAL_DARK

BG = "#FAFAF9"
CARD = "#FFFFFF"
SIDEBAR = "#0D1117"
SIDEBAR_2 = "#161B22"

T1 = "#111827"
T2 = "#6B7280"
T3 = "#9CA3AF"
LINE = "#ECECEA"

DATA_PALETTE = [C1_CORAL, C2_TEAL, C3_LAVENDER, C4_AMBER, C5_SKY,
                C1_CORAL_TINT, C2_TEAL_TINT, C3_LAVENDER_TINT, C4_AMBER_TINT, C5_SKY_TINT]

FAMILY_COLORS = {
    "acido_hialuronico": C1_CORAL,
    "niacinamida": C2_TEAL,
    "shampoo_sin_sulfatos": C3_LAVENDER,
    "retinol": C4_AMBER,
    "vitamina_c": C5_SKY,
}


PLOTLY_LAYOUT = dict(
    font=dict(family="DM Sans, system-ui, sans-serif", color=T1, size=12),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=40, r=20, t=20, b=36),
    xaxis=dict(gridcolor=LINE, zerolinecolor=LINE, tickfont=dict(family="JetBrains Mono, monospace", size=11, color=T2)),
    yaxis=dict(gridcolor=LINE, zerolinecolor=LINE, tickfont=dict(family="JetBrains Mono, monospace", size=11, color=T2)),
    hoverlabel=dict(bgcolor=CORAL_DARK, font=dict(color="white", family="JetBrains Mono, monospace")),
    legend=dict(font=dict(size=11, color=T2), bgcolor="rgba(0,0,0,0)"),
)


def inject_css() -> None:
    """Inject the design system CSS into the Streamlit app once per session."""
    st.markdown(
        f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600;700&display=swap');

:root {{
  --coral: {CORAL}; --coral-tint: {CORAL_TINT}; --coral-dark: {CORAL_DARK};
  --c1-coral: {C1_CORAL}; --c2-teal: {C2_TEAL}; --c3-lavender: {C3_LAVENDER};
  --c4-amber: {C4_AMBER}; --c5-sky: {C5_SKY};
  --pos: {POS}; --neu: {NEU}; --neg: {NEG};
  --success: {SUCCESS}; --warning: {WARNING}; --danger: {DANGER};
  --bg: {BG}; --card: {CARD}; --sidebar: {SIDEBAR}; --sidebar-2: {SIDEBAR_2};
  --t1: {T1}; --t2: {T2}; --t3: {T3}; --line: {LINE};
  --serif: 'Playfair Display', Georgia, serif;
  --sans: 'DM Sans', system-ui, sans-serif;
  --mono: 'JetBrains Mono', ui-monospace, monospace;
  --sh-sm: 0 1px 2px rgba(17,24,39,0.04), 0 1px 3px rgba(17,24,39,0.05);
  --sh-md: 0 2px 4px rgba(17,24,39,0.04), 0 6px 16px rgba(17,24,39,0.06);
  --sh-pop: 0 8px 24px rgba(201,64,96,0.18), 0 2px 6px rgba(17,24,39,0.08);
}}

html, body, [class*="css"] {{
  font-family: var(--sans);
  color: var(--t1);
}}
.stApp {{ background: var(--bg); }}

/* Hide default Streamlit chrome */
#MainMenu, header[data-testid="stHeader"], footer {{ visibility: hidden; height: 0; }}
.block-container {{ padding-top: 1rem; padding-bottom: 2rem; max-width: 1440px; }}

/* ── Sidebar (dark) ── */
section[data-testid="stSidebar"] {{
  background: var(--sidebar) !important;
  border-right: 1px solid #21262D;
}}
section[data-testid="stSidebar"] * {{ color: #C9D1D9 !important; }}
section[data-testid="stSidebar"] .stRadio label {{
  padding: 9px 10px; border-radius: 8px;
  font-size: 13.5px; font-weight: 500; cursor: pointer;
  display: flex; align-items: center; gap: 12px;
}}
section[data-testid="stSidebar"] .stRadio label:hover {{ background: var(--sidebar-2); }}
section[data-testid="stSidebar"] .stRadio [role="radiogroup"] {{ gap: 2px; }}
section[data-testid="stSidebar"] .stRadio [role="radio"][aria-checked="true"] + div p {{
  color: #fff !important; font-weight: 600 !important;
}}

.mp-brand {{
  display: flex; align-items: center; gap: 10px; padding: 4px 8px 22px 8px;
  font-family: var(--serif); font-weight: 700; font-size: 17px; color: #fff;
  letter-spacing: -0.01em;
}}
.mp-brand .mark {{
  width: 28px; height: 28px; border-radius: 8px;
  background: linear-gradient(135deg, var(--coral) 0%, var(--coral-dark) 100%);
  display: grid; place-items: center;
  box-shadow: 0 6px 16px rgba(240,100,122,0.35);
}}
.mp-brand em {{ font-style: normal; color: var(--coral); }}
.mp-nav-section {{
  font-size: 10px; text-transform: uppercase; letter-spacing: 0.14em;
  color: #6E7681; padding: 14px 10px 6px; font-family: var(--sans);
}}
.mp-side-foot {{
  margin-top: 24px; padding: 12px 10px; border-top: 1px solid #21262D;
  display: flex; align-items: center; gap: 10px; color: #6E7681; font-size: 12px;
}}
.mp-side-foot .av {{
  width: 26px; height: 26px; border-radius: 50%; background: #2A6FDB;
  display: grid; place-items: center; color: #fff; font-weight: 600; font-size: 11px;
}}

/* ── Top bar ── */
.mp-top {{
  display: flex; align-items: center; gap: 14px;
  padding: 10px 0 14px; border-bottom: 1px solid var(--line);
  margin-bottom: 18px;
}}
.mp-search {{
  flex: 1; max-width: 460px;
  display: flex; align-items: center; gap: 10px;
  background: #fff; border: 1px solid var(--line);
  padding: 8px 12px; border-radius: 10px;
  color: var(--t2); font-size: 13.5px;
  box-shadow: var(--sh-sm);
}}
.mp-chip {{
  display: inline-flex; align-items: center; gap: 6px;
  background: #fff; border: 1px solid var(--line); padding: 6px 10px; border-radius: 999px;
  font-size: 12.5px; color: var(--t1); white-space: nowrap;
}}
.mp-chip.dot::before {{
  content: ''; width: 6px; height: 6px; border-radius: 50%; background: var(--success);
  box-shadow: 0 0 0 2px rgba(34,197,94,0.18);
}}
.mp-chip.brand {{ background: var(--coral-tint); border-color: transparent; color: var(--coral-dark); }}
.mp-btn {{
  display: inline-flex; align-items: center; gap: 6px;
  background: #fff; border: 1px solid var(--line); padding: 7px 12px; border-radius: 8px;
  font-size: 13px; font-weight: 500; color: var(--t1);
  box-shadow: var(--sh-sm); cursor: pointer;
}}
.mp-btn.primary {{ background: var(--coral); color: #fff; border-color: var(--coral); }}

/* ── Title / page header ── */
.mp-kicker {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.12em;
  color: var(--t2); font-weight: 600; margin-bottom: 6px; }}
.mp-h1 {{ font-family: var(--serif); font-weight: 700; font-size: 34px;
  letter-spacing: -0.02em; line-height: 1.1; color: var(--t1); margin: 0; }}
.mp-sub {{ color: var(--t2); font-size: 13.5px; margin-top: 6px; }}
.mp-breadcrumb {{ font-size: 12px; color: var(--t2); margin-bottom: 8px; }}
.mp-filterstrip {{ display: flex; gap: 8px; flex-wrap: wrap; padding: 10px 0 18px; }}

/* ── Card ── Streamlit's bordered container provides the box; we style it */
[data-testid="stVerticalBlockBorderWrapper"] {{
  background: var(--card);
  border-radius: 14px;
  border: 1px solid var(--line) !important;
  box-shadow: var(--sh-sm);
  padding: 18px 22px !important;
  color: var(--t1);
}}
.mp-card-head {{
  display: flex; align-items: flex-start; justify-content: space-between;
  margin-bottom: 12px; gap: 12px; color: var(--t1);
}}
.mp-card-head .title {{ font-weight: 600; font-size: 15px; color: var(--t1); }}
.mp-card-head .sub {{ font-size: 12px; color: var(--t2); margin-top: 2px; }}
.mp-card-head .actions {{ color: var(--t2); font-size: 12px; }}

/* Standalone styled blocks (product tiles etc.) */
.mp-card {{
  background: var(--card); border: 1px solid var(--line);
  border-radius: 14px; padding: 18px;
  box-shadow: var(--sh-sm); color: var(--t1);
}}
.mp-card * {{ color: inherit; }}
.mp-card .muted, .mp-card .sub {{ color: var(--t2); }}

/* Force dark labels on widgets in the main body (sidebar handled separately) */
section.main [data-testid="stWidgetLabel"] p,
section.main [data-testid="stWidgetLabel"] {{ color: var(--t1) !important; font-weight: 500; }}
.main p, .main label {{ color: var(--t1); }}

/* Multiselect tag pills — coral on tint, not Streamlit's default red */
section.main span[data-baseweb="tag"] {{
  background: var(--coral-tint) !important;
  color: var(--coral-dark) !important;
  border-color: transparent !important;
}}
section.main span[data-baseweb="tag"] * {{ color: var(--coral-dark) !important; }}

/* ── KPI ── */
.mp-kpi {{
  background: var(--card); border: 1px solid var(--line);
  border-radius: 14px; padding: 18px 20px; box-shadow: var(--sh-sm);
  height: 100%;
}}
.mp-kpi.active {{ border-color: var(--coral); box-shadow: 0 0 0 3px var(--coral-tint), var(--sh-sm); }}
.mp-kpi .lbl {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.12em;
  color: var(--t2); font-weight: 600; }}
.mp-kpi .val {{ font-family: var(--mono); font-weight: 600; font-size: 28px;
  color: var(--t1); margin-top: 8px; letter-spacing: -0.01em; line-height: 1; }}
.mp-kpi .delta {{ display: inline-flex; align-items: center; gap: 4px;
  font-size: 12px; font-weight: 500; margin-top: 8px; font-family: var(--mono); }}
.mp-kpi .delta.up {{ color: var(--success); }}
.mp-kpi .delta.down {{ color: var(--danger); }}
.mp-kpi .meta {{ font-size: 11.5px; color: var(--t2); margin-top: 8px; }}

/* Sentiment chips */
.sent-chip {{ display: inline-flex; align-items: center; gap: 6px;
  padding: 3px 8px; border-radius: 999px; font-size: 11.5px; font-weight: 500; }}
.sent-chip::before {{ content: ''; width: 6px; height: 6px; border-radius: 50%; }}
.sent-chip.pos {{ background: rgba(20,184,166,0.12); color: #0F766E; }}
.sent-chip.pos::before {{ background: var(--pos); }}
.sent-chip.neu {{ background: rgba(245,158,11,0.12); color: #B45309; }}
.sent-chip.neu::before {{ background: var(--neu); }}
.sent-chip.neg {{ background: rgba(201,64,96,0.12); color: var(--coral-dark); }}
.sent-chip.neg::before {{ background: var(--neg); }}

/* Theme pills */
.theme-pill {{ display: inline-flex; align-items: center; gap: 8px;
  padding: 6px 10px; border-radius: 8px; font-size: 12.5px; font-weight: 500;
  margin: 3px 4px 3px 0; }}
.theme-pill .pct {{ font-family: var(--mono); font-size: 11.5px; opacity: .8; }}
.theme-pill.pos {{ background: rgba(20,184,166,0.10); color: #0F766E;
  border: 1px solid rgba(20,184,166,0.25); }}
.theme-pill.neg {{ background: rgba(201,64,96,0.08); color: var(--coral-dark);
  border: 1px solid rgba(201,64,96,0.22); }}

/* Filter chip */
.fchip {{ display: inline-flex; align-items: center; gap: 6px;
  padding: 5px 10px; border-radius: 999px; background: #fff;
  border: 1px solid var(--line); font-size: 12.5px; }}
.fchip .k {{ color: var(--t2); }}
.fchip .v {{ color: var(--t1); font-weight: 600; }}

/* Stars */
.stars {{ color: {C4_AMBER}; letter-spacing: 1px; }}
.stars .off {{ color: #E5E5E2; }}

/* low-N */
.lown {{ display: inline-flex; align-items: center; gap: 4px;
  background: rgba(245,158,11,0.14); color: #92400E;
  font-size: 10.5px; font-weight: 600; padding: 2px 6px; border-radius: 4px;
  font-family: var(--mono); }}

/* Featured quote cards */
.quote-card {{ padding: 16px; border-radius: 10px;
  font-size: 13px; line-height: 1.5; height: 100%; }}
.quote-card.pos {{ background: rgba(20,184,166,0.06); border: 1px solid rgba(20,184,166,0.18); }}
.quote-card.neu {{ background: rgba(245,158,11,0.06); border: 1px solid rgba(245,158,11,0.18); }}
.quote-card.neg {{ background: rgba(201,64,96,0.05); border: 1px solid rgba(201,64,96,0.18); }}
.quote-card .who {{ font-size: 11.5px; color: var(--t2); margin-top: 12px; }}

.mono {{ font-family: var(--mono); font-feature-settings: 'tnum' on, 'lnum' on; }}
.caps {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.12em;
  font-weight: 600; color: var(--t2); }}

/* Streamlit dataframe tweaks */
[data-testid="stDataFrame"] {{ background: #fff; border-radius: 10px; }}

/* Selectbox compaction */
[data-testid="stSelectbox"] > div > div {{ background: #fff; border-color: var(--line); }}

/* Expander look */
.streamlit-expanderHeader {{ background: #fff; border-radius: 10px; }}
</style>
        """,
        unsafe_allow_html=True,
    )
