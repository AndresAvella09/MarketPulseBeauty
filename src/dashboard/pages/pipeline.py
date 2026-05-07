"""Page 8 — Pipeline Health, on real Postgres data."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import charts, components as ui, data_loader as dl


def render(data: dl.DashboardData) -> None:
    runs = dl.load_pipeline_runs()

    ui.page_header(
        kicker="Pipeline",
        title="Last-run health",
        sub="Airflow gold layer · gold.pipeline_runs",
    )

    if runs.empty:
        st.warning("gold.pipeline_runs is empty.")
        return

    last = runs.iloc[0]
    success_rate = (runs["status"] == "success").mean() * 100
    fails = int((runs["status"] != "success").sum())

    duration = last.get("duration_seconds")
    if pd.notna(duration):
        mins, secs = divmod(int(duration), 60)
        dur_str = f"{mins}m {secs:02d}s"
    else:
        dur_str = "—"

    finished = last.get("finished_at")
    finished_str = finished.strftime("%Y-%m-%d · %H:%M UTC") if pd.notna(finished) else "n/a"

    cols = st.columns(3)
    with cols[0]:
        st.markdown(
            ui.kpi_card("LAST RUN · DURATION", dur_str,
                        meta=f"{last['dag_name']} · {finished_str}"),
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            ui.kpi_card("ROWS WRITTEN (last)",
                        f"{int(last['rows_written'] or 0):,}",
                        meta=f"status: {last['status']}"),
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            ui.kpi_card(f"{len(runs)}-RUN SUCCESS RATE",
                        f"{success_rate:.1f}%",
                        meta=f"{fails} failures"),
            unsafe_allow_html=True,
        )

    st.markdown("&nbsp;", unsafe_allow_html=True)

    with ui.card("Run history", sub="duration in minutes · color = status"):
        runs_for_chart = runs.assign(duration=runs["duration_seconds"].fillna(0) / 60).iloc[::-1]
        runs_list = [{"status": r.status, "duration": float(r.duration)}
                     for r in runs_for_chart.itertuples()]
        st.plotly_chart(charts.pipeline_runs(runs_list, height=240),
                        use_container_width=True, key="pipe_runs")

    st.markdown("&nbsp;", unsafe_allow_html=True)

    with ui.card("Recent runs", sub="all DAGs · most recent first"):
        show = runs.copy()
        show["duration"] = (show["duration_seconds"] / 60).round(2).astype(str) + " min"
        show = show[["started_at", "dag_name", "status", "rows_written", "duration", "run_id"]]
        st.dataframe(show, use_container_width=True, hide_index=True)
