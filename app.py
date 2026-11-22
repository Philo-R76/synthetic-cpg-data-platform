from __future__ import annotations

import streamlit as st

from src.dashboard_data import DEFAULT_DB_PATH, database_ready, get_overview


st.set_page_config(
    page_title="ADP CPG Data Platform",
    layout="wide",
)

st.title("ADP | CPG Data Platform Demo")
st.caption("Synthetic multi-tenant ingestion and product-mapping benchmark")
st.info(
    "All records and benchmark outcomes are synthetic. Metrics describe this "
    "demo dataset only; they are not production performance claims."
)

if not database_ready():
    st.warning("The demo database is missing or incomplete.")
    st.code("python -m src.generate_cpg_data\npython -m src.run_pipeline")
    st.stop()

overview = get_overview()
first_row = st.columns(4)
first_row[0].metric("Tenants", int(overview["tenants"]))
first_row[1].metric("Raw feed records", int(overview["labeled_records"]))
first_row[2].metric("Mapped records", int(overview["mapped_records"]))
first_row[3].metric("Human review queue", int(overview["review_records"]))

second_row = st.columns(3)
second_row[0].metric("Mapping coverage", f'{overview["coverage"]:.1%}')
second_row[1].metric("Benchmark precision", f'{overview["precision"]:.1%}')
second_row[2].metric("Benchmark recall", f'{overview["recall"]:.1%}')

st.subheader("Bronze feed volume")
st.bar_chart(overview["feeds"].set_index("feed"), y="records", color="#3B82F6")

st.subheader("Platform walkthrough")
st.markdown(
    """
    - **Mapping Benchmark** compares matcher releases against labeled synthetic
      ground truth, broken down by tenant and source feed.
    - **Human Review Queue** surfaces records the deterministic rules could not
      resolve, including the expected synthetic answer for inspection.
    - **DuckDB** keeps the demo local and makes every metric reproducible.
    """
)
st.caption(f"Data source: `{DEFAULT_DB_PATH}`")
