import streamlit as st

from src.dashboard_data import (
    database_ready,
    get_review_records,
    get_tenants,
)


st.set_page_config(page_title="Human Review Queue", layout="wide")
st.title("Human-in-the-Loop Review Queue")
st.caption("Unresolved synthetic product identities requiring analyst review.")
st.info("This page is an inspection queue; it does not persist analyst decisions.")

if not database_ready():
    st.warning("Build the demo database first: `python -m src.run_pipeline`.")
    st.stop()

filters = st.columns(3)
tenant_options = ["All tenants", *get_tenants()]
tenant_choice = filters[0].selectbox("Tenant", tenant_options)
feed_options = [
    "All feeds",
    "retailer_pos",
    "warehouse",
    "trade_promotion",
    "syndicated",
]
feed_choice = filters[1].selectbox("Feed", feed_options)
row_limit = filters[2].selectbox("Maximum rows", [25, 50, 100, 250], index=2)

queue = get_review_records(
    tenant_id=None if tenant_choice == "All tenants" else tenant_choice,
    source_system=None if feed_choice == "All feeds" else feed_choice,
    limit=row_limit,
)
st.metric("Records in this view", len(queue))
if queue.empty:
    st.success("No records need review for these filters.")
else:
    st.dataframe(
        queue,
        hide_index=True,
        width="stretch",
        column_config={
            "confidence": st.column_config.NumberColumn(format="%.0%"),
            "expected_product_id": st.column_config.TextColumn(
                "Expected product (synthetic label)"
            ),
        },
    )

st.caption(
    "Expected product fields are visible only because this is a labeled synthetic "
    "benchmark. A real operational review queue would not expose ground truth."
)
