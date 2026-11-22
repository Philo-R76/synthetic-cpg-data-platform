import plotly.express as px
import streamlit as st

from src.dashboard_data import (
    database_ready,
    get_mapping_metrics,
    get_mapping_method_counts,
    get_mapping_release_metrics,
    get_tenants,
)


st.set_page_config(page_title="Mapping Benchmark", layout="wide")
st.title("Ontology Mapping Benchmark")
st.caption("Mapping quality measured across ontology and enrichment releases.")
st.info("The scores below are synthetic benchmark results, not production accuracy.")

if not database_ready():
    st.warning("Build the demo database first: `python -m src.run_pipeline`.")
    st.stop()

tenants = ["All tenants", *get_tenants()]
selected_tenant = st.selectbox("Tenant", tenants)
tenant_filter = None if selected_tenant == "All tenants" else selected_tenant
metrics = get_mapping_metrics(tenant_filter)

if metrics.empty:
    st.warning("No mapping benchmark records are available for this selection.")
    st.stop()

totals = metrics[
    ["true_positives", "false_positives", "false_negatives", "review_records"]
].sum()
precision = (
    totals["true_positives"]
    / (totals["true_positives"] + totals["false_positives"])
    if totals["true_positives"] + totals["false_positives"]
    else 0.0
)
recall = (
    totals["true_positives"]
    / (totals["true_positives"] + totals["false_negatives"])
    if totals["true_positives"] + totals["false_negatives"]
    else 0.0
)
f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

kpis = st.columns(4)
kpis[0].metric("Precision", f"{precision:.1%}")
kpis[1].metric("Recall", f"{recall:.1%}")
kpis[2].metric("F1", f"{f1:.1%}")
kpis[3].metric("Review records", int(totals["review_records"]))

chart_col, table_col = st.columns([1, 1])
with chart_col:
    st.subheader("Scores by feed")
    score_data = metrics.melt(
        id_vars=["tenant_id", "source_system"],
        value_vars=["precision", "recall", "f1"],
        var_name="metric",
        value_name="score",
    )
    score_data["tenant_feed"] = (
        score_data["tenant_id"] + " - " + score_data["source_system"]
    )
    chart = px.bar(
        score_data,
        x="tenant_feed",
        y="score",
        color="metric",
        barmode="group",
        range_y=[0, 1],
        labels={"tenant_feed": "Tenant · feed", "score": "Score"},
    )
    st.plotly_chart(chart, width="stretch")

with table_col:
    st.subheader("Confusion counts and quality metrics")
    st.dataframe(
        metrics,
        hide_index=True,
        width="stretch",
        column_config={
            "precision": st.column_config.NumberColumn(format="%.1%"),
            "recall": st.column_config.NumberColumn(format="%.1%"),
            "f1": st.column_config.NumberColumn(format="%.1%"),
        },
    )

st.subheader("Enrichment release progression")
releases = get_mapping_release_metrics(tenant_filter)
release_chart = px.line(
    releases.melt(
        id_vars="release_version",
        value_vars=["precision", "recall", "f1"],
        var_name="metric",
        value_name="score",
    ),
    x="release_version",
    y="score",
    color="metric",
    markers=True,
    range_y=[0, 1],
    labels={"release_version": "Mapping release", "score": "Score"},
)
st.plotly_chart(release_chart, width="stretch")
st.dataframe(
    get_mapping_method_counts(tenant_filter),
    hide_index=True,
    width="stretch",
    column_config={
        "average_confidence": st.column_config.NumberColumn(format="%.1%"),
    },
)

st.markdown(
    """
    **Metric definitions:** A true positive is a mapped product ID matching its
    synthetic label. A wrong non-null prediction counts as both a false positive
    and false negative; an unresolved record is a false negative and enters the
    review queue. This makes the benchmark's treatment of misses explicit.
    """
)
