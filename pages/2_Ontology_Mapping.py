import plotly.express as px
import streamlit as st

from src.dashboard_data import (
    database_ready,
    get_mapping_method_counts,
    get_mapping_records,
    get_ontology_regression_results,
    get_product_catalog,
    get_tenants,
)
from src.ontology import DEFAULT_ONTOLOGY_PATH, load_ontology


st.set_page_config(page_title="Ontology Mapping", layout="wide")
st.title("CPG Ontology and Entity Resolution")
st.caption("Canonical product identity, curated synonyms, and review decisions.")
st.info(
    "Fuzzy matching here is lexical string similarity, not an embedding or LLM. "
    "Ambiguous or low-confidence values stay in the review queue."
)

if not database_ready():
    st.warning("Build the demo database first: `python -m src.run_pipeline`.")
    st.stop()

ontology = load_ontology(DEFAULT_ONTOLOGY_PATH)
catalog = get_product_catalog()
with st.expander("Ontology relationships and mapping releases", expanded=True):
    st.write("Relationships:", " | ".join(ontology["relationships"]))
    st.dataframe(
        catalog,
        hide_index=True,
        width="stretch",
    )
    st.json(ontology["mapping_releases"], expanded=False)

synonym_rows = [
    {"product_id": product_id, "synonym": synonym}
    for product_id, entity in ontology["entities"].items()
    for synonym in entity["synonyms"]
]
st.subheader("Curated synonym dictionary")
st.dataframe(synonym_rows, hide_index=True, width="stretch")

regression = get_ontology_regression_results()
passed = int((regression["status"] == "PASS").sum())
failed = int((regression["status"] == "FAIL").sum())
regression_kpis = st.columns(3)
regression_kpis[0].metric("Regression checks", len(regression))
regression_kpis[1].metric("Passed", passed)
regression_kpis[2].metric("Failed", failed)
st.dataframe(regression, hide_index=True, width="stretch")

st.subheader("Mapping decision inspector")
filters = st.columns(3)
tenant_options = ["All tenants", *get_tenants()]
tenant_choice = filters[0].selectbox("Tenant", tenant_options)
method_options = [
    "All methods",
    "rule_exact",
    "ontology_synonym",
    "fuzzy_lexical",
    "low_conf_hitl",
]
method_choice = filters[1].selectbox("Mapping method", method_options)
row_limit = filters[2].selectbox("Maximum rows", [25, 50, 100, 250], index=2)
selected_tenant = None if tenant_choice == "All tenants" else tenant_choice
method_counts = get_mapping_method_counts(selected_tenant)
method_chart = px.bar(
    method_counts,
    x="mapping_method",
    y="record_count",
    color="mapping_method",
    labels={"mapping_method": "Decision method", "record_count": "Records"},
)
st.plotly_chart(method_chart, width="stretch")
records = get_mapping_records(
    tenant_id=selected_tenant,
    mapping_method=None if method_choice == "All methods" else method_choice,
    limit=row_limit,
)
if records.empty:
    st.info("No mapping records match these filters.")
else:
    st.dataframe(
        records,
        hide_index=True,
        width="stretch",
        column_config={
            "confidence": st.column_config.NumberColumn(format="%.1%"),
        },
    )

st.subheader("Ground-truth release benchmark")
st.markdown(
    """
    Use **Mapping Benchmark** to compare precision, recall, and F1 across the
    configured releases. A release only receives credit when its predicted
    canonical product ID matches the synthetic row-level ground truth.
    """
)
