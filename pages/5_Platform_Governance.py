import plotly.express as px
import streamlit as st

from src.dashboard_data import (
    database_ready,
    get_feed_ingestion_events,
    get_pipeline_runs,
    get_tenant_run_metrics,
)


st.set_page_config(page_title="Platform Governance", layout="wide")
st.title("Platform Observability and Tenant Economics")
st.caption("Measured local pipeline runs with explicitly modeled cost assumptions.")
st.warning(
    "Compute and storage amounts are illustrative estimates from config/platform.json, "
    "not cloud bills. Onboarding hours are scenario assumptions, not measured outcomes."
)

if not database_ready():
    st.warning("Build the demo database first: `python -m src.run_pipeline`.")
    st.stop()

runs = get_pipeline_runs()
if runs.empty:
    st.info("No pipeline runs are recorded yet. Run `python -m src.run_pipeline`.")
    st.stop()

latest = runs.iloc[0]
total_runs = len(runs)
failed_runs = int((runs["status"] == "FAILED").sum())
failure_rate = failed_runs / total_runs
successful = runs[runs["status"] == "SUCCESS"]
retry_rate = (
    runs["retry_count"].sum() / total_runs
    if total_runs
    else 0
)

kpis = st.columns(4)
kpis[0].metric("Latest run", str(latest["status"]))
kpis[1].metric("Latest duration", f'{latest["duration_seconds"]:.2f} sec')
kpis[2].metric("Recorded run failure rate", f"{failure_rate:.1%}")
kpis[3].metric("Retries recorded", int(runs["retry_count"].sum()))
st.caption(
    f"{total_runs} recent runs; retry rate {retry_rate:.1%}. The local pipeline "
    "currently has no automatic retry policy."
)

run_chart = px.scatter(
    runs,
    x="started_at",
    y="duration_seconds",
    color="status",
    hover_data=["run_id", "input_records", "error_type"],
    labels={"started_at": "Run started", "duration_seconds": "Wall time (seconds)"},
    title="Observed pipeline duration by run",
)
st.plotly_chart(run_chart, width="stretch")

if latest["status"] == "SUCCESS":
    st.subheader("Incremental feed ingestion")
    feed_events = get_feed_ingestion_events(str(latest["run_id"]))
    st.dataframe(
        feed_events,
        hide_index=True,
        width="stretch",
    )

    tenant_metrics = get_tenant_run_metrics(str(latest["run_id"]))
    if not tenant_metrics.empty:
        tenant_metrics["compute_usd_per_million_rows"] = (
            tenant_metrics["estimated_compute_usd"]
            / tenant_metrics["processed_rows"].replace(0, float("nan"))
            * 1_000_000
        )
        tenant_metrics["onboarding_hours_saved_model"] = (
            tenant_metrics["onboarding_hours_before_config"]
            - tenant_metrics["onboarding_hours_after_config"]
        )
        cost_col, volume_col = st.columns(2)
        with cost_col:
            st.subheader("Estimated cost per tenant")
            cost_chart = px.bar(
                tenant_metrics,
                x="tenant_id",
                y="compute_usd_per_million_rows",
                labels={
                    "tenant_id": "Tenant",
                    "compute_usd_per_million_rows": "Estimated USD per 1M rows",
                },
            )
            st.plotly_chart(cost_chart, width="stretch")
        with volume_col:
            st.subheader("Input volume by tenant")
            volume_chart = px.bar(
                tenant_metrics,
                x="tenant_id",
                y="allocated_input_bytes",
                labels={
                    "tenant_id": "Tenant",
                    "allocated_input_bytes": "Allocated raw input bytes",
                },
            )
            st.plotly_chart(volume_chart, width="stretch")

        st.subheader("Latest-run tenant allocation")
        st.dataframe(
            tenant_metrics,
            hide_index=True,
            width="stretch",
            column_config={
                "estimated_compute_usd": st.column_config.NumberColumn(format="$%.6f"),
                "estimated_storage_usd_per_month": st.column_config.NumberColumn(
                    format="$%.6f"
                ),
                "compute_usd_per_million_rows": st.column_config.NumberColumn(
                    format="$%.4f"
                ),
            },
        )
        st.caption(
            "Tenant bytes are allocated from feed file sizes in proportion to each "
            "tenant's feed row counts; they are not measured object-store usage."
        )

        st.subheader("Onboarding scenario")
        onboarding = tenant_metrics[
            [
                "tenant_id",
                "onboarding_hours_before_config",
                "onboarding_hours_after_config",
                "onboarding_hours_saved_model",
            ]
        ].melt(
            id_vars="tenant_id",
            var_name="scenario",
            value_name="hours",
        )
        onboarding["scenario"] = onboarding["scenario"].map(
            {
                "onboarding_hours_before_config": "Before config framework",
                "onboarding_hours_after_config": "With config framework",
                "onboarding_hours_saved_model": "Modeled hours saved",
            }
        )
        onboarding_chart = px.bar(
            onboarding,
            x="tenant_id",
            y="hours",
            color="scenario",
            barmode="group",
            labels={"tenant_id": "Tenant", "hours": "Estimated hours"},
        )
        st.plotly_chart(onboarding_chart, width="stretch")

st.subheader("Recent pipeline run ledger")
st.dataframe(
    runs,
    hide_index=True,
    width="stretch",
    column_config={
        "estimated_compute_usd": st.column_config.NumberColumn(format="$%.6f"),
        "duration_seconds": st.column_config.NumberColumn(format="%.2f sec"),
    },
)

with st.expander("Model assumptions and trade-offs"):
    st.markdown(
        """
        - Wall-clock duration and raw feed file sizes are measured locally for each run.
        - Tenant input bytes are allocated by row share within each feed; this is an
          approximation for variable-width records.
        - Compute and storage rates and onboarding hours come from the local JSON
          configuration and are illustrative only.
        - The pipeline is fail-fast and records zero retries; a successful run does
          not imply production SLA or fault-tolerance behavior.
        """
    )
