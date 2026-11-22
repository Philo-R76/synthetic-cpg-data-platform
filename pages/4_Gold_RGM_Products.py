import plotly.express as px
import streamlit as st

from src.dashboard_data import (
    database_ready,
    get_categories,
    get_contract_results,
    get_gold_performance,
    get_product_dimension_history,
    get_tenants,
)


st.set_page_config(page_title="Gold RGM Products", layout="wide")
st.title("Gold RGM Product Performance")
st.caption("Monthly POS, shipment, and promotion summaries by tenant and product.")
st.info(
    "Currency values are kept within the selected tenant's currency. Promotion "
    "spend and discounts are descriptive; this prototype does not estimate causal lift."
)

if not database_ready():
    st.warning("Build the demo database first: `python -m src.run_pipeline`.")
    st.stop()

filters = st.columns(2)
tenant_id = filters[0].selectbox("Tenant", get_tenants())
category = filters[1].selectbox("Category", ["All categories", *get_categories()])
category_filter = None if category == "All categories" else category

performance = get_gold_performance(tenant_id, category_filter)
if performance.empty:
    st.warning("No Gold performance rows are available for this selection.")
    st.stop()

currency_code = performance["currency_code"].dropna().unique()
currency_label = currency_code[0] if len(currency_code) == 1 else "local currency"
totals = performance[
    ["pos_units", "net_sales", "shipped_each_units", "promotion_count", "promo_spend"]
].sum()
kpis = st.columns(5)
kpis[0].metric("POS units", f'{totals["pos_units"]:,.0f}')
kpis[1].metric("Net sales", f'{currency_label} {totals["net_sales"]:,.2f}')
kpis[2].metric("Shipped eaches", f'{totals["shipped_each_units"]:,.0f}')
kpis[3].metric("Promotions", int(totals["promotion_count"]))
kpis[4].metric("Promotion spend", f'{currency_label} {totals["promo_spend"]:,.2f}')

monthly = (
    performance.groupby("sales_month", as_index=False)
    .agg(net_sales=("net_sales", "sum"), pos_units=("pos_units", "sum"))
    .sort_values("sales_month")
)
st.subheader("Monthly POS sales")
sales_chart = px.line(
    monthly,
    x="sales_month",
    y="net_sales",
    markers=True,
    labels={"sales_month": "Month", "net_sales": f"Net sales ({currency_label})"},
)
st.plotly_chart(sales_chart, width="stretch")

chart_col, table_col = st.columns([1, 1])
with chart_col:
    st.subheader("Shipment to POS units")
    reconciliation = performance[
        performance["shipment_to_pos_each_ratio"].notna()
    ]
    if reconciliation.empty:
        st.info("No comparable shipment-each/POS-unit pairs for these filters.")
    else:
        shipment_chart = px.scatter(
            reconciliation,
            x="pos_units",
            y="shipped_each_units",
            color="category",
            hover_data=["sales_month", "product_id", "shipment_to_pos_each_ratio"],
            labels={"pos_units": "POS units", "shipped_each_units": "Shipped eaches"},
        )
        st.plotly_chart(shipment_chart, width="stretch")
        st.caption(
            "Only shipment quantities labeled each/EA are compared with POS units. "
            "Case quantities are reported separately."
        )

with table_col:
    st.subheader("Data contract checks")
    contracts = get_contract_results(tenant_id)
    st.dataframe(
        contracts,
        hide_index=True,
        width="stretch",
        column_config={
            "pass_rate": st.column_config.NumberColumn(format="%.1%"),
        },
    )
    st.caption(
        "Injected malformed values are expected to fail parsing checks; review "
        "queue records are warnings rather than silent mapping successes."
    )

st.subheader("Gold monthly product table")
st.dataframe(
    performance,
    hide_index=True,
    width="stretch",
    column_config={
        "avg_discount_pct": st.column_config.NumberColumn(format="%.1%"),
        "shipment_to_pos_each_ratio": st.column_config.NumberColumn(format="%.2f"),
    },
)

with st.expander("Product dimension history (SCD Type 2)"):
    st.dataframe(
        get_product_dimension_history(),
        hide_index=True,
        width="stretch",
    )
