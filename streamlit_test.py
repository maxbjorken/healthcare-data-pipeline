import streamlit as st
import duckdb

st.set_page_config(page_title="Data Quality Monitor", layout="wide")
st.title("Data Quality Monitor for Healthcare Pipeline")

con = duckdb.connect('hapi_fhir.db')

st.subheader("Data Freshness")
freshness_df = con.execute("SELECT MAX(ingested_at) as last_run FROM fct_conditions").df()
st.metric("Last Pipeline Run", str(freshness_df['last_run'][0])[:19])

st.subheader("Integrity Check: Conditions vs Patients")
integrity_df = con.execute("SELECT * FROM qc_diagnosis").df()
st.table(integrity_df)

st.bar_chart(integrity_df.set_index('category')['error_rate_percent'])