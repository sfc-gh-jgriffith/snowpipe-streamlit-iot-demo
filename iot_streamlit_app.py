import os
import streamlit as st
from snowflake.snowpark import Session  #upm package(snowflake-connector-python==2.7.0)
import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col
import pandas as pd
from time import sleep

@st.experimental_singleton
def init_connection():
    connection_parameters = {
        "user":os.getenv("SFUSER"),
        "password":os.getenv("SFPASSWORD"),
        "account":os.getenv("ACCOUNT"),
        "role":os.getenv("ROLE"),
        "warehouse":"LARGE",
        "database":"IOT_DEMO",
        "schema":"PUBLIC"
    }
    return Session.builder.configs(connection_parameters).create()

session = init_connection()

st.subheader("Streaming IOT Analytics")
st.write("Simulated IOT data dropped into S3 and imported to Snowflake automatically by Snowpipe.")

records_by_minute = st.empty()

current_n_rows = st.empty()
max_timestamp_display = st.empty()

controls_1, controls_2 = st.columns(2)
with controls_1:
    hours_to_display = st.number_input("Hours to Display", min_value=1, max_value=24, value=12)
with controls_2:
    refresh_frequency = st.number_input("Refresh Frequency (Seconds)", min_value=5, max_value=60, value=15)


charts_container = st.container()
col1, col2, col3 = charts_container.columns(3)

with col1:
    st.caption("Temp")
    amps_chart = st.empty()
with col2:
    st.caption("Speed")
    t_chart = st.empty()
with col3:
    st.caption("Vibration")
    wind_chart = st.empty()

iot = session.table("iot_view")


while (True):
    df_records_by_minute = (iot.group_by(f.date_trunc('minute', 'TIMESTAMP')).count().to_pandas())
    df_records_by_minute.columns = ['MINUTE','RECORDS']
    records_by_minute.bar_chart(df_records_by_minute, x="MINUTE", y="RECORDS", height=200)
    current_n_rows.write(f"Number of Rows in Table: {iot.count():,.0f}")
    max_timestamp = iot.agg(f.max(col("TIMESTAMP"))).to_pandas().iloc[0]['MAX(TIMESTAMP)']
    timestamp_agg = (iot.filter(col("TIMESTAMP") >= f.dateadd('hour', f.lit(-hours_to_display), f.current_timestamp())).group_by('TIMESTAMP')
                        .agg(f.mean("TEMP"), f.mean("SPEED"), f.mean("VIBRATION"),
                        f.approx_percentile("TEMP", .25), f.approx_percentile("SPEED", .25), f.approx_percentile("VIBRATION", .25),
                        f.approx_percentile("TEMP", .75), f.approx_percentile("SPEED", .75), f.approx_percentile("VIBRATION", .75),
                        f.count("TIMESTAMP"))
                        .to_pandas())
                        
    timestamp_agg[timestamp_agg.columns[1:]] = timestamp_agg[timestamp_agg.columns[1:]].astype("float")
    amps_chart.area_chart(timestamp_agg, x="TIMESTAMP", y=[c for c in timestamp_agg.columns if "TEMP" in c])
    t_chart.line_chart(timestamp_agg, x="TIMESTAMP", y=[c for c in timestamp_agg.columns if "SPEED" in c])
    wind_chart.line_chart(timestamp_agg, x="TIMESTAMP", y=[c for c in timestamp_agg.columns if "VIBRATION" in c])
    
    
    
    max_timestamp_display.write(f"Most Recent Timestamp: {max_timestamp}")
    
    sleep(refresh_frequency)







