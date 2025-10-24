"""
dashboard.py

Streamlit dashboard for Health Data Governance
This module expects Snowflake credentials available in environment variables (or via .env loaded by main).
It fetches the masked view CDI_DATA_MASKED and displays summary + charts.
"""

from dotenv import load_dotenv
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector

# load .env if present
load_dotenv()

# Page config
st.set_page_config(page_title="Health Data Governance Dashboard", layout="wide")
st.markdown(
    """
    <style>
    .stApp {
        background-color: #0E1117;
        color: #FFFFFF;
    }
    .stDataFrame table { color: #FFFFFF; }
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Health Data Governance Dashboard")

# Read Snowflake credentials from env
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "HEALTH_GOV_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "CDC_SCHEMA")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

@st.cache_data(ttl=120)
def load_masked_data(limit: int = 1000):
    query = f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CDI_DATA_MASKED LIMIT {limit}"
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(ttl=120)
def load_tags():
    query = f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COLUMN_TAGS"
    return pd.read_sql(query, conn)

# Load data
df = load_masked_data(1000)
tags = load_tags()

# Summary metrics
st.subheader("Summary Metrics")
c1, c2, c3 = st.columns(3)
c1.metric("Rows (sample)", len(df))
c2.metric("Unique locations", int(df['LOCATIONABBR'].nunique()) if 'LOCATIONABBR' in df.columns else 0)
c3.metric("Unique topics", int(df['TOPIC'].nunique()) if 'TOPIC' in df.columns else 0)

# Filters
st.subheader("Filters")
topics = ["All"] + sorted(df['TOPIC'].dropna().unique().tolist()) if 'TOPIC' in df.columns else ["All"]
topic = st.selectbox("Topic", topics)
locations = ["All"] + sorted(df['LOCATIONABBR'].dropna().unique().tolist()) if 'LOCATIONABBR' in df.columns else ["All"]
location = st.selectbox("Location", locations)

filtered = df.copy()
if topic != "All":
    filtered = filtered[filtered['TOPIC'] == topic]
if location != "All":
    filtered = filtered[filtered['LOCATIONABBR'] == location]

st.write("### Data Preview")
st.dataframe(filtered.head(50), use_container_width=True)

# Charts
st.write("### Visualizations")

if 'TOPIC' in df.columns and not df.empty:
    topic_counts = df['TOPIC'].value_counts().reset_index()
    topic_counts.columns = ['TOPIC', 'COUNT']
    fig = px.bar(topic_counts, x='TOPIC', y='COUNT', title='Records per Topic', template='plotly_dark')
    st.plotly_chart(fig, use_container_width=True)

if 'DATAVALUE' in df.columns and not df.empty:
    fig2 = px.histogram(filtered, x='DATAVALUE', nbins=40, title='DATAVALUE distribution', template='plotly_dark')
    st.plotly_chart(fig2, use_container_width=True)

# Optional geolocation plot (only if GEOLOCATION exists and not masked)
if 'GEOLOCATION' in df.columns and not df['GEOLOCATION'].isna().all():
    geo = df[df['GEOLOCATION'].notnull()].copy()
    # try to extract coordinates if in "lat,lon" or "POINT (lon lat)" format
    def extract_coords(val):
        try:
            s = str(val)
            if s.startswith("POINT"):
                # POINT (lon lat)
                inner = s.split("(", 1)[1].rstrip(")")
                lon, lat = inner.split()
                return float(lat), float(lon)
            if ',' in s:
                lat, lon = s.split(",")
                return float(lat), float(lon)
        except:
            return None, None
    geo[['LAT', 'LON']] = geo['GEOLOCATION'].apply(lambda v: pd.Series(extract_coords(v)))
    geo = geo.dropna(subset=['LAT','LON'])
    if not geo.empty:
        fig3 = px.scatter_mapbox(geo, lat='LAT', lon='LON', hover_name='LOCATIONDESC', hover_data=['TOPIC','DATAVALUE'],
                                 zoom=3, height=400)
        fig3.update_layout(mapbox_style="open-street-map", template='plotly_dark')
        st.plotly_chart(fig3, use_container_width=True)

# Close connection when Streamlit session ends
# (Snowflake connector will be closed by interpreter exit)
