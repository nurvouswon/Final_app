import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from pybaseball import statcast
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# -------------- Connect & Set Schema --------------
@st.experimental_singleton
def get_snowflake_conn():
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],  # set schema here, casing doesn't matter here
        autocommit=False,  # We'll manually commit DDL
    )
    with conn.cursor() as cur:
        cur.execute(f"USE SCHEMA {st.secrets['snowflake']['schema'].upper()}")
    return conn

conn = get_snowflake_conn()

# === Utility functions ===

def dedup_columns(df):
    return df.loc[:, ~df.columns.duplicated()]

def downcast_numeric(df):
    for col in df.select_dtypes(include=['float']):
        df[col] = pd.to_numeric(df[col], downcast='float')
    for col in df.select_dtypes(include=['int']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    return df

def parse_custom_weather_string_v2(s):
    if pd.isna(s):
        return pd.Series([np.nan]*7, index=['temp','wind_vector','wind_field_dir','wind_mph','humidity','condition','wind_dir_string'])
    s = str(s)
    temp_match = re.search(r'(\d{2,3})\s*[OI°]?\s', s)
    temp = int(temp_match.group(1)) if temp_match else np.nan
    wind_vector_match = re.search(r'\d{2,3}\s*([OI])\s', s)
    wind_vector = wind_vector_match.group(1) if wind_vector_match else np.nan
    wind_field_dir_match = re.search(r'\s([A-Z]{2})\s*\d', s)
    wind_field_dir = wind_field_dir_match.group(1) if wind_field_dir_match else np.nan
    mph = re.search(r'(\d{1,3})\s*-\s*(\d{1,3})', s)
    if mph:
        wind_mph = (int(mph.group(1)) + int(mph.group(2))) / 2
    else:
        mph = re.search(r'([1-9][0-9]?)\s*(?:mph)?', s)
        wind_mph = int(mph.group(1)) if mph else np.nan
    humidity_match = re.search(r'(\d{1,3})%', s)
    humidity = int(humidity_match.group(1)) if humidity_match else np.nan
    condition = "outdoor" if "outdoor" in s.lower() else ("indoor" if "indoor" in s.lower() else np.nan)
    wind_dir_string = f"{wind_vector} {wind_field_dir}".strip()
    return pd.Series([temp, wind_vector, wind_field_dir, wind_mph, humidity, condition, wind_dir_string],
                     index=['temp','wind_vector','wind_field_dir','wind_mph','humidity','condition','wind_dir_string'])

# === Streamlit UI and logic ===

st.title("MLB Data Pipeline: Fetch Statcast & Upload to Snowflake")

# --- Step 1: Fetch Raw Statcast Event Data ---

st.header("1. Fetch Raw Statcast Event Data")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", datetime.today() - timedelta(days=7))
with col2:
    end_date = st.date_input("End Date", datetime.today())

if start_date > end_date:
    st.error("End date must be after start date.")
    st.stop()

if st.button("Fetch Event Data"):
    with st.spinner(f"Fetching Statcast data from {start_date} to {end_date}..."):
        try:
            df_events = statcast(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
            if df_events.empty:
                st.warning("No data fetched for this date range.")
                st.stop()
        except Exception as e:
            st.error(f"Error fetching Statcast data: {e}")
            st.stop()

    st.success(f"Fetched {len(df_events):,} rows of raw event data.")

    df_events.columns = [c.strip().lower().replace(" ", "_") for c in df_events.columns]
    df_events['game_date'] = pd.to_datetime(df_events['game_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    st.dataframe(df_events.head(20))

    csv_bytes = df_events.to_csv(index=False).encode('utf-8')
    st.download_button("Download Raw Event Data CSV", csv_bytes, file_name="statcast_raw_event_data.csv")

    if 'df_events' in locals() and st.button("Upload Event Data to Snowflake"):
        with st.spinner("Uploading event data to Snowflake..."):
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE event_level_data")
                # Snowflake likes uppercase columns
                df_events.columns = [c.upper() for c in df_events.columns]
                success, nchunks, nrows, _ = write_pandas(conn, df_events, 'EVENT_LEVEL_DATA')
                if success:
                    st.success(f"Uploaded {nrows:,} rows of event data to Snowflake.")
                else:
                    st.error("Upload failed.")
            except Exception as e:
                st.error(f"Upload error: {e}")

# --- Step 2: Upload Matchups CSV ---

st.header("2. Upload Matchups (Daily Lineups)")

matchups_file = st.file_uploader("Upload Matchups CSV", type="csv", key="matchups_upload")

if matchups_file:
    try:
        df_matchups = pd.read_csv(matchups_file)
    except Exception as e:
        st.error(f"Error reading matchup CSV: {e}")
        df_matchups = None

    if df_matchups is not None:
        # Minimal preprocessing for memory efficiency
        df_matchups = dedup_columns(df_matchups)
        df_matchups = downcast_numeric(df_matchups)

        # Parse weather_str column if present
        if 'weather_str' in df_matchups.columns:
            weather_parsed = df_matchups['weather_str'].apply(parse_custom_weather_string_v2)
            df_matchups = pd.concat([df_matchups, weather_parsed], axis=1)

        st.dataframe(df_matchups.head(20))

        if df_matchups is not None and st.button("Upload Matchups to Snowflake"):
            with st.spinner("Uploading matchup data to Snowflake..."):
                try:
                    with conn.cursor() as cur:
                        cur.execute("TRUNCATE TABLE matchups")
                    df_matchups.columns = [c.upper() for c in df_matchups.columns]
                    success, nchunks, nrows, _ = write_pandas(conn, df_matchups, 'MATCHUPS')
                    if success:
                        st.success(f"Uploaded {nrows:,} rows to matchups table.")
                    else:
                        st.error("Matchup upload failed.")
                except Exception as e:
                    st.error(f"Upload error: {e}")

# --- Step 3: Download Enriched Features ---

st.header("3. Download Enriched Features")

with st.form("enriched_feature_form"):
    feature_date = st.date_input("Select Game Date", datetime.today())
    submitted = st.form_submit_button("Fetch Enriched Features")

if submitted:
    query = f"""
        SELECT * FROM today_features WHERE game_date = '{feature_date.strftime('%Y-%m-%d')}'
    """
    try:
        df_features = pd.read_sql(query, conn)
        st.write(f"Downloaded {len(df_features):,} rows of features for {feature_date}")
        st.dataframe(df_features.head(20))
        csv_bytes = df_features.to_csv(index=False).encode('utf-8')
        st.download_button("Download Features CSV", csv_bytes, file_name=f"today_features_{feature_date}.csv")
    except Exception as e:
        st.error(f"Error fetching features: {e}")
