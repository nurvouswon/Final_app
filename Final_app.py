import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from pybaseball import statcast
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# === Snowflake Connection ===
@st.cache_resource(show_spinner=False)
def get_snowflake_conn():
    conn = snowflake.connector.connect(
        user=st.secrets["sf_user"],
        password=st.secrets["sf_password"],
        account=st.secrets["sf_account"],
        warehouse=st.secrets["sf_warehouse"],
        database=st.secrets["sf_database"],
        schema=st.secrets["sf_schema"],
        role=st.secrets["sf_role"]
    )
    return conn

conn = get_snowflake_conn()

# === App Title & Description ===
st.title("MLB Data Pipeline: Fetch Statcast & Upload to Snowflake")

# === Step 1: Fetch Event Data ===
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

    # Standardize columns & format dates
    df_events.columns = [c.strip().lower().replace(" ", "_") for c in df_events.columns]
    df_events['game_date'] = pd.to_datetime(df_events['game_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    st.dataframe(df_events.head(20))

    csv_bytes = df_events.to_csv(index=False).encode()
    st.download_button("Download Raw Event Data CSV", csv_bytes, file_name="statcast_raw_event_data.csv")

    if st.button("Upload Event Data to Snowflake"):
        with st.spinner("Uploading event data to Snowflake..."):
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE event_level_data")
                success, nchunks, nrows, _ = write_pandas(conn, df_events, 'event_level_data')
                if success:
                    st.success(f"Uploaded {nrows:,} rows of event data to Snowflake.")
                else:
                    st.error("Upload failed.")
            except Exception as e:
                st.error(f"Upload error: {e}")

# === Step 2: Upload Matchups CSV ===
st.header("2. Upload Matchups (Daily Lineups)")

matchups_file = st.file_uploader("Upload Matchups CSV", type="csv", key="matchups_upload")

if matchups_file:
    try:
        df_matchups = pd.read_csv(matchups_file)
        st.dataframe(df_matchups.head(20))
    except Exception as e:
        st.error(f"Error reading matchup CSV: {e}")
        df_matchups = None

    if df_matchups is not None:
        if st.button("Upload Matchups to Snowflake"):
            with st.spinner("Uploading matchup data to Snowflake..."):
                try:
                    with conn.cursor() as cur:
                        cur.execute("TRUNCATE TABLE matchups")
                    success, nchunks, nrows, _ = write_pandas(conn, df_matchups, 'matchups')
                    if success:
                        st.success(f"Uploaded {nrows:,} rows to matchups table.")
                    else:
                        st.error("Matchup upload failed.")
                except Exception as e:
                    st.error(f"Upload error: {e}")

# === Optional Step 3: Download Enriched Features from Snowflake ===
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
        csv_bytes = df_features.to_csv(index=False).encode()
        st.download_button("Download Features CSV", csv_bytes, file_name=f"today_features_{feature_date}.csv")
    except Exception as e:
        st.error(f"Error fetching features: {e}")
