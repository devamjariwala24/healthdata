"""
main.py

Orchestrator script:
- Ensures required packages are present
- Loads environment variables
- Connects to Snowflake
- Executes setup.sql (idempotent)
- Loads data.csv into CDI_DATA (if present)
- Launches the Streamlit dashboard (dashboard.py)
"""

import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

# -------------------------
# Ensure required packages
# -------------------------
required_pip_packages = [
    "python-dotenv",
    "pandas",
    "plotly",
    "streamlit",
    "snowflake-connector-python[pandas]"
]

def ensure_packages(packages):
    for pkg in packages:
        name = pkg.split("[")[0].split("==")[0]
        try:
            __import__(name)
        except ImportError:
            print(f"[INFO] Installing {pkg} ...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

ensure_packages(required_pip_packages)

# -------------------------
# Load environment variables
# -------------------------
load_dotenv()  # Reads .env in project root

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "HEALTH_GOV_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "CDC_SCHEMA")

if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
    print("ERROR: Missing Snowflake credentials in .env. Copy .env.example -> .env and fill values.")
    sys.exit(1)

# -------------------------
# Connect to Snowflake
# -------------------------
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

print("[INFO] Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
cur = conn.cursor()
print("[INFO] Connected.")

# -------------------------
# Execute setup.sql idempotently
# -------------------------
sql_path = Path(__file__).parent / "setup.sql"
if not sql_path.exists():
    print(f"ERROR: setup.sql not found at {sql_path}")
    sys.exit(1)

print(f"[INFO] Executing setup.sql -> {sql_path}")
sql_text = sql_path.read_text()
for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
    try:
        cur.execute(stmt)
    except Exception as e:
        print(f"[WARN] SQL execution error (continuing): {e}")

print("[INFO] setup.sql executed (errors printed as warnings).")

# -------------------------
# Load CSV data (if present)
# -------------------------
csv_path = Path(__file__).parent / "data.csv"
if csv_path.exists():
    print(f"[INFO] Loading CSV into Snowflake from {csv_path} ...")
    df = pd.read_csv(csv_path)
    print(f"[DEBUG] CSV columns: {df.columns.tolist()}")

    # Uppercase columns to match Snowflake table
    df.columns = [c.upper() for c in df.columns]
    success, nchunks, nrows, _ = write_pandas(conn, df, 'CDI_DATA')
    print(f"[INFO] Loaded {nrows} rows into CDI_DATA (success={success}).")
else:
    print("[INFO] No data.csv found; skipping data load.")

# -------------------------
# Launch Streamlit dashboard
# -------------------------
print("[INFO] Launching Streamlit dashboard...")
subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard.py"])

# -------------------------
# Close Snowflake connection
# -------------------------
cur.close()
conn.close()
print("[INFO] Finished.")
