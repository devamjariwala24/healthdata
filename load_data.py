import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Snowflake connection
conn = snowflake.connector.connect(
    user='DEVAMJARIWALA',
    password='Snowflake123!@#',
    account='VYXYLSV-TLC53551',
    warehouse='COMPUTE_WH',
    database='HEALTH_GOV_DB',
    schema='CDC_SCHEMA'
)

# Path to your CSV
csv_file = "data.csv"

# Columns to load
usecols = [
    'YearStart','YearEnd','LocationAbbr','LocationDesc','Topic','Question','Response',
    'DataValue','DataValueUnit','StratificationCategory1','Stratification1','Geolocation',
    'DataValueFootnote','LowConfidenceLimit','HighConfidenceLimit'
]

# Load CSV in chunks
chunksize = 5000
for chunk in pd.read_csv(csv_file, chunksize=chunksize, usecols=usecols):
    # Replace NaN with None
    chunk = chunk.where(pd.notnull(chunk), None)
    
    # Uppercase columns for Snowflake
    chunk.columns = [c.upper() for c in chunk.columns]
    
    # Bulk insert
    success, nchunks, nrows, _ = write_pandas(conn, chunk, 'CDI_DATA')
    print(f"Chunk inserted: {nrows} rows, success: {success}")

conn.close()
print("All data loaded successfully!")
