"""
Load Raw Script - Loads data exactly as extracted (NO modifications)
This is the key ELT principle: preserve raw data
"""

import pandas as pd
import duckdb
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
DB_PATH = os.path.join(DATA_DIR, 'air_quality.duckdb')

def load_raw_data():
    """
    Load raw data WITHOUT any cleaning or transformation
    """
    input_path = os.path.join(RAW_DIR, 'air_quality_extracted.csv')
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Extract file not found: {input_path}. Run extract.py first.")
    
    # Read CSV exactly as is - NO modifications
    df = pd.read_csv(input_path)
    
    print(f"Loading {len(df)} records...")
    print(f"Columns: {list(df.columns)}")
    print(f"NULL values preserved: PM2.5={df['pm25'].isna().sum()}, NO2={df['no2'].isna().sum()}")
    
    # Connect to DuckDB
    con = duckdb.connect(DB_PATH)
    
    # Drop and recreate table to ensure clean load
    con.execute("DROP TABLE IF EXISTS raw_data_air_quality")
    
    # Create raw table
    con.execute("""
        CREATE TABLE raw_data_air_quality (
            station_id VARCHAR,
            station_type VARCHAR,
            timestamp VARCHAR,
            pm25 DOUBLE,
            pm10 DOUBLE,
            no2 DOUBLE,
            o3 DOUBLE,
            so2 DOUBLE,
            co DOUBLE,
            temperature DOUBLE,
            humidity DOUBLE
        )
    """)
    
    # Load raw data WITHOUT any cleaning
    con.execute("""
        INSERT INTO raw_data_air_quality 
        SELECT 
            station_id,
            station_type,
            timestamp,
            pm25,
            pm10,
            no2,
            o3,
            so2,
            co,
            temperature,
            humidity
        FROM df
    """)
    
    # Verify load
    row_count = con.execute("SELECT COUNT(*) FROM raw_data_air_quality").fetchone()[0]
    null_count = con.execute("""
        SELECT COUNT(*) FROM raw_data_air_quality 
        WHERE pm25 IS NULL OR no2 IS NULL OR o3 IS NULL
    """).fetchone()[0]
    
    print(f"\nRaw table loaded: {row_count} records")
    print(f"NULL values in raw table: {null_count} (preserved as-is)")
    
    # Also save as Parquet for scalability
    parquet_path = os.path.join(RAW_DIR, 'raw_data_air_quality.parquet')
    df.to_parquet(parquet_path, index=False)
    print(f"Raw Parquet saved: {parquet_path}")
    
    con.close()
    return row_count

if __name__ == '__main__':
    load_raw_data()
