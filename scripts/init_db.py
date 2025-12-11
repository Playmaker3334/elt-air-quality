"""
Database Initialization Script
Creates the DuckDB database and required directories
"""

import duckdb
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH = os.path.join(DATA_DIR, 'air_quality.duckdb')

def init_database():
    """Initialize the database and create directories"""
    
    # Create directories
    os.makedirs(os.path.join(DATA_DIR, 'raw'), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, 'analytics'), exist_ok=True)
    
    print(f"Data directory: {DATA_DIR}")
    print(f"Database path: {DB_PATH}")
    
    # Initialize DuckDB
    con = duckdb.connect(DB_PATH)
    
    # Create tables
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_data_air_quality (
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
    
    print("Database initialized successfully")
    
    con.close()

if __name__ == '__main__':
    init_database()
