"""
Transform Script - Apply transformations AFTER load (ELT principle)
Transformations run inside the database using SQL
"""

import pandas as pd
import duckdb
import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
ANALYTICS_DIR = os.path.join(DATA_DIR, 'analytics')
DB_PATH = os.path.join(DATA_DIR, 'air_quality.duckdb')

def transform_data():
    """
    Apply transformations AFTER load:
    - Clean missing values
    - Fix data types
    - Calculate AQI
    - Create aggregations
    """
    os.makedirs(ANALYTICS_DIR, exist_ok=True)
    
    con = duckdb.connect(DB_PATH)
    
    # Verify raw data exists
    raw_count = con.execute("SELECT COUNT(*) FROM raw_data_air_quality").fetchone()[0]
    if raw_count == 0:
        raise ValueError("Raw table is empty. Run load_raw.py first.")
    
    print(f"Starting transformation of {raw_count} raw records...")
    
    # TRANSFORMATION 1: Clean and type-fix
    print("Step 1: Cleaning missing values and fixing types...")
    
    con.execute("""
        CREATE OR REPLACE TABLE analytics_air_quality AS
        WITH cleaned AS (
            SELECT 
                station_id,
                station_type,
                CAST(timestamp AS TIMESTAMP) as timestamp,
                -- Fill missing values with station average
                COALESCE(pm25, AVG(pm25) OVER (PARTITION BY station_id)) as pm25,
                COALESCE(pm10, AVG(pm10) OVER (PARTITION BY station_id)) as pm10,
                COALESCE(no2, AVG(no2) OVER (PARTITION BY station_id)) as no2,
                COALESCE(o3, AVG(o3) OVER (PARTITION BY station_id)) as o3,
                so2,
                co,
                temperature,
                humidity
            FROM raw_data_air_quality
        ),
        
        -- TRANSFORMATION 2: Calculate AQI
        with_aqi AS (
            SELECT *,
                -- US EPA AQI formula for PM2.5
                CASE 
                    WHEN pm25 <= 12.0 THEN pm25 * 50.0 / 12.0
                    WHEN pm25 <= 35.4 THEN 50.0 + (pm25 - 12.0) * 50.0 / 23.4
                    WHEN pm25 <= 55.4 THEN 100.0 + (pm25 - 35.4) * 50.0 / 20.0
                    WHEN pm25 <= 150.4 THEN 150.0 + (pm25 - 55.4) * 50.0 / 95.0
                    WHEN pm25 <= 250.4 THEN 200.0 + (pm25 - 150.4) * 100.0 / 100.0
                    ELSE 300.0 + (pm25 - 250.4) * 100.0 / 150.0
                END as aqi,
                EXTRACT(HOUR FROM timestamp) as hour,
                EXTRACT(DOW FROM timestamp) as day_of_week,
                CAST(timestamp AS DATE) as date
            FROM cleaned
        )
        
        -- TRANSFORMATION 3: Add health category
        SELECT *,
            CASE 
                WHEN aqi <= 50 THEN 'Good'
                WHEN aqi <= 100 THEN 'Moderate'
                WHEN aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
                WHEN aqi <= 200 THEN 'Unhealthy'
                WHEN aqi <= 300 THEN 'Very Unhealthy'
                ELSE 'Hazardous'
            END as health_category,
            CASE 
                WHEN aqi <= 50 THEN '#00E400'
                WHEN aqi <= 100 THEN '#FFFF00'
                WHEN aqi <= 150 THEN '#FF7E00'
                WHEN aqi <= 200 THEN '#FF0000'
                WHEN aqi <= 300 THEN '#8F3F97'
                ELSE '#7E0023'
            END as health_color
        FROM with_aqi
    """)
    
    print("Step 2: Creating aggregation views...")
    
    # Daily aggregations
    con.execute("""
        CREATE OR REPLACE VIEW daily_stats AS
        SELECT 
            date,
            station_id,
            station_type,
            ROUND(AVG(pm25), 2) as avg_pm25,
            ROUND(MAX(pm25), 2) as max_pm25,
            ROUND(MIN(pm25), 2) as min_pm25,
            ROUND(AVG(pm10), 2) as avg_pm10,
            ROUND(AVG(no2), 2) as avg_no2,
            ROUND(AVG(o3), 2) as avg_o3,
            ROUND(AVG(aqi), 2) as avg_aqi,
            ROUND(MAX(aqi), 2) as max_aqi,
            COUNT(*) as measurements,
            SUM(CASE WHEN health_category = 'Good' THEN 1 ELSE 0 END) as good_hours,
            SUM(CASE WHEN health_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) as unhealthy_hours
        FROM analytics_air_quality
        GROUP BY date, station_id, station_type
    """)
    
    # Hourly patterns
    con.execute("""
        CREATE OR REPLACE VIEW hourly_patterns AS
        SELECT 
            hour,
            ROUND(AVG(pm25), 2) as avg_pm25,
            ROUND(AVG(pm10), 2) as avg_pm10,
            ROUND(AVG(no2), 2) as avg_no2,
            ROUND(AVG(o3), 2) as avg_o3,
            ROUND(AVG(aqi), 2) as avg_aqi,
            COUNT(*) as measurements
        FROM analytics_air_quality
        GROUP BY hour
        ORDER BY hour
    """)
    
    # Station summary
    con.execute("""
        CREATE OR REPLACE VIEW station_summary AS
        SELECT 
            station_id,
            station_type,
            ROUND(AVG(pm25), 2) as avg_pm25,
            ROUND(AVG(aqi), 2) as avg_aqi,
            ROUND(MAX(aqi), 2) as max_aqi,
            COUNT(*) as total_measurements,
            ROUND(SUM(CASE WHEN health_category = 'Good' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_good,
            ROUND(SUM(CASE WHEN health_category IN ('Unhealthy', 'Very Unhealthy') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_unhealthy
        FROM analytics_air_quality
        GROUP BY station_id, station_type
    """)
    
    print("Step 3: Exporting to Parquet and JSON...")
    
    # Export to Parquet
    analytics_df = con.execute("SELECT * FROM analytics_air_quality").df()
    analytics_df.to_parquet(os.path.join(ANALYTICS_DIR, 'analytics_air_quality.parquet'), index=False)
    
    daily_df = con.execute("SELECT * FROM daily_stats").df()
    daily_df.to_parquet(os.path.join(ANALYTICS_DIR, 'daily_stats.parquet'), index=False)
    
    hourly_df = con.execute("SELECT * FROM hourly_patterns").df()
    hourly_df.to_parquet(os.path.join(ANALYTICS_DIR, 'hourly_patterns.parquet'), index=False)
    
    station_df = con.execute("SELECT * FROM station_summary").df()
    station_df.to_parquet(os.path.join(ANALYTICS_DIR, 'station_summary.parquet'), index=False)
    
    # Export JSON for dashboard
    hourly_df.to_json(os.path.join(ANALYTICS_DIR, 'hourly_patterns.json'), orient='records')
    station_df.to_json(os.path.join(ANALYTICS_DIR, 'station_summary.json'), orient='records')
    daily_df.to_json(os.path.join(ANALYTICS_DIR, 'daily_stats.json'), orient='records')
    
    # Summary statistics
    summary = con.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT date) as total_days,
            COUNT(DISTINCT station_id) as total_stations,
            ROUND(AVG(pm25), 2) as avg_pm25,
            ROUND(MAX(pm25), 2) as max_pm25,
            ROUND(AVG(aqi), 2) as avg_aqi,
            ROUND(MAX(aqi), 2) as max_aqi,
            ROUND(SUM(CASE WHEN health_category = 'Good' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_good,
            ROUND(SUM(CASE WHEN health_category = 'Moderate' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_moderate,
            ROUND(SUM(CASE WHEN health_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_unhealthy
        FROM analytics_air_quality
    """).df().to_dict('records')[0]
    
    with open(os.path.join(ANALYTICS_DIR, 'summary_stats.json'), 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("\n" + "="*50)
    print("TRANSFORMATION COMPLETE")
    print("="*50)
    print(f"Total records transformed: {summary['total_records']}")
    print(f"Date range: {summary['total_days']} days")
    print(f"Stations: {summary['total_stations']}")
    print(f"Average AQI: {summary['avg_aqi']}")
    print(f"Good air quality: {summary['pct_good']}%")
    print(f"Unhealthy: {summary['pct_unhealthy']}%")
    print("="*50)
    
    # Verify raw data unchanged
    raw_nulls = con.execute("""
        SELECT COUNT(*) FROM raw_data_air_quality 
        WHERE pm25 IS NULL OR no2 IS NULL
    """).fetchone()[0]
    print(f"\nRAW DATA VERIFICATION: {raw_nulls} NULL values preserved (unchanged)")
    
    con.close()
    return summary

if __name__ == '__main__':
    transform_data()
