"""
ELT Pipeline for Air Quality Monitoring Data
Universidad Politécnica de Yucatán - Visual Information Modeling
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

sys.path.insert(0, '/opt/airflow/scripts')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'air_quality_elt_pipeline',
    default_args=default_args,
    description='ELT Pipeline for Air Quality Monitoring Data',
    schedule_interval='0 6 * * *',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['air-quality', 'elt', 'environment'],
)


def extract_data(**context):
    import pandas as pd
    import numpy as np
    import logging
    
    logging.info("Starting data extraction...")
    
    np.random.seed(42)
    stations = {
        'ST001': 'urban',
        'ST002': 'industrial', 
        'ST003': 'residential',
        'ST004': 'traffic',
        'ST005': 'background'
    }
    
    start_date = datetime(2025, 11, 1)
    dates = pd.date_range(start=start_date, periods=30*24, freq='H')
    
    records = []
    for station_id, station_type in stations.items():
        base_levels = {
            'urban': {'pm25': 35, 'pm10': 50, 'no2': 30},
            'industrial': {'pm25': 55, 'pm10': 80, 'no2': 45},
            'residential': {'pm25': 25, 'pm10': 35, 'no2': 20},
            'traffic': {'pm25': 45, 'pm10': 65, 'no2': 50},
            'background': {'pm25': 15, 'pm10': 25, 'no2': 10}
        }
        
        base = base_levels[station_type]
        
        for dt in dates:
            hour = dt.hour
            is_weekend = dt.weekday() >= 5
            rush_factor = 1.5 if hour in [7, 8, 17, 18] else 1.0
            weekend_factor = 0.7 if is_weekend else 1.0
            
            record = {
                'station_id': station_id,
                'station_type': station_type,
                'timestamp': dt.strftime('%Y-%m-%d %H:%M:%S'),
                'pm25': round(base['pm25'] * rush_factor * weekend_factor * np.random.uniform(0.7, 1.3), 2),
                'pm10': round(base['pm10'] * rush_factor * weekend_factor * np.random.uniform(0.6, 1.4), 2),
                'no2': round(base['no2'] * rush_factor * weekend_factor * np.random.uniform(0.8, 1.2), 2),
                'o3': round(np.random.uniform(20, 80), 2),
                'so2': round(np.random.uniform(5, 20), 2),
                'co': round(np.random.uniform(0.5, 2.0), 2),
                'temperature': round(np.random.uniform(10, 35), 1),
                'humidity': round(np.random.uniform(40, 90), 1)
            }
            records.append(record)
    
    df = pd.DataFrame(records)
    
    missing_mask_pm25 = np.random.random(len(df)) < 0.05
    missing_mask_no2 = np.random.random(len(df)) < 0.03
    df.loc[missing_mask_pm25, 'pm25'] = None
    df.loc[missing_mask_no2, 'no2'] = None
    
    output_dir = '/opt/airflow/data/raw'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f'{output_dir}/air_quality_extracted.csv'
    df.to_csv(output_path, index=False)
    
    logging.info(f"Extracted {len(df)} records to {output_path}")
    return output_path


def load_raw_data(**context):
    import pandas as pd
    import duckdb
    import logging
    
    logging.info("Starting raw data load...")
    
    ti = context['ti']
    input_path = ti.xcom_pull(task_ids='extract_data')
    
    df = pd.read_csv(input_path)
    logging.info(f"Loading {len(df)} records...")
    
    db_path = '/opt/airflow/data/air_quality.duckdb'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    
    con.execute("DROP TABLE IF EXISTS raw_data_air_quality")
    con.execute("CREATE TABLE raw_data_air_quality AS SELECT * FROM df")
    
    null_count = con.execute("SELECT COUNT(*) - COUNT(pm25) + COUNT(*) - COUNT(no2) FROM raw_data_air_quality").fetchone()[0]
    
    logging.info(f"Raw table loaded: {len(df)} records")
    logging.info(f"NULL values preserved: {null_count}")
    
    parquet_path = '/opt/airflow/data/raw/raw_data_air_quality.parquet'
    df.to_parquet(parquet_path, index=False)
    
    con.close()
    return db_path


def transform_data(**context):
    import duckdb
    import logging
    
    logging.info("Starting transformations...")
    
    db_path = '/opt/airflow/data/air_quality.duckdb'
    con = duckdb.connect(db_path)
    
    try:
        con.execute("DROP TABLE IF EXISTS analytics_air_quality")
        con.execute("""
            CREATE TABLE analytics_air_quality AS
            SELECT 
                station_id,
                station_type,
                CAST(timestamp AS TIMESTAMP) as timestamp,
                COALESCE(pm25, (
                    SELECT AVG(pm25) FROM raw_data_air_quality r2 
                    WHERE r2.station_id = raw_data_air_quality.station_id AND pm25 IS NOT NULL
                )) as pm25,
                pm10,
                COALESCE(no2, (
                    SELECT AVG(no2) FROM raw_data_air_quality r2 
                    WHERE r2.station_id = raw_data_air_quality.station_id AND no2 IS NOT NULL
                )) as no2,
                o3,
                so2,
                co,
                temperature,
                humidity,
                CASE 
                    WHEN COALESCE(pm25, 0) <= 12 THEN COALESCE(pm25, 0) * 50 / 12
                    WHEN COALESCE(pm25, 0) <= 35.4 THEN 50 + (COALESCE(pm25, 0) - 12) * 50 / 23.4
                    WHEN COALESCE(pm25, 0) <= 55.4 THEN 100 + (COALESCE(pm25, 0) - 35.4) * 50 / 20
                    WHEN COALESCE(pm25, 0) <= 150.4 THEN 150 + (COALESCE(pm25, 0) - 55.4) * 50 / 95
                    ELSE 200 + (COALESCE(pm25, 0) - 150.4) * 100 / 100
                END as aqi
            FROM raw_data_air_quality
        """)
        
        con.execute("ALTER TABLE analytics_air_quality ADD COLUMN health_category VARCHAR")
        con.execute("""
            UPDATE analytics_air_quality SET health_category = 
            CASE 
                WHEN aqi <= 50 THEN 'Good'
                WHEN aqi <= 100 THEN 'Moderate'
                WHEN aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
                WHEN aqi <= 200 THEN 'Unhealthy'
                WHEN aqi <= 300 THEN 'Very Unhealthy'
                ELSE 'Hazardous'
            END
        """)
        
        con.execute("DROP VIEW IF EXISTS hourly_patterns")
        con.execute("""
            CREATE VIEW hourly_patterns AS
            SELECT EXTRACT(HOUR FROM timestamp) as hour, AVG(pm25) as avg_pm25, AVG(no2) as avg_no2, AVG(aqi) as avg_aqi
            FROM analytics_air_quality GROUP BY EXTRACT(HOUR FROM timestamp) ORDER BY hour
        """)
        
        con.execute("DROP VIEW IF EXISTS station_summary")
        con.execute("""
            CREATE VIEW station_summary AS
            SELECT station_id, station_type, ROUND(AVG(pm25), 1) as avg_pm25, ROUND(AVG(aqi), 1) as avg_aqi, ROUND(MAX(aqi), 0) as max_aqi,
                ROUND(100.0 * SUM(CASE WHEN health_category = 'Good' THEN 1 ELSE 0 END) / COUNT(*), 1) as good_pct,
                ROUND(100.0 * SUM(CASE WHEN health_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) / COUNT(*), 1) as unhealthy_pct
            FROM analytics_air_quality GROUP BY station_id, station_type
        """)
        
        output_dir = '/opt/airflow/data/analytics'
        os.makedirs(output_dir, exist_ok=True)
        con.execute(f"COPY analytics_air_quality TO '{output_dir}/analytics_air_quality.parquet' (FORMAT PARQUET)")
        
        stats = con.execute("SELECT COUNT(*), ROUND(AVG(aqi), 2) FROM analytics_air_quality").fetchone()
        logging.info(f"TRANSFORMATION COMPLETE: {stats[0]} records, Avg AQI: {stats[1]}")
        
    except Exception as e:
        logging.error(f"Transformation error: {e}")
        raise
    finally:
        con.close()


def validate_pipeline(**context):
    import duckdb
    import logging
    
    logging.info("Validating pipeline...")
    
    db_path = '/opt/airflow/data/air_quality.duckdb'
    con = duckdb.connect(db_path)
    
    try:
        raw_count = con.execute("SELECT COUNT(*) FROM raw_data_air_quality").fetchone()[0]
        raw_nulls = con.execute("SELECT COUNT(*) - COUNT(pm25) FROM raw_data_air_quality").fetchone()[0]
        analytics_count = con.execute("SELECT COUNT(*) FROM analytics_air_quality").fetchone()[0]
        
        logging.info("=" * 50)
        logging.info("PIPELINE VALIDATION")
        logging.info("=" * 50)
        logging.info(f"Raw table: {raw_count} records, {raw_nulls} NULL PM2.5 values")
        logging.info(f"Analytics table: {analytics_count} records")
        logging.info("Pipeline completed successfully!")
        
    finally:
        con.close()


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_raw_data',
    python_callable=load_raw_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_pipeline',
    python_callable=validate_pipeline,
    dag=dag,
)

extract_task >> load_task >> transform_task >> validate_task