"""
Extract Script - Generates synthetic air quality data
Can be used standalone or called from Airflow DAG
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')

def extract_air_quality_data(date_str=None, num_days=30):
    """
    Generate synthetic air quality data from monitoring stations
    
    Args:
        date_str: Specific date (YYYY-MM-DD) or None for multiple days
        num_days: Number of days to generate if date_str is None
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    
    stations = ['ST001', 'ST002', 'ST003', 'ST004', 'ST005']
    station_types = ['urban', 'industrial', 'residential', 'traffic', 'background']
    
    all_data = []
    
    if date_str:
        dates = [date_str]
    else:
        base_date = datetime(2025, 11, 1)
        dates = [(base_date + pd.Timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]
    
    for date in dates:
        np.random.seed(hash(date) % 2**32)
        
        for station, stype in zip(stations, station_types):
            for hour in range(24):
                # Base pollution levels by station type
                base_pm25 = {'urban': 35, 'industrial': 55, 'residential': 25, 
                            'traffic': 45, 'background': 15}[stype]
                base_pm10 = base_pm25 * 1.8
                
                # Rush hour effect (7-9 AM, 5-7 PM)
                rush_factor = 1.5 if hour in [7, 8, 9, 17, 18, 19] else 1.0
                
                # Weekend reduction
                day_of_week = datetime.strptime(date, '%Y-%m-%d').weekday()
                weekend_factor = 0.7 if day_of_week >= 5 else 1.0
                
                record = {
                    'station_id': station,
                    'station_type': stype,
                    'timestamp': f"{date} {hour:02d}:00:00",
                    'pm25': round(max(0, base_pm25 * rush_factor * weekend_factor + np.random.normal(0, 10)), 2),
                    'pm10': round(max(0, base_pm10 * rush_factor * weekend_factor + np.random.normal(0, 15)), 2),
                    'no2': round(max(0, 30 * rush_factor * weekend_factor + np.random.normal(0, 8)), 2),
                    'o3': round(max(0, 40 + np.random.normal(0, 12)), 2),
                    'so2': round(max(0, 10 + np.random.normal(0, 3)), 2),
                    'co': round(max(0, 0.8 * rush_factor + np.random.normal(0, 0.2)), 2),
                    'temperature': round(22 + np.random.normal(0, 5), 1),
                    'humidity': round(min(100, max(0, 60 + np.random.normal(0, 15))), 1),
                }
                
                # Introduce realistic missing values
                if np.random.random() < 0.05:
                    record['pm25'] = None
                if np.random.random() < 0.03:
                    record['no2'] = None
                if np.random.random() < 0.02:
                    record['o3'] = None
                    
                all_data.append(record)
    
    df = pd.DataFrame(all_data)
    
    # Save combined file
    output_path = os.path.join(RAW_DIR, 'air_quality_extracted.csv')
    df.to_csv(output_path, index=False)
    
    print(f"Extracted {len(df)} records to {output_path}")
    print(f"Date range: {dates[0]} to {dates[-1]}")
    print(f"Stations: {stations}")
    print(f"Missing values: PM2.5={df['pm25'].isna().sum()}, NO2={df['no2'].isna().sum()}")
    
    return df

if __name__ == '__main__':
    extract_air_quality_data(num_days=30)
