"""
Run Complete ELT Pipeline
Execute all phases: Extract -> Load -> Transform
"""

import os
import sys

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

from extract import extract_air_quality_data
from load_raw import load_raw_data
from transform import transform_data

def run_pipeline():
    print("="*60)
    print("ELT PIPELINE - AIR QUALITY MONITORING")
    print("="*60)
    
    print("\n[PHASE 1] EXTRACT")
    print("-"*40)
    extract_air_quality_data(num_days=30)
    
    print("\n[PHASE 2] LOAD RAW")
    print("-"*40)
    load_raw_data()
    
    print("\n[PHASE 3] TRANSFORM")
    print("-"*40)
    transform_data()
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print("\nOutputs:")
    print("  - Raw data: data/raw/")
    print("  - Analytics: data/analytics/")
    print("  - Database: data/air_quality.duckdb")
    print("  - Dashboard: dashboard/index.html")
    print("\nTo view dashboard:")
    print("  python -m http.server 8000 --directory dashboard")
    print("  Open: http://localhost:8000")

if __name__ == '__main__':
    run_pipeline()
