import pandas as pd
from src.main import query

def test_percentile(percentile=0.9):
    df = query(f"""
    select * from yellow_taxi_trips ytt
    where trip_distance >= (
    select percentile_cont({percentile}) within group (order by trip_distance) 
    from yellow_taxi_trips
    )""",mode='query')
    assert isinstance(df,pd.DataFrame) or isinstance(df,str), "Error querying percentiles"
    
if __name__ == "__main__":
    test_percentile()
    print("Everything passed")