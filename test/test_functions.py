#%%
import urllib.request as urllib
import os
import pandas as pd
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from src.functions import save, write, query
import pytest

global spark

spark = SparkSession.builder \
    .master("local") \
    .appName("load_parquet") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .getOrCreate()

@pytest.fixture(scope="session")
def cache_dir(tmp_path_factory):
   #creates a context
   with tmp_path_factory.mktemp("files") as f:
       #yields the file path, holds the context open
       yield f / "test.parquet"

def test_save(cache_dir):
    url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
    save(url,cache_dir)
    assert os.path.isfile(cache_dir), "Error saving file"

def test_write(cache_dir):
    df = pd.read_parquet(cache_dir, engine='pyarrow')
    if 'filename' not in df.columns:
        df['filename'] = '20-03'

    table_create_sql = f'''
        CREATE TABLE IF NOT EXISTS test (
            VendorId bigint,
            tpep_pickup_datetime  timestamp,
            tpep_dropoff_datetime timestamp,
            passenger_count decimal,
            trip_distance decimal,
            RatecodeID decimal,
            store_and_fwd_flag char,
            PULocationID int,
            DOLocationID int,
            payment_type int,
            fare_amount decimal,
            extra decimal,
            mta_tax decimal,
            tip_amount decimal,
            tolls_amount decimal,
            improvement_surcharge decimal,
            total_amount decimal,
            congestion_surcharge decimal,
            airport_fee decimal,
            filename char(5)
            )
        '''
    
    query(table_create_sql)
    query("truncate table test;")
    
    res = write('test',df,'psycopg2')

    assert res=='Success', "Error writing file"
    
def test_query(sql="select * from test LIMIT 10000",mode='query'):
    df = query(sql,mode,autocommit=True)
    
    assert isinstance(df,pd.DataFrame), "Error querying table tests"

def test_percentile(percentile=0.9):
    query("CREATE INDEX IF NOT EXISTS ix_trip_distance ON test (trip_distance);")
    df = query(f"""
    select * from test ytt
    where trip_distance >= (
    select percentile_cont({percentile}) within group (order by trip_distance) 
    from test
    )""",mode='query',autocommit=True)
    
    assert isinstance(df,pd.DataFrame), "Error querying percentiles"

#%% Run tests
if __name__ == "__main__":
    print(cache_dir)
    test_save()
    test_write()
    test_query()
    test_percentile()
    print("Everything passed")
# %%
