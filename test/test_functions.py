#%%
import urllib.request as urllib
import os
import pandas as pd
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from src.main import save, write, query

global spark

spark = SparkSession.builder \
    .master("local") \
    .appName("load_parquet") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .getOrCreate()

def test_save(url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet',filename='/app/test/files/test.parquet'):
    save(url,filename)
    assert os.path.isfile(filename), "Error saving file"

def test_write(write_method='psycopg2'):

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

    df = spark.read.parquet('test/files/test.parquet')
    if 'filename' not in df.columns:
        df = df.withColumn('filename',psf.lit('23-01'))

    res = write('test','test',write_method)
    assert res, "Error writing file"
    
def test_query(sql="select count(*) from test",mode='execute'):
    df = query(sql,mode)
    assert isinstance(df,pd.DataFrame), "Error querying file"

def test_percentile(percentile=0.9):
    df = query(f"""
    select * from test ytt
    where trip_distance >= (
    select percentile_cont({percentile}) within group (order by trip_distance) 
    from test
    )""",mode='query')
    assert isinstance(df,pd.DataFrame), "Error querying percentiles"

#%% Run tests
if __name__ == "__main__":
    test_save()
    test_write()
    test_query()
    test_percentile()
    print("Everything passed")