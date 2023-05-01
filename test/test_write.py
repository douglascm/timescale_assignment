import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from main import write,query,save

def test_write(write_method='psycopg2'):
    if spark == None:
        # Startup spark session
        spark = SparkSession.builder \
            .master("local") \
            .appName("load_parquet") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
            .getOrCreate()
        
    save('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet', 'test/files/test.parquet')

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

    write('test','test',write_method)
    df = query("select * from test LIMIT 100;",mode='query')
    assert isinstance(df,pd.Dataframe), "Error writing file"
    
if __name__ == "__main__":
    test_write()
    print("Everything passed")