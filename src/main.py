#%%
import urllib.request as urllib
import os
import time
import sys
import time
import psycopg2
import pandas as pd
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from functions import delete_indexes, save, write, query, ask_range

nyc_path = '/src/files/nyc-ytaxi-data'
if not os.path.isdir(nyc_path): os.makedirs(nyc_path)

print('Welcome to the NYC yellow taxi data import toll')
input_mode=False
if input_mode:
    yn_range = 'No'
    while yn_range!='Yes':
        years = ask_range()
        yn_range = input(f'Files from {years[0]} up to {years[1]}. Is this range of years correct? Yes or No')
else:
    years=[2022,2023]

print(f'\nPulling data from Range {years[0]}-{years[1]}...')

years = range(years[0], years[1]+1)

# Url path
bUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
# File prefix
ycabPrx = "yellow_tripdata_"

#Availaiblity of data set by month & year
yearsDict = {}
months = range(1,13)

for year in years:    
    yearsDict[year] = months

ycabUrls = []
ycabFnames = []

for year, months in yearsDict.items():
    year_string = str(year)
    for month in months:
        month_string = str(month)
        if len(month_string) == 1:
            month_string = "0"+month_string
        url = bUrl+ycabPrx+year_string+'-'+month_string+".parquet"
        ycabUrls.append(url)
        ycabFnames.append(ycabPrx+".parquet")

conn_string = os.getenv('PSYCOPG2_JDBC_URL')
taxy_table = 'yellow_taxi_trips'

table_create_sql = f'''
CREATE TABLE IF NOT EXISTS {taxy_table} (
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

# Creates Tables and Hypertable Definition
print('Creating {taxy_table} on test database')
query(table_create_sql)
print('Creating {taxy_table} hypertable on test database')
query(f"SELECT create_hypertable('{taxy_table}','tpep_pickup_datetime', if_not_exists => TRUE);")

del_index=''
if input_mode:
    while del_index not in ('Yes', 'No'):
        del_index = input(f'Do you wish to delete all indexes (this improves performance for mass imports)? Yes or No')    
    if del_index=='Yes':
        delete_indexes(taxy_table)

# Startup spark session
spark = SparkSession.builder \
        .master("local") \
        .appName("load_parquet") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
        .getOrCreate()

# Select skip or upsert data
if input_mode:
    write_db_method=''
    while write_db_method not in ('Skip', 'Replace'):
        write_db_method = input(f'Do you wish to skip files already uploaded or replace date (upsert)? Skip or Replace')
else:
    write_db_method='Skip'

print(f'Querying files already imported into db...')
fnames = query(f"select distinct filename from {taxy_table}",mode='query').filename.values
print(f'Files already imported into db: {fnames}')

start_time_upload = time.time()

#%% For loop that donwloads all data from NYC taxis
for i, t in enumerate(zip(ycabUrls,ycabFnames)):
    #Skips Files already Inserted
    link, filename=t[0], t[1]
    if link[-13:-8] not in fnames or write_db_method=='Replace':
        print(i, link, filename)
        if save(link, nyc_path + '/' + filename):
            print('\n'+ nyc_path + '/' + filename)
            
            df = spark.read.parquet(nyc_path + '/' + filename)
            if 'filename' not in df.columns:
                df = df.withColumn('filename',psf.lit(link[-13:-8]))
                
            if link[-13:-8] in fnames and write_db_method=='Replace':
                query(f"delete from {taxy_table} where filename='{link[-13:-8]}';")
                
            # Function that writes to db
            write(taxy_table,df,spark=spark)
        else:
            print(f'{link} File not found, moving to next on the list...')
    else:
        print(f'{link} already imported into database, moving to next on the list...')
    
    # Removes temporary files
    for file in ['/src/files/temp.dir/yellow_taxi_trips.csv'
                 ,nyc_path + '/' + filename]:
        if os.path.isfile(file):
            os.remove(file)
    
print("Upload duration: {} seconds".format(time.time() - start_time_upload))

#%% Creates index for assignment tasks, after bulk loading
print(f'Creating index ix_fname on table test.{taxy_table}')
query("CREATE INDEX IF NOT EXISTS ix_fname ON yellow_taxi_trips (filename);")
print(f'Creating index ix_trip_distance on table test.{taxy_table}')
query("CREATE INDEX IF NOT EXISTS ix_trip_distance ON yellow_taxi_trips (trip_distance);")
print(f'Creating index ix_fnix_trip_locationame on table test.{taxy_table}')
query("CREATE INDEX IF NOT EXISTS ix_trip_location ON yellow_taxi_trips (pulocationid);")
print(f'Creating index ix_passenger_count_fare_amount_pulocationid on table test.{taxy_table}')
query("CREATE INDEX IF NOT EXISTS ix_passenger_count_fare_amount_pulocationid ON yellow_taxi_trips (passenger_count, fare_amount, pulocationid);")

# Return all the trips over 0.9 percentile in the distance traveled, limiting query since amount is 40m+ lines for the entire dataset
print(f'Return all the trips over 0.9 percentile in the distance traveled')
df = query("""
    select * from yellow_taxi_trips ytt
    where trip_distance >= (
        select percentile_cont(0.9) within group (order by trip_distance) 
        from yellow_taxi_trips
    ) LIMIT 1000000
    """,mode='query')
print(df.head(50))

#%% Aggregate that rolls up stats on passenger count and fare amount by pickup location. Leverages created indexes.
query("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS yellow_taxi_trips_pickup_loc
    WITH (timescaledb.continuous) AS
    SELECT
        pulocationid,
        time_bucket(INTERVAL '1 day', tpep_pickup_datetime) as bucket,
        sum(passenger_count) as sum_pax,
        max(passenger_count) AS high_pax,
        sum(fare_amount) as sum_fare,
        max(fare_amount) AS max_fare,
        min(fare_amount) AS low_fare
    FROM yellow_taxi_trips ytt
    GROUP BY pulocationid, bucket WITH DATA;
    """,autocommit=True)
