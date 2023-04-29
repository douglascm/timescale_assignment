#%%
import urllib.request as urllib
import os
import time
import sys
import psycopg2
import pandas as pd
#import numpy as np
#from pyspark.sql import SparkSession

nyc_path = '/src/files/nyc-ytaxi-data'
if not os.path.isdir(nyc_path): os.makedirs(nyc_path)

# Url path
bUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
# File prefix
ycabPrx = "yellow_tripdata_"

#Availaiblity of data set by month & year
yearsDict = {}
years = range(2009, 2023)
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
        ycabFnames.append(ycabPrx+year_string+'-'+month_string+".parquet")

def reporthook(count, block_size, total_size):
    global start_time
    if count == 0:
        start_time = time.time()
        return
    duration = time.time() - start_time
    progress_size = int(count * block_size)
    speed = int(progress_size / (1024 * duration))
    percent = int(count * block_size * 100 / total_size)
    size_mb = progress_size / (1024 * 1024)
    sys.stdout.write(f"\r...{percent}%%, {size_mb} MB, {speed} KB/s, {duration} seconds passed")
    sys.stdout.flush()

def save(url, filename):
    urllib.urlretrieve(url, filename, reporthook)

conn_string = "postgresql://postgres:password@host.docker.internal:5432/example"

table_create_sql = '''
CREATE TABLE IF NOT EXISTS {} (
    VendorId bigint,
    tpep_pickup_datetime  timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count decimal,
    trip_distance decimal,
    RatecodeID decimal,
    store_and_fwd_flag boolean,
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
    airport_fee decimal
    )
'''

pg_conn = psycopg2.connect(conn_string)
cur = pg_conn.cursor()
cur.execute(table_create_sql.format('yellow_taxi_trips'))
#cur.execute("SELECT create_hypertable('yellow_taxi_trips','tpep_pickup_datetime');")
#cur.execute("CREATE INDEX ix_trip_distance ON yellow_taxi_trips (trip_distance);")
#cur.execute("CREATE INDEX ix_passenger_count_fare_amount_PULocationID ON yellow_taxi_trips (passenger_count, fare_amount,PULocationID);")
cur.execute('TRUNCATE TABLE yellow_taxi_trips')
pg_conn.commit()
cur.close()


#%%
for i,t in enumerate(zip(ycabUrls,ycabFnames)):
    link,filename=t[0],t[1]
    print(i, link, filename)
    save(url, nyc_path + '/' + filename)
    print('\n'+ nyc_path + '/' + filename)
    
    start_time = time.time()
    df = pd.read_parquet(nyc_path + '/' + filename, engine='pyarrow')
    # df.replace(r'^\s+$', np.nan, regex=True) 
    print("read_parquet duration: {} seconds".format(time.time() - start_time))
    
    start_time = time.time()
    df.to_csv('upload_test_data_from_copy.csv', index=False, header=False)
    print("to_csv duration: {} seconds".format(time.time() - start_time))
    
    pg_conn = psycopg2.connect(conn_string)
    cur = pg_conn.cursor()
    cur.execute(table_create_sql.format('yellow_taxi_trips'))
    #cur.execute('TRUNCATE TABLE copy_test')

    start_time = time.time()
    with open('upload_test_data_from_copy.csv', 'r') as f:
        cur.copy_from(f, 'copy_test', sep=',', null='')    

    pg_conn.commit()
    cur.close()
    print("COPY duration: {} seconds".format(time.time() - start_time))
    if i>9:
        break

#%%

cur = pg_conn.cursor()
cur.execute("select * from copy_test")
df2 = pd.DataFrame(cur.fetchall(),columns = [desc[0] for desc in cur.description])

#close connection
cur.close()
# %%