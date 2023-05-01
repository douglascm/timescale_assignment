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

def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno
    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)
    print ("psycopg2 traceback:", traceback, "-- obj:", err_obj)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def execute_copy(fileName,table_name,conn_string):
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
    with open(fileName, 'r') as f:
        cur.copy_from(f, table_name, sep=',', null='')    
    con.commit()
    con.close()
    
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

def parquet2csv(df,path,filename):
    # Spark write method is 5x faster than pd.to_csv
    df.coalesce(1).write.options(header='False',delimeter=',').mode("overwrite").csv(path)
    listFiles = os.listdir(f'{path}') 
    for subFiles in listFiles:
        if subFiles[-4:] == ".csv":
            os.rename(path + subFiles,  f'{path}{filename}')
    return print(f'{filename} saved into folder{path}')

def write(table,df,write_method='psycopg2',spark=None):
    if spark == None:
        # Startup spark session
        spark = SparkSession.builder \
            .master("local") \
            .appName("load_parquet") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
            .getOrCreate()
    
    if write_method == 'spark':
        # Write with spark (~150s per file)
        start_time = time.time()
        df.write.mode("append").jdbc(
            url=os.environ.get('SPARK_JDBC_URL'),
            table=table,
            properties = {
            'user': 'postgres',
            'password': 'password',
            'driver': 'org.postgresql.Driver',
            'stringtype': 'unspecified'}
        )
        print("spark write duration: {} seconds".format(time.time() - start_time))
    elif write_method=='psycopg2':
        # Write through pandas and csv (~40s per file)
        start_time = time.time()
        # Writing dataframe to csv and renaming to readeble filename
        parquet2csv(df,'/src/files/temp.dir/','yellow_taxi_trips.csv')
        print("spark write csv duration: {} seconds".format(time.time() - start_time))
        
        start_time = time.time()
        # With Postgresql COPY command and psycopg2 data is pushed into database table yellow_taxi_trips in conn_string
        execute_copy('/src/files/temp.dir/yellow_taxi_trips.csv',table,os.environ.get('PSYCOPG2_JDBC_URL'))
        print("psycopg2 COPY duration: {} seconds".format(time.time() - start_time))
    else: 
        print('Invalid write method.')

def query(sql,mode='execute'):
    pg_conn = psycopg2.connect(os.environ.get('PSYCOPG2_JDBC_URL'))
    cur = pg_conn.cursor()
    try:
        cur.execute(sql)
        if mode == 'query':
            df = pd.DataFrame(cur.fetchall(),columns = [desc[0] for desc in cur.description])
        else:
            df = print('Query executed successfully...')
    except Exception as err:
        df = print_psycopg2_exception(err)
    pg_conn.commit()
    cur.close()
    return df

nyc_path = '/src/files/nyc-ytaxi-data'
if not os.path.isdir(nyc_path): os.makedirs(nyc_path)

# Url path
bUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
# File prefix
ycabPrx = "yellow_tripdata_"

#Availaiblity of data set by month & year
yearsDict = {}
years = range(2013, 2023)
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
query(table_create_sql)
query(f"SELECT create_hypertable('{taxy_table}','tpep_pickup_datetime', if_not_exists => TRUE);")

# Startup spark session
spark = SparkSession.builder \
        .master("local") \
        .appName("load_parquet") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
        .getOrCreate()

#%% For loop that donwloads all data from NYC taxis
fnames = query(f"select distinct filename from {taxy_table}",mode='query').values.tolist()
start_time_upload = time.time()

for i, t in enumerate(zip(ycabUrls,ycabFnames)):
    #Skips Files already Inserted
    link, filename=t[0], t[1]
    if link[-13:-8] not in fnames:
        print(i, link, filename)
        save(link, nyc_path + '/' + filename)
        print('\n'+ nyc_path + '/' + filename)
        
        df = spark.read.parquet(nyc_path + '/' + filename)
        if 'filename' not in df.columns:
            df = df.withColumn('filename',psf.lit(link[-13:-8]))
        
        # Function that writes to db
        write(taxy_table,df,spark=spark)
    
    # Removes temporary files
    for file in ['/src/files/temp.dir/yellow_taxi_trips.csv'
                 ,nyc_path + '/' + filename]:
        if os.path.isfile(file):
            os.remove(file)
    
print("Upload duration: {} seconds".format(time.time() - start_time_upload))

#%% Creates index for assignment tasks
query("CREATE INDEX IF NOT EXISTS ix_fname ON yellow_taxi_trips (filename);")
query("CREATE INDEX IF NOT EXISTS ix_trip_distance ON yellow_taxi_trips (trip_distance);")
query("CREATE INDEX IF NOT EXISTS ix_trip_location ON yellow_taxi_trips (pulocationid);")
query("CREATE INDEX IF NOT EXISTS ix_passenger_count_fare_amount_pulocationid ON yellow_taxi_trips (passenger_count, fare_amount, pulocationid);")

#%% Return all the trips over 0.9 percentile in the distance traveled, limiting query since amount is 40m+ lines for the entire dataset
df = query("""
    select * from yellow_taxi_trips ytt
    where trip_distance >= (
        select percentile_cont(0.9) within group (order by trip_distance) 
        from yellow_taxi_trips
    ) LIMIT 1000000
    """,method='query')

#%% Aggregate that rolls up stats on passenger count and fare amount by pickup location. Leverages created indexes.
query("""
    CREATE MATERIALIZED VIEW yellow_taxi_trips_pickup_loc
    WITH (timescaledb.continuous) AS
    SELECT
        pulocationid,
        sum(passenger_count) as sum_pax,
        max(passenger_count) AS high_pax,
        sum(fare_amount) as sum_fare,
        max(fare_amount) AS max_fare,
        min(fare_amount) AS low_fare
    FROM yellow_taxi_trips ytt
    GROUP BY pulocationid WITH NO DATA;
    REFRESH MATERIALIZED VIEW yellow_taxi_trips_pickup_loc;
    """)