#%%
import urllib.request as urllib
import os
import time
import sys
import time
import psycopg2
import pandas as pd
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

write_method='pandas'
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
    airport_fee decimal
    )
'''

pg_conn = psycopg2.connect(conn_string)
cur = pg_conn.cursor()
cur.execute(table_create_sql.format('yellow_taxi_trips'))
cur.execute("SELECT create_hypertable('yellow_taxi_trips','tpep_pickup_datetime');")
cur.execute("ALTER TABLE yellow_taxi_trips SET UNLOGGED")
cur.execute("ALTER TABLE yellow_taxi_trips DISABLE TRIGGER ALL")
#cur.execute('TRUNCATE TABLE yellow_taxi_trips')
pg_conn.commit()
cur.close()


appName = "load_parquet"
master = "local"

spark = SparkSession.builder \
        .master(master) \
        .appName(appName) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
        .getOrCreate()

#%%
start_time_upload = time.time()
for i, t in enumerate(zip(ycabUrls,ycabFnames)):
    link, filename=t[0], t[1]
    print(i, link, filename)
    save(url, nyc_path + '/' + filename)
    print('\n'+ nyc_path + '/' + filename)
    
    if write_method == 'spark':
        # Write with spark (~150s per file)
        start_time = time.time()
        df = spark.read.parquet(nyc_path + '/' + filename)
        print("spark read duration: {} seconds".format(time.time() - start_time))
        start_time = time.time()
        df.write.mode("append").jdbc(
            url='jdbc:postgresql://host.docker.internal:5432/example',
            table="yellow_taxi_trips",
            properties = {
            'user': 'postgres',
            'password': 'password',
            'driver': 'org.postgresql.Driver',
            'stringtype': 'unspecified'}
        )
        print("spark write  duration: {} seconds".format(time.time() - start_time))
    elif write_method=='pandas':
        # Write through pandas and csv (~40s per file)
        start_time = time.time()
        df = spark.read.parquet(nyc_path + '/' + filename)
        print("spark read parquet duration: {} seconds".format(time.time() - start_time))
        
        start_time = time.time()
        df.coalesce(1).write.options(header='False',delimeter=',').mode("overwrite").csv('/src/files/temp.dir/')
        listFiles = os.listdir('/src/files/temp.dir/') 
        for subFiles in listFiles:
            if subFiles[-4:] == ".csv":
                os.rename('/src/files/temp.dir/' + subFiles,  '/src/files/temp.dir/yellow_taxi_trips.csv')
        
        print("spark write csv duration: {} seconds".format(time.time() - start_time))
        
        start_time = time.time()
        execute_copy('/src/files/temp.dir/yellow_taxi_trips.csv','yellow_taxi_trips',conn_string)
        print("psycopg2 COPY duration: {} seconds".format(time.time() - start_time))
    else: 
        print('Invalid write method.')
    
    # Removes temporary files
    for file in ['/src/upload_test_data_from_copy.csv',nyc_path + '/' + filename]:
        if os.path.isfile(file):
            os.remove(file)
    
print("Upload duration: {} seconds".format(time.time() - start_time_upload))

cur.execute("CREATE INDEX ix_trip_distance ON yellow_taxi_trips (trip_distance);")
cur.execute("CREATE INDEX ix_trip_location ON yellow_taxi_trips (pulocationid);")
cur.execute("CREATE INDEX ix_passenger_count_fare_amount_pulocationid ON yellow_taxi_trips (passenger_count, fare_amount, pulocationid);")

#%%
pg_conn = psycopg2.connect(conn_string)
cur = pg_conn.cursor()
cur.execute("ALTER TABLE yellow_taxi_trips SET LOGGED")
cur.execute("ALTER TABLE yellow_taxi_trips ENABLE TRIGGER ALL")
cur.close()
pg_conn.commit()

#%% Test Read Spark
start_time = time.time()
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/example") \
    .option("dbtable", "yellow_taxi_trips") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()
print("spark read duration: {} seconds".format(time.time() - start_time))

#%% Test Read psycopg2
start_time = time.time()
pg_conn = psycopg2.connect(conn_string)
cur = pg_conn.cursor()
cur.execute("select count(*) from yellow_taxi_trips")
df = pd.DataFrame(cur.fetchall(),columns = [desc[0] for desc in cur.description])
cur.close()
#%%
cur = pg_conn.cursor()
cur.execute("""
    select * from yellow_taxi_trips ytt
    where trip_distance >= (
        select percentile_cont(0.9) within group (order by trip_distance) 
        from yellow_taxi_trips
    ) LIMIT 1000000
    """) #query returns 44m rows, so this limit it for testing purposes
df = pd.DataFrame(cur.fetchall(),columns = [desc[0] for desc in cur.description])
cur.close()
#%%
cur = pg_conn.cursor()
continous_aggregate_sql = """
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
    """
try:
    cur.execute(continous_aggregate_sql)
except Exception as err:
    print_psycopg2_exception(err)
cur.close()
# %%

cur = pg_conn.cursor()
cur.execute("""
    SELECT * FROM information_schema.columns 
    WHERE table_name = 'yellow_taxi_trips'
""")
df = pd.DataFrame(cur.fetchall(),columns = [desc[0] for desc in cur.description])
cur.close()
#%%