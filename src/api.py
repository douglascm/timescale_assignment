import urllib.request as urllib
import os
import time
import sys
import time
import logging
import psycopg2
import pandas as pd
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from functions import delete_indexes, save, write, query, ask_range
from flask import Flask, request, render_template, jsonify, Response, stream_with_context
from loguru import logger

app = Flask(__name__)
logger.add("job.log", format="{time} - {message}")

def flask_logger():
    with open("job.log") as log_info:
        data = log_info.read()
        yield data.encode()
        time.slee(1)

def run_code(start_year,end_year,del_index,write_db_method):
    #%% Configure logger

    nyc_path = '/src/files/nyc-ytaxi-data'
    if not os.path.isdir(nyc_path): os.makedirs(nyc_path)

    logger.info('Welcome to the NYC yellow taxi data import toll')
    logger.info(f'\nPulling data from Range {start_year}-{end_year}...')
    
    years = range(int(start_year), int(end_year)+1)

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
    logger.info(f'Creating {taxy_table} on test database')
    query(table_create_sql)
    logger.info(f'Creating {taxy_table} hypertable on test database')
    query(f"SELECT create_hypertable('{taxy_table}','tpep_pickup_datetime', if_not_exists => TRUE);")

    if del_index=='Yes':
        delete_indexes(taxy_table)

    # Startup spark session
    spark = SparkSession.builder \
            .master("local") \
            .appName("load_parquet") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
            .getOrCreate()

    logger.info(f'Querying files already imported into db...')
    fnames = query(f"select distinct filename from {taxy_table}",mode='query').filename.values
    logger.info(f'Files already imported into db:')
    logger.info(f'{fnames}')

    start_time_upload = time.time()

    #%% For loop that donwloads all data from NYC taxis
    for i, t in enumerate(zip(ycabUrls,ycabFnames)):
        #Skips Files already Inserted
        link, filename=t[0], t[1]
        if link[-13:-8] not in fnames or write_db_method=='Replace':
            logger.info('Downloading file ' + str(i) + '-' + link + ' - into file ' + filename + '...')
            if save(link, nyc_path + '/' + filename):
                print(nyc_path + '/' + filename)
                
                df = spark.read.parquet(nyc_path + '/' + filename)
                if 'filename' not in df.columns:
                    df = df.withColumn('filename',psf.lit(link[-13:-8]))
                    
                if link[-13:-8] in fnames and write_db_method=='Replace':
                    logger.info('Deleting data from file ['+ link[-13:-8] + '] from the database...')
                    query(f"delete from {taxy_table} where filename='{link[-13:-8]}';")
                    
                # Function that writes to db
                write(taxy_table,df,spark=spark,logging=True)
            else:
                logger.info(f'{link} File not found, moving to next on the list...')
        else:
            logger.info(f'{link} already imported into database, moving to next on the list...')
        
        # Removes temporary files
        for file in ['/src/files/temp.dir/yellow_taxi_trips.csv',nyc_path + '/' + filename]:
            if os.path.isfile(file):
                os.remove(file)
        
    logger.info("Upload duration: {} seconds".format(time.time() - start_time_upload))

    #%% Creates index for assignment tasks, after bulk loading
    logger.info(f'Creating index ix_fname on table test.{taxy_table}')
    query(f"CREATE INDEX IF NOT EXISTS ix_fname ON {taxy_table} (filename);")
    logger.info(f'Creating index ix_trip_distance on table test.{taxy_table}')
    query(f"CREATE INDEX IF NOT EXISTS ix_trip_distance ON {taxy_table} (trip_distance);")
    logger.info(f'Creating index ix_fnix_trip_locationame on table test.{taxy_table}')
    query(f"CREATE INDEX IF NOT EXISTS ix_trip_location ON {taxy_table} (pulocationid);")
    logger.info(f'Creating index ix_passenger_count_fare_amount_pulocationid on table test.{taxy_table}')
    query(f"CREATE INDEX IF NOT EXISTS ix_passenger_count_fare_amount_pulocationid ON {taxy_table} (passenger_count, fare_amount, pulocationid);")

    #%% Aggregate that rolls up stats on passenger count and fare amount by pickup location. Leverages created indexes.
    logger.info(f'Creating MATERIALIZED view test.{taxy_table}_pickup_loc on table test.{taxy_table}')
    query(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {taxy_table}_pickup_loc
        WITH (timescaledb.continuous) AS
        SELECT
            pulocationid,
            time_bucket(INTERVAL '1 day', tpep_pickup_datetime) as bucket,
            sum(passenger_count) as sum_pax,
            max(passenger_count) AS high_pax,
            sum(fare_amount) as sum_fare,
            max(fare_amount) AS max_fare,
            min(fare_amount) AS low_fare
        FROM {taxy_table} ytt
        GROUP BY pulocationid, bucket WITH DATA;
        """,autocommit=True)
    
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        with open('job.log', 'w'):
            pass

        # Retrieve user inputs from form submission
        input_1 = request.form['input_1']
        input_2 = request.form['input_2']
        input_3 = request.form.get('input_3')
        input_4 = request.form.get('input_4')
        
        # Call function to run code with user inputs
        run_code(input_1, input_2, input_3, input_4)
        success='ETL Ran succesfully'
        # Return output to user
        return render_template('index.html', etl_success=success)
    else:
        # Render form for user inputs
        return render_template('index.html')

@app.route('/table')
def showData():
    try:
        # Return all the trips over 0.9 percentile in the distance traveled, limiting query since amount is 40m+ lines for the entire dataset
        logger.info(f'Return all the trips over 0.9 percentile in the distance traveled')
        df = query("""
        select * from yellow_taxi_trips ytt
        where trip_distance >= (
            select percentile_cont(0.9) within group (order by trip_distance) 
            from yellow_taxi_trips
        ) LIMIT 1000
        """,mode='query')
        logger.info(df.head(50).to_string(max_cols=5))
        return render_template('table.html',data_var=df.to_html())
    except:
        error='Database not found... Run ETL first'
        return render_template('index.html',query_error=error)

@app.route("/log_stream", methods=["GET"])
def log_stream():
    """returns logging information"""
    return Response(flask_logger(), mimetype="text/plain", content_type="text/event-stream")
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)