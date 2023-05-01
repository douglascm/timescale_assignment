import os
import main
import time
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

def test_main():
    main.query("truncate table test;")
    err = None
    try:
        nyc_path = '/src/files/nyc-ytaxi-data'
        if not os.path.isdir(nyc_path): os.makedirs(nyc_path)

        # Url path
        bUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        # File prefix
        ycabPrx = "yellow_tripdata_"

        #Availaiblity of data set by month & year
        yearsDict = {}
        years = range(2022, 2023)
        months = range(8,13)

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

        taxy_table = 'sample_yellow_taxi_trips'

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
        main.query(table_create_sql)
        main.query(f"SELECT create_hypertable('{taxy_table}','tpep_pickup_datetime', if_not_exists => TRUE);")

        # Startup spark session
        spark = SparkSession.builder \
                .master("local") \
                .appName("load_parquet") \
                .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
                .getOrCreate()

        #%% For loop that donwloads all data from NYC taxis
        fnames = main.query(f"select distinct filename from {taxy_table}",mode='query').values.tolist()
        start_time_upload = time.time()

        for i, t in enumerate(zip(ycabUrls,ycabFnames)):
            #Skips Files already Inserted
            link, filename=t[0], t[1]
            if link[-13:-8] not in fnames:
                main.print(i, link, filename)
                main.save(link, nyc_path + '/' + filename)
                main.print('\n'+ nyc_path + '/' + filename)
                
                df = spark.read.parquet(nyc_path + '/' + filename)
                if 'filename' not in df.columns:
                    df = df.withColumn('filename',psf.lit(link[-13:-8]))
                
                # Function that writes to db
                main.write(taxy_table,df,spark=spark)
            
            # Removes temporary files
            for file in ['/src/files/temp.dir/yellow_taxi_trips.csv'
                        ,nyc_path + '/' + filename]:
                if os.path.isfile(file):
                    os.remove(file)
            
        print("Upload duration: {} seconds".format(time.time() - start_time_upload))

        # Creates index for assignment tasks
        main.query("CREATE INDEX IF NOT EXISTS ix_fname ON yellow_taxi_trips (filename);")
        main.query("CREATE INDEX IF NOT EXISTS ix_trip_distance ON yellow_taxi_trips (trip_distance);")
        main.query("CREATE INDEX IF NOT EXISTS ix_trip_location ON yellow_taxi_trips (pulocationid);")
        main.query("CREATE INDEX IF NOT EXISTS ix_passenger_count_fare_amount_pulocationid ON yellow_taxi_trips (passenger_count, fare_amount, pulocationid);")

        #%% Return all the trips over 0.9 percentile in the distance traveled, limiting query since amount is 40m+ lines
        df = main.query("""
            select * from yellow_taxi_trips ytt
            where trip_distance >= (
                select percentile_cont(0.9) within group (order by trip_distance) 
                from yellow_taxi_trips
            ) LIMIT 1000000
            """,method='query')

        #%% Aggregate that rolls up stats on passenger count and fare amount by pickup location. Leverages created indexes.
        main.query("""
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
    except Exception as err:
        print(err)

    assert err == None, "Something went wrong"

if __name__ == "__main__":
    test_main()
    print("Everything passed")