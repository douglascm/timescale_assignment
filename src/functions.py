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
from loguru import logger

def delete_indexes(table_name):
    query(f"""
DO
$do$
DECLARE
   _sql text;
BEGIN   
   SELECT 'DROP INDEX ' || string_agg(indexrelid::regclass::text, ', ')
   FROM   pg_index  i
   LEFT   JOIN pg_depend d ON d.objid = i.indexrelid
                          AND d.deptype = 'i'
   WHERE  i.indrelid = '{table_name}'::regclass  
   AND    d.objid IS NULL                      
   INTO   _sql;
   
   IF _sql IS NOT NULL THEN
     EXECUTE _sql;
   END IF;
END
$do$;""",autocommit=True)

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
    try:
        con = psycopg2.connect(conn_string)
        cur = con.cursor()
        with open(fileName, 'r') as f:
            cur.copy_from(f, table_name, sep=',', null='')    
        con.commit()
        cur.close()
        con.close()
        return True
    except:
        print('Query failed...')
        return False
    
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
    try:
        urllib.urlretrieve(url, filename, reporthook)
        return True
    except:
        return False  

def parquet2csv(df,path,filename):
    # Spark write method is 5x faster than pd.to_csv
    df.coalesce(1).write.options(header='False',delimeter=',').mode("overwrite").csv(path)
    listFiles = os.listdir(f'{path}') 
    for subFiles in listFiles:
        if subFiles[-4:] == ".csv":
            os.rename(path + subFiles,  f'{path}{filename}')
    return print(f'{filename} saved into folder{path}')

def write(table,df,write_method='psycopg2',spark=None,logging=False):
    try:
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
            if logging: logger.info("spark write csv duration: {} seconds".format(time.time() - start_time))
            print("spark write csv duration: {} seconds".format(time.time() - start_time))
            
            start_time = time.time()
            # With Postgresql COPY command and psycopg2 data is pushed into database table yellow_taxi_trips in conn_string
            execute_copy('/src/files/temp.dir/yellow_taxi_trips.csv',table,os.environ.get('PSYCOPG2_JDBC_URL'))
            if logging: logger.info("psycopg2 COPY duration: {} seconds".format(time.time() - start_time))
            print("psycopg2 COPY duration: {} seconds".format(time.time() - start_time))
        else: 
            print('Invalid write method.')
        return 'Success'
    except Exception as err:
        print(str(err))
        return 'Failure'

def query(sql,mode='execute',autocommit=False):
    pg_conn = psycopg2.connect(os.environ.get('PSYCOPG2_JDBC_URL'))
    pg_conn.autocommit = autocommit
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
    pg_conn.close()
    return df

def ask_range():
    start_year = int(input("Enter the desired year to start collecting data:"))
    if start_year not in range(2013,2024):
        print('Year outside range (2013-2023), please try again')
        try:
            start_year = int(input("Enter the desired year to start collecting data:"))
        except:
            print('Not a number')
    end_year = int(input("Enter the desired year to end collecting data:"))
    if end_year not in range(2013,2024):
        print('Year outside range (2013-2023), please try again')
        try:
            end_year = int(input("Enter the desired year to end collecting data:"))
        except:
            print('Not a number')
    elif end_year<start_year:
        print('End year preceeds start year, please try again')
        end_year = input("Enter the desired year to end collecting data:")
    
    return start_year, end_year