# Timescale DB data ingestion assignment

This guide explain the step by step process employed for the assignment provided. Its main objective is to import data from
[NYC “Yellow Taxi” Trips](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and push it to a timescaledb service.
The service chosen for this assignment was to use a docker image within the same project docker network. This way we can have an all-in-one
solution for the project.

## Getting Started

### Prerequisite

* Install Python
* [Install git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* Create new project folder
    * Create a new empty gitlab project [here](https://github.com/new)
    * Navigate on terminal to user/developer
    * Create a new folder and navigate into it, then run `git init --initial-branch=<default-branch>` on the terminal.
    * Show hidden files, navigate to .git folder, your config file should be:
    ```
    [core]
        repositoryformatversion = 0
        filemode = true
        bare = false
        logallrefupdates = true
        ignorecase = true
        precomposeunicode = true
	sshCommand = "ssh -i ~/.ssh/personal" <assumes you have correcly setup ssh keys on user folder>
    [remote "origin"]
	url = [YOUR PROJECT GITLAB SSH LINK]
	fetch = +refs/heads/*:refs/remotes/origin/*
    [user]
    	name = [YOUR USERNAME]
    	email = [YOUR EMAIL]
    	signingkey= [YOUR SIGN IN KEY]
    [github]
  	    user = [YOUR GITHUB USER]
    [branch "master"]
	    remote = origin
	    merge = refs/heads/master
    [branch "main"]
	    remote = origin
	    merge = refs/heads/main
    [pull]
	    rebase = false
    ```
    * Run `git clone git@github.com:douglascm/timescale_assignment.git .`
    * Run `git push -u origin <default-branch>`

## Development with vscode and devcontainer.json

We can use Dockerfile for building our development environment as well as our prod environment, but first we must install the prerequisites.

* [Install Docker](https://docs.docker.com/desktop/install/windows-install/)
* [Install vscode](https://code.visualstudio.com/download)
* [Install vscode: Remote Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

Navigate to project folder and run `code .` opening vscode. 

### Create the image locally and run
Run `docker-compose build --no-cache` and `docker-compose -p timescale_assignment up --build` on the current project folder. The project will start running. 
Run `docker run -it --entrypoint=/bin/bash timescale_assignment` to navigate files in the continer.

## Project Layout

```
.devcontainer
    devcontainer.json
src
    files
    main.py
    ___init.py
test
    files
    test_XXX.py
.dockerignore
.gitignore
build.sh
docker-compose.yml
Dockerfile
LICENSE
README.md
requirements.txt
```

A description of each noteworthy file:

* devcontainer.json: contains isntructions for building the containers
* docker-compose.yml: contains instrunctions for mounting volumes, setting up a shared network, building services (Dockerfiles, ports, dependencies and env variables). The timescale docker image is setup on the db service, it uses the `timescale/timescaledb-ha:pg14-latest` image.
* requirements.txt: contains the addon packages required for the project, read by pip on the image build step.
* Dockerfile: contains build instrucitons for docker to create a custom image with requirements (requirements.txt)
* text_XXX.py: contains the code for functionality tests, detailed below
* main.py: contains the code for the project, detailed below

## Main.py

This section explains the solution and the noteworthy decisions.

### Reading the data

A for loop is implemented in order to go through all files in the NYC taxi database. The parquet files are downloaded into the container with the `urlretrieve` function from package `urllib.request`. Once a parquet file is downloaded, spark reads the data into a spark dataframe. Spark was selected in this step due to its df.write.csv method running 4x faster than pandas df.to_csv.

### Writing into timescale db

In each of the for loops when a .csv file is created, psycopg2 can leverage the COPY functionality for one the fastest ways to push data into timescaledb, about 3-5x faster than sparks own postgresql writing capabilities. Hypertables do not support `SET UNLOGGED` and `DISABLE TRIGGER ALL` for faster table insertion.

### Setting up indexes, hypertables

After data being pushed into the database, 3 indexes are created helping speed up query results and data retrieval from the database.

```
CREATE INDEX IF NOT EXISTS ix_fname ON yellow_taxi_trips (filename); --facilitates the backfilling of data
CREATE INDEX IF NOT EXISTS ix_trip_distance ON yellow_taxi_trips (trip_distance);
CREATE INDEX IF NOT EXISTS ix_trip_location ON yellow_taxi_trips (pulocationid);
CREATE INDEX IF NOT EXISTS ix_passenger_count_fare_amount_pulocationid ON yellow_taxi_trips (passenger_count, fare_amount, pulocationid);
```

With these indexes a query can easily output all the trips over 0.9 percentile in the distance traveled for the records in the database:

```
select * from yellow_taxi_trips ytt
        where trip_distance >= (
            select percentile_cont(0.9) within group (order by trip_distance) 
            from yellow_taxi_trips
        ) LIMIT 1000000
```

The next step was to create a continuous aggregate that rolls up stats on passenger count and fare amount by pickup location. The following script creates a materalized view than can leverage the hypertables capabilities in order to do continues aggregates for specific dimensions:

```
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
```

This concludes step 1 and 2 from the assignment. As for step 3 the next section explains the architecture and the solutions

## Salesforce Architecture

> Another request we have is to upload that information to another system daily. We need a solution that allows us to push some information into our Salesforce so Sales, Finance, and Marketing can make better decisions.
>
>    1 We need to have a daily dump of the trips into Salesforce.
>
>    2 We should avoid duplicates and have a clear way to backfill if needed.

To achieve the requirement of uploading information to Salesforce on a daily basis and avoiding duplicates, I would follow these steps:

I would start by setting up a connection between the application and Salesforce. For this I would use the Salesforce API to connect the application to Salesforce. The python package [simple_salesforce](https://pypi.org/project/simple-salesforce/) can achieve this, it can query, manage records, CRUD Metadata API Calls, File Based Metadata API Calls, Upsert, Bulk, pandas resourcers, among other useful features.

Next, Airflow would be configured to extract the data from the source system and load it into Salesforce using the SalesforceHook provided by the Salesforce plugin for Airflow.

To avoid duplicates, the Salesforce upsert operation would be utilized, enabling records to be inserted or updated based on a unique external ID. A custom external ID field would be defined in Salesforce and used as the key to identify duplicate records.

A DAG would be set up in Airflow that runs on a daily basis, ensuring that data is uploaded into Salesforce every day. The dag would run a modified main.py function in order to upload only new data, but using the same custom docker image and container. The PythonOperator would be used to execute the task of extracting the data and using the SalesforceHook to load it into Salesforce.

To backfill data, I would use the Salesforce bulk API to insert or update large volumes of data in Salesforce. I would define a start and end date for the backfill period and extract the data from the source system for that period. I would then use the bulk API to insert or update the data in Salesforce.

Finally, I would set up monitoring and alerts to ensure the data is being uploaded successfully and to detect any errors or failures in the data upload process. I would use Airflow's built-in monitoring and alerting capabilities or integrate with a third-party monitoring and alerting system.

By following these steps, I would set up a solution that allows me to upload data from my source system to Salesforce on a daily basis while avoiding duplicates and providing a clear way to backfill data if needed.

## Testing

Testing is supplied for the main functionalities of the project, as well as an integration test of the main script for a sample of six months. The files are:

* test_percentile: tests the percentile query for the desired amount.
* test_query: tests the hability to connect to the timescale db and return results from the cursor.
* test_save.py: tests the download of a file from the an URL
* test_write.py: tests the method to push data into the timescale database. It pushes data into the test table, truncating it before.

## Final Notes

I started the project cloning the [getting-started](https://github.com/docker/getting-started.git) from docker to setup files faster but ended up with excessive amounts of commits and unecessary files. File which I had to remove later.

## Closing Comments

I found interesting trying different methods of read/write to improve performance and stability.




    
