# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
FROM apache/spark-py as base

LABEL maintainer="Douglas Martins <douglas.capelossi@gmail.com>"
USER root

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN mv postgresql-42.2.5.jar /opt/spark/jars

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ADD . /opt/spark/jars

# SET SPARK ENV VARIABLES
ENV CLASSPATH /opt/spark/jars/postgresql-42.2.5.jar

# SET PYSPARK VARIABLES
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONSTARTUP=main.py

ADD /src/main.py .

WORKDIR /app

COPY . . 

EXPOSE 8000
EXPOSE 5432

HEALTHCHECK --interval=5m --timeout=3s \
  CMD curl -f http://host.docker.internal/ || exit 1

CMD ["python3", "/app/src/main.py"]

