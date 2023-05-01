# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
FROM apache/spark-py

LABEL maintainer="Douglas Martins <douglas.capelossi@gmail.com>"
USER root

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN mv postgresql-42.2.5.jar /opt/spark/jars

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ADD . /opt/spark/jars

ENV CLASSPATH /opt/spark/jars/postgresql-42.2.5.jar

WORKDIR /app

COPY /app /app
COPY /test /test

EXPOSE 5432
