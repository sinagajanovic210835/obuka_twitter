#! /bin/bash

cd spark/SparkUsers && \
sbt assembly && \
docker exec -d spark-master \
/spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar \
/spark/primeri/SparkUsers/target/scala-2.12/SparkUsers-assembly-0.1.0-SNAPSHOT.jar 
