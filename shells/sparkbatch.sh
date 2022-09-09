#! /bin/bash

cd spark/SparkBatch && \
sbt assembly && \
docker exec -d spark-master \
/spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar \
/spark/primeri/CleanData/target/scala-2.12/SparkBatch-assembly-0.1.0-SNAPSHOT.jar 