#!/bin/bash

docker-compose -f ./docker-compose-nifi.yml up & \
# git clone https://github.com/apache/superset.git &&
docker-compose -f ./superset/docker-compose-non-dev.yml up & \
sleep 10 && \ 
docker network connect superset_default postgres && \
docker network connect superset_default druid && \
docker exec -d spark-master sh /shell/setcron.sh
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar /spark/primeri/Users_Videos.py && \
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar CleanData.py && \
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar BatcSpark.py && \

# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/GEO_kafka_extend_earliest-assembly-0.1.0-SNAPSHOT.jar
# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/GEO_kafka_extend-assembly-0.1.0-SNAPSHOT.jar 
 
# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/Stream.py 
