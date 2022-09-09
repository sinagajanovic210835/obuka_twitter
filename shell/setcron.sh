#! /bin/bash

MIN=$1
HOUR=$2
DAY=$3
MONTH=$4
WEEKDAY=$5
if [ $MIN = "x" ] 
then
    MIN="*"
fi
if [ $HOUR = "x" ] 
then
    HOUR="*"
fi
if [ $DAY = "x" ] 
then    DAY="*"
fi
if [ $MONTH = "x"  ] 
then
    MONTH="*"    
fi
if [ $WEEKDAY = "x" ] 
then
    WEEKDAY="*"
fi
echo -e "$MIN\t$HOUR\t$DAY\t$MONTH\t$WEEKDAY\t/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars /driver/postgresql-42.3.5.jar /spark/primeri/Stream/target/scala-2.12/GEO_kafka_extend-assembly-0.1.0-SNAPSHOT.jar" >> /var/spool/cron/crontabs/root
if [ $? == 0 ]
then
    crond start
fi
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar /spark/primeri/Users_Videos.py && \
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar CleanData.py && \
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar BatcSpark.py && \

# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/GEO_kafka_extend_earliest-assembly-0.1.0-SNAPSHOT.jar
# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/GEO_kafka_extend-assembly-0.1.0-SNAPSHOT.jar 
 
# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/Stream.py 
