#! /bin/bash

echo -e "50\t*\t*\t*\t*\t/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars /driver/postgresql-42.3.5.jar /spark/primeri/GEO_kafka_extend-assembly-0.1.0-SNAPSHOT.jar" >> /var/spool/cron/crontabs/root && \
# echo -e "*/1\t*\t*\t*\t*\techo 'sinisa' >> /spark/sinissssa.txt" >> /var/spool/cron/crontabs/root && \
crond start

# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar /spark/primeri/Users_Videos.py && \
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar CleanData.py && \
# docker exec -it spark-master spark/bin/spark-submit --jars /driver/postgresql-42.3.5.jar BatcSpark.py && \

# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/GEO_kafka_extend_earliest-assembly-0.1.0-SNAPSHOT.jar
# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/GEO_kafka_extend-assembly-0.1.0-SNAPSHOT.jar 
 
# docker exec -it spark-master spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  --jars \
#  /driver/postgresql-42.3.5.jar /spark/primeri/Stream.py 
