#!/bin/bash

# git clone https://github.com/apache/superset.git 
MIN=$2
HOUR=$3
DAY=$4
MONTH=$5
WEEKDAY=$6
ARGS=`expr $# - 1`

if [ $ARGS -lt 5 ]
then
    WEEKDAY="x"
fi

if [ $ARGS -lt 4 ]
then
    MONTH="x"
fi

if [ $ARGS -lt 3 ]
then
    DAY="x"
fi

if [ $ARGS -lt 2 ]
then
    HOUR="x"
fi

if [ $ARGS -lt 1 ]
then
    MIN="x"        
fi

docker-compose -f ./docker-compose-nifi.yml up & docker-compose -f ./superset/docker-compose-non-dev.yml up & \
sleep 10
docker network connect superset_default postgres
docker network connect superset_default druid
SUCC=$?
if [ $SUCC == 0 -a $1 = "st" ] 
then
    STR="$MIN $HOUR $DAY $MONTH $WEEKDAY"
    docker exec -d spark-master sh /shell/setcron.sh $STR       
fi    
