#!/bin/bash

# git clone https://github.com/apache/superset.git 
MIN=$2
HOUR=$3
DAY=$4
MONTH=$5
WEEKDAY=$6

if [ -z $MIN -o $MIN = "x" ] 
then
    MIN="x"
fi
if [ -z $HOUR -o $HOUR = "x" ] 
then
    HOUR="x"
fi
if [ -z $DAY -o $DAY = "x" ] 
then    DAY="x"
fi
if [ -z $MONTH -o $MONTH = "x"  ] 
then
    MONTH="x"    
fi
if [ -z $WEEKDAY -o $WEEKDAY = "x" ] 
then
    WEEKDAY="x"
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
