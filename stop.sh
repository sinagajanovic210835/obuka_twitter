#!/bin/bash

 docker-compose -f ./docker-compose-nifi.yml down && \
 docker-compose -f ./superset/docker-compose-non-dev.yml down
