#!/bin/bash

 docker-compose -f ./docker-compose-nifi.yml down && \
 docker-compose -f ./superset/docker-compose-non-dev.yml down && \
 rm -rf ./spark/*/target/ && \
 rm -rf ./spark/*/project/target && \
 rm -rf ./spark/*/project/project
