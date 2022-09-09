#!/bin/bash

 docker-compose -f ./docker-compose-nifi.yml down && \
 docker-compose -f ./superset/docker-compose-non-dev.yml down && \
 rm -rf ./spark/Stream/target/ && \
 rm -rf ./spark/Stream/project/target && \
 rm -rf ./spark/Stream/project/project
