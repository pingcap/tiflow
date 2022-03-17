#!/bin/bash

docker-compose -f $1 up -d --force-recreate
