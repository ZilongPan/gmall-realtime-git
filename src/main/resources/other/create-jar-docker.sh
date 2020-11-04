#!/bin/bash
mkdir -p /data/containers/$1/

if [ $# -eq 2 ]; then
  docker run --name $1 \
  --network sleepywork --ip 172.18.0.$2 \
  -u 1000:1000 \
  -v /data/containers/$1/:/data/ \
  -v /etc/localtime:/etc/localtime:ro \
  -v /etc/timezone:/etc/timezone \
  -v /etc/hosts:/etc/hosts \
  -d java:8 java -jar /data/$1.jar
fi
if [ $# -eq 3 ]; then
  docker run -p $3:$3 --name $1 \
  --network sleepywork --ip 172.18.0.$2 \
  -u 1000:1000 \
  -v /data/containers/$1/:/data/ \
  -v /etc/localtime:/etc/localtime:ro \
  -v /etc/timezone:/etc/timezone \
  -v /etc/hosts:/etc/hosts \
  -d java:8 java -jar /data/$1.jar
fi


