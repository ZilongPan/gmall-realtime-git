#!/bin/bash
case $1 in
"start")
  zk.sh start
  start-dfs.sh
  start-hbase.sh
  docker restart mysql
  docker restart clickhouse
  docker restart redis
  docker restart es
  kafka.sh start
  docker restart maxwell
  ;;
"stop")
  docker stop maxwell
  kafka.sh stop
  docker stop mysql
  docker stop clickhouse
  docker stop redis
  docker stop es
   stop-hbase.sh
   stop-dfs.sh
  
  echo "sleppy 30s second after close zookeeper"
  sleep 30
  zk.sh stop
  ;;
"ps")
  xcall.sh docker ps
  xcall.sh jps
  ;;
*)
  echo "args is error"
  ;;
esac
