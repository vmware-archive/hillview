#!/bin/bash

if [[ ! -v JAVA_HOME ]]
then
  exit
fi

SPARK_WORKER_INSTANCES=3
SPARK_ROOT="spark-2.0.0-bin-hadoop2.7"
WORKER_RUN_PARAMS="SPARK_WORKER_INSTANCES=$SPARK_WORKER_INSTANCES"
SPARK_MASTER_URL="spark://`hostname`:7077"

#
# First check if there are any instances running and shut them down.
# 
# XXX: this will be inconsistent across runs if you specify different number
# of worker instances each time. In that case, start_services will complain.
# In which case, run "jps" and kill the processes manually with "kill -9".
#
function stop_services {
  HADOOP_SSH_OPTS="-i ~/.ssh/hdfs_localhost" bash hadoop-2.7.3/sbin/stop-dfs.sh
  ${SPARK_ROOT}/sbin/stop-master.sh
  env SPARK_WORKER_INSTANCES=$SPARK_WORKER_INSTANCES ${SPARK_ROOT}/sbin/stop-slave.sh $SPARK_MASTER_URL
}

#
# Start a master and SPARK_WORKER_INSTANCES number of workers.
#
function start_services {
  HADOOP_SSH_OPTS="-i ~/.ssh/hdfs_localhost" bash hadoop-2.7.3/sbin/start-dfs.sh
  ${SPARK_ROOT}/sbin/start-master.sh
  env SPARK_WORKER_INSTANCES=$SPARK_WORKER_INSTANCES ${SPARK_ROOT}/sbin/start-slave.sh $SPARK_MASTER_URL
}

#
# Start a master and SPARK_WORKER_INSTANCES number of workers.
#
function restart_services {
  stop_services
  start_services
}


[ "$#" -eq 1 ] || die "1 argument required, $# provided"

if [ $1 == "start" ]
then
  start_services
elif [ $1 == "stop" ]
then
  stop_services
elif [ $1 == "restart" ]
then
  restart_services
else
  echo "Invalid argument to services.sh. Available commands are."
  echo "services.sh <start | stop | restart>"
fi
