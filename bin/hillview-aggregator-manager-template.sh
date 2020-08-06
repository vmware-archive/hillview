#!/bin/bash
# This is teamplate for a script that manages a hillview aggregator as a Unix service.

#REPLACE_WITH_VARIABLES

FOREVERPID="forever-aggregator.pid"
PIDFILE="hillview-aggregator.pid"

cd ${SERVICE_DIRECTORY} || exit 1

start() {
    if [ "x${CLEANUP}" == "x1" ]; then
        rm -f hillview-agg.log hillview-agg.log.* hillview-agg.log*.lck
    fi
    ./forever.sh ${FOREVERPID} nohup java -ea -Dlog4j.configurationFile=./log4j.properties -server -Xmx${HEAPSIZE} -jar ${SERVICE_DIRECTORY}/hillview-server-jar-with-dependencies.jar ${SERVICE_DIRECTORY}/workers 0.0.0.0:${AGGREGATOR_PORT} >nohup.out 2>&1 &
}

killbypid() {
    local PIDFILE=$1
    if [ -f ${PIDFILE} ]; then
         # First try to find the service pid
         read LINE < ${SERVICE_DIRECTORY}/${PIDFILE}
         if [ -d "/proc/${LINE}" ]; then
             echo "Killing $2 process ${LINE}"
  	     kill ${LINE}
         fi
         rm -f ${SERVICE_DIRECTORY}/${PIDFILE}
         return 0
    else
        return 1
    fi
}

stop() {
    killbypid ${FOREVERPID} forever
    killbypid ${PIDFILE} hillview-server
}

status() {
    if [ -f ${PIDFILE} ]; then
         read LINE < ${SERVICE_DIRECTORY}/${PIDFILE}
         if [ -d "/proc/${LINE}" ]; then
             echo "Process seems to be running"
  	     return 0
         fi
    fi
    echo "Could not find running aggregator"
}

case ${1} in
        start)
                start
                ;;
        stop)
                stop
                ;;
        restart)
                stop
                start
                ;;
        status)
                status
                ;;
        *)
                echo "Usage: $0 {start|stop|restart|status}"
esac
