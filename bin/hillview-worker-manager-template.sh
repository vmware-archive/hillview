#!/bin/bash
# This is teamplate for a script that manages a hillview worker as a service.

#REPLACE_WITH_VARIABLES

# File storing PID of the hillview worker
PIDFILE="hillview-worker.pid"
# File storing PID of the forever process that runs the hillview worker
FOREVERPID="forever-worker.pid"

cd ${SERVICE_DIRECTORY}

start() {
    echo "Starting worker"
    if [ "x${CLEANUP}" == "x1" ]; then
        rm -f hillview.log hillview.log.* hillview.log*.lck
    fi
    ./forever.sh ${FOREVERPID} nohup java -ea -Dlog4j.configurationFile=./log4j.properties -server -Xmx${HEAPSIZE} -jar ${SERVICE_DIRECTORY}/hillview-server-jar-with-dependencies.jar 0.0.0.0:${WORKER_PORT} >nohup.out 2>&1 &
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
    if [ $? -ne 0 ]; then
        # Kill it by name; may have collateral damage
        if pgrep -f hillview-server; then
            pkill -f hillview-server
            echo "Stopped"
        else
            echo "Already stopped"
        fi
    fi
    true
}

status() {
    if [ -f ${PIDFILE} ]; then
         read LINE < ${PIDFILE}
         if [ -d "/proc/${LINE}" ]; then
             echo "Process seems to be running"
             return 0
         fi
    fi
    echo "Could not find running worker"
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
