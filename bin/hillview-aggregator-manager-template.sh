#!/bin/bash
# This is teamplate for a script that manages a hillview aggregator as a Unix service.

#REPLACE_WITH_VARIABLES

WORK_DIRECTORY=${SERVICE_FOLDER}/hillview

start() {
    cd ${WORK_DIRECTORY}
    if [ "x${CLEANUP}" == "x1" ]; then
        rm -f hillview-agg.log hillview-agg.log.* hillview-agg.log*.lck
    fi
    nohup java -ea -Dlog4j.configurationFile=./log4j.properties -server -Xmx ${HEAPSIZE} -jar ${SERVICE_FOLDER}/hillview-server-jar-with-dependencies.jar ${SERVICE_FOLDER}/workers 0.0.0.0:${AGGREGATOR_PORT} >nohup.out 2>&1 &
    ln -sf ${WORK_DIRECTORY}/hillview-agg.log ${WORK_DIRECTORY}
}

stop() {
    if [ -f ${PIDFILE} ]; then
        # First try to find the service pid
         read LINE < ${WORK_DIRECTORY}/${PIDFILE}
         if [ -d "/proc/${LINE}" ]; then
  	     kill -KILL -u ${USER} ${LINE}
         fi
         rm -f ${WORK_DIRECTORY}/${PIDFILE}
    else
        # Kill it by name; may have collateral damage
        if pgrep -f hillview-server; then
            pkill -f hillview-server
            echo "Stopped"
        else
            echo "Already stopped"
        fi
    fi
}

status() {
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
