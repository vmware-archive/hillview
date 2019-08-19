#!/bin/bash
# This is teamplate for a script that manages a hillview web server as a service.

#REPLACE_WITH_VARIABLES

# File storing PID of the hillview worker
PIDFILE="hillview-webserevr.pid"
cd ${SERVICE_DIRECTORY}

start() {
    if [ "x${CLEANUP}" == "x1" ]; then
        rm -f hillview-web.log hillview-web.log.* hillview-web.log*.lck
    fi
    export WEB_CLUSTER_DESCRIPTOR=serverlist
    nohup ./${TOMCAT}/bin/startup.sh &
}

stop() {
    if pgrep -f tomcat; then
        ${SERVICE_DIRECTORY}"/"${TOMCAT}/bin/shutdown.sh
        echo Stopped
    else
        echo "Web server already stopped"
    fi
    pkill -f Bootstrap
    true
}

status() {
    if ! pgrep -f tomcat; then
        echo "Web server not running"
    fi
    true
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
