#!/bin/bash
# This is teamplate for a script that manages a hillview web server as a service.

#REPLACE_WITH_VARIABLES

cd ${SERVICE_DIRECTORY} || exit 1

start() {
    if [ "x${CLEANUP}" == "x1" ]; then
        rm -f hillview-web.log hillview-web.log.* hillview-web.log*.lck
    fi
    export WEB_CLUSTER_DESCRIPTOR=serverlist
    export CATALINA_PID=catalina.pid
    ./${TOMCAT}/bin/startup.sh
}

stop() {
    export CATALINA_PID=catalina.pid
    if pgrep -f tomcat; then
        ${SERVICE_DIRECTORY}"/"${TOMCAT}/bin/shutdown.sh -force
        echo Stopped
    else
        echo "Web server already stopped"
    fi
    pkill -f Bootstrap
    true
}

status() {
    # This assumes there's a single tomcat on the machine, which may not be true...
    if ! pgrep -f tomcat; then
        echo "Web server not running"
    else
        echo "Web server running"
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
