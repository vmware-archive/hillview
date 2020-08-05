#!/bin/bash
# This scripts starts the Hillview front-end service locally

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source ${mydir}/lib.sh

# If you add this line the java process enables a remote debugger to be connected to it
# on port 5005
export JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

# If you want GRPC logging uncomment the following line.
# LOGGING=" -Djava.util.logging.config.file=logging.properties"
export JAVA_OPTS="$JAVA_OPTS$LOGGING"
export CATALINA_PID="catalina.pid"
cd ${mydir}/.. || exit 1
./apache-tomcat-${TOMCATVERSION}/bin/catalina.sh run
