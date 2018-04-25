#!/bin/bash
# This scripts starts the Hillview front-end service locally

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source $mydir/lib.sh

# If you add this line the java process enables a remote debugger to be connected to it
# on port 5005
#export JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
$mydir/../apache-tomcat-${TOMCATVERSION}/bin/catalina.sh run
