#!/bin/bash
# This scripts starts the Hillview front-end service locally

mydir="$(dirname "$0")"
source $mydir/config.sh

# If you add this line the java process enables a remote debugger to be connected to it
# on port 5005
#export JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
../apache-tomcat-${TOMCATVERSION}/bin/catalina.sh run
