#!/bin/bash
# This script starts the Hillview back-end service on the local machine

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source $mydir/config.sh


cd $mydir/../platform/
java -server -jar target/hillview-server-jar-with-dependencies.jar 127.0.0.1:3569
