#!/bin/bash
# This script starts the Hillview back-end service on the local machine

mydir="$(dirname "$0")"
source $mydir/config.sh

cd ../platform/
java -server -jar target/hillview-server-jar-with-dependencies.jar 127.0.0.1:3569
