#!/bin/bash
# This script starts the Hillview back-end service on the local machine

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source $mydir/lib.sh

# If you want GRPC logging uncomment the following line
# This will make the hillviedw logs unparsable
#LOGGING=-Djava.util.logging.config.file=logging.properties

cd $mydir/../platform/
java $LOGGING -server -jar target/hillview-server-jar-with-dependencies.jar 127.0.0.1:3569
