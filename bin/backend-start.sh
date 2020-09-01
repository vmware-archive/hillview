#!/bin/bash
# This script starts the Hillview back-end service on the local machine

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi

# If you want GRPC logging uncomment the following line
# LOGGING="-Djava.util.logging.config.file=logging.properties"

# Uncomment this to debug classloader issues
# DEBUG_CLASSLOADER="-verbose:class"

cd ${mydir}/.. || exit 1
java ${LOGGING} ${DEBUG_CLASSLOADER} -server -jar platform/target/hillview-server-jar-with-dependencies.jar 127.0.0.1:3569
