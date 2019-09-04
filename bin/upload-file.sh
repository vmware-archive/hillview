#!/bin/bash
# Invokes a Java program that splits a file into pieces and uploads them to a cluster

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi

java -jar ${mydir}/../platform/target/DataUpload-jar-with-dependencies.jar $*
