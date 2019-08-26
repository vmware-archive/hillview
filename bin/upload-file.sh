#!/bin/sh
# Invokes a Java program that splits a file into pieces and uploads them to a cluster

java -jar ../platform/target/DataUpload-jar-with-dependencies.jar $*
