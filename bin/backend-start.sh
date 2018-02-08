#!/bin/bash
# This script starts the Hillview back-end service on the local machine

source ./config.sh

cd ../platform/
java -server -jar target/hillview-server-jar-with-dependencies.jar 127.0.0.1:3569
