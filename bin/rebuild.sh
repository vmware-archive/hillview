#!/bin/sh
# A small shell script which rebuilds both projects that compose Hillview

cd ../platform
mvn install
cd ../web
mvn package
