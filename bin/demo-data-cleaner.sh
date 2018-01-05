#!/bin/bash

set -e

export MAVEN_OPTS="-Xmx2048M"
pushd ../data/ontime
./download.sh
popd
pushd ../platform
java -jar target/data-cleaner-jar-with-dependencies.jar
popd
