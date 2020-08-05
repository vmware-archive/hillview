#!/bin/bash
# A small shell script which rebuilds both projects that compose Hillview

usage() {
    echo "rebuild.sh [-s][-h][-a]"
    echo "Rebuild and the program"
    echo "-s: skip tests (default)"
    echo "-t: run tests"
    echo "-h: help"
    echo "-a: build all jars, including various tools"
    exit 1
}

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
# shellcheck source=./lib.sh
source ${mydir}/lib.sh

# Bail out on first error; verbose
set -ex

TESTARGS="-DskipTests"
TOOLSARGS=""
while getopts shta FLAG; do
   case ${FLAG} in
      s) TESTARGS="-DskipTests"
         echo "Skipping tests"
         ;;
      a) TOOLSARGS="-P tools"
         echo "Building extra tools"
         ;;
      t) TESTARGS=""
         echo "Running tests"
         ;;
      *) usage
         ;;
   esac
done

if [ "x${TOOLSARGS}" != "x" ]; then
   pushd ${mydir}/../cassandra-shaded
   mvn clean package
   mvn install:install-file -DcreateChecksum=true -Dfile=./target/shaded-cassandra-all.jar -DgroupId=org.hillview  -DartifactId=shaded-cassandra-all -Dversion=1.0 -Dpackaging=jar
   popd
fi
export MAVEN_OPTS="-Xmx2048M"
pushd ${mydir}/../platform
mvn ${TOOLSARGS} ${TESTARGS} clean install
popd
pushd ${mydir}/../web
mvn ${TESTARGS} clean package
popd
