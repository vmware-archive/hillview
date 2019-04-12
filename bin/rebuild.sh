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
source $mydir/lib.sh

# Bail out on first error; verbose
set -e

SKIP="1"
TOOLS="0"
while getopts shta FLAG; do
   case $FLAG in
      s) SKIP="1"
         ;;
      a) TOOLS="1"
         ;;
      t) SKIP="0"
         ;;
      *) usage
         ;;
   esac
done


export MAVEN_OPTS="-Xmx2048M"
EXTRAARGS=""
if [ $SKIP -eq "1" ]; then
    EXTRAARGS="-DskipTests "$EXTRAARGS
fi
if [ $TOOLS -eq "1" ]; then
    EXTRAARGS="-P tools "$EXTRAARGS
fi
pushd $mydir/../platform
mvn $EXTRAARGS clean install
popd
pushd $mydir/../web
mvn clean package
popd
