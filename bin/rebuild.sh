#!/bin/bash
# A small shell script which rebuilds both projects that compose Hillview


usage() {
    echo "rebuild.sh [-s]"
    echo "Rebuild and the program"
    echo "-s: skip tests"
    echo "-h: help"
    exit 1
}

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source $mydir/config.sh

# Bail out on first error; verbose
set -ex

SKIP="0"
while getopts sh FLAG; do
   case $FLAG in
      s) SKIP="1"
         ;;
      *) usage
         ;;
   esac
done


export MAVEN_OPTS="-Xmx2048M"
EXTRAARGS=""
if [ $SKIP -eq "1" ]; then
    EXTRAARGS="-Dskiptests"
fi

pushd $mydir/../platform
mvn $EXTRAARGS install
popd
pushd $mydir/../web
mvn $EXTRAARGS clean package
popd
