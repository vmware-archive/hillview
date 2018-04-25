#!/bin/sh
# Compile and deploy program

# Bail out at first error
set -e

usage() {
    echo "redeploy.sh [-shp] config.py"
    echo "Rebuild and redeploy program on a cluster"
    echo "-p: skip deploying software, just stop and start"
    echo "-s: skip rebuilding, just redeploy"
    echo "-h: help"
    exit 1
}

SKIP=""
NOREDEPLOY=""
while getopts sph FLAG; do
   case $FLAG in
      s) SKIP="1"
         ;;
      p) NOREDEPLOY="1"
         ;;
      *) usage
         ;;
   esac
done
shift $OPTIND-1

if [ $* -ne 0 ]; then
    usage()
fi

CONFIG=$0

if [ -z $SKIP ]; then
    cd ../platform
    mvn -DskipTets install
    cd ../web
    mvn package
    cd ../deployment
fi

echo "Stopping services"
stop.py $CONFIG
if [ -z $NOREDEPLOY ]; then
    echo "Installing software"
    deploy.py $CONFIG
fi
echo "Starting services"
start.py $CONFIG
