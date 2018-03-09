#!/bin/sh
# Prepares some files for deployment

ONECLOUD=""
while getopts soh FLAG; do
   case $FLAG in
      o) ONECLOUD="1";
         ;;
      h) echo "prepare.sh [-osh]"
         echo "Prepare a program for deployment on a cluster"
         echo "-o: prepare to redeploy on OneCloud, else on fast cluster"
         echo "-h: help"
         exit 1
         ;;
   esac
done

if [ -z $ONECLOUD ]; then
    ln -sf config-fast.yaml config.yaml
else
    ln -sf config-onecloud.yaml config.yaml
fi
