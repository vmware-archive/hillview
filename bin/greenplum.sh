#!/bin/bash
# Deploy hillview on the test greenplum cluster

ROOT="104.196.238.49"
USER="gpadmin"
# home directory of hillview
HILLVIEW=hillview

set -ex

./rebuild.sh
./package-binaries.sh
scp ../hillview-bin.zip ${USER}@${ROOT}:
scp config-greenplum.json ${USER}@${ROOT}:
ssh ${USER}@${ROOT} unzip -o hillview-bin.zip
ssh ${USER}@${ROOT} "cd bin; ./upload-data.py -d . -s dump-greenplum.sh config-greenplum.json"
ssh ${USER}@${ROOT} "cd bin; ./redeploy.sh -s config-greenplum.json"
scp ../repository/*.jar ${USER}@${ROOT}:${HILLVIEW}/apache-tomcat-9.0.4/lib
