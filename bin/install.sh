#!/bin/bash
# Script which downloads and installs the Hillview release on the local machine
# http://github.com/vmware/hillview

mydir="$(dirname "$0")"
if [[ ! -d "${mydir}" ]]; then mydir="${PWD}"; fi
source "{$mydir}/lib.sh"

SAVEDIR=${PWD}
cd ${mydir}
source ./install-dependencies.sh -u
cd ..
wget https://github.com/vmware/hillview/releases/download/v${HILLVIEW_VERSION}/hillview-bin.zip
unzip hillview-bin.zip
cd ${SAVEDIR}