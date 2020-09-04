#!/bin/bash
# Script which downloads and installs the Hillview release on the local machine
# http://github.com/vmware/hillview

HILLVIEW_VERSION="0.9-alpha"

mydir="$(dirname -- "$0")"
if [[ ! -d "${mydir}" ]]; then mydir="${PWD}"; fi

SAVEDIR=${PWD}
cd ${mydir} || exit 1

cd ..
wget https://github.com/vmware/hillview/releases/download/v${HILLVIEW_VERSION}/hillview-bin.zip
unzip -o hillview-bin.zip
cd ${SAVEDIR} || exit 1
