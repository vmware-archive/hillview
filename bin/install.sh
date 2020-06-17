#!/bin/bash
# Script which downloads and installs the Hillview release on the local machine
# http://github.com/vmware/hillview

source ./bin/install-dependencies.sh
wget https://github.com/vmware/hillview/releases/download/v0.8-alpha/hillview-bin.zip
unzip hillview-bin.zip