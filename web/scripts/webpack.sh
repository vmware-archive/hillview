#!/bin/bash

set -e

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
source $mydir/../../bin/lib.sh

# Helper script to run webpack from maven

pushd src/main/webapp/
#echo "Installing npm packages"
#npm install
rm -rf bundle*
echo "Running webpack command in src/main/webapp"
./node_modules/.bin/webpack --display-modules
ln -sf dist/bundle.js .
ln -sf dist/bundle.js.map .
popd
