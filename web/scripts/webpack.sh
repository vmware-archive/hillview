#!/bin/bash

set -e

# Helper script to run webpack from maven

pushd src/main/webapp/
echo "Installing npm packages"
npm install
npm install --only=dev
rm -rf bundle*
echo "Running webpack command in src/main/webapp"
webpack --display-modules
popd
