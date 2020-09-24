#!/bin/sh
# This script runs npm install

rm node_modules/typescript
npm install
npm link typescript
