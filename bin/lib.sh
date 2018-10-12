#!/bin/bash
# Part of the Hillview configuration
# This is a shell script which only defines some variables used by other scripts.

TOMCATVERSION="9.0.4"

# Detect operating system
# Sets "INSTALL" to the program that can install software
# Sets "SUDO" to whatever necessary to install software with enough privileges
# Sets various variables to the names of the software packages needed

case "$OSTYPE" in
    linux*) INSTALL="apt-get"; SUDO="sudo"; NODEJS="nodejs-legacy"; NPM="npm" ;;
    darwin*) INSTALL="brew"; SUDO=""; NODEJS="node"; NPM="" ;;
    *) echo "Unhandled operating system $OSTYPE"; exit 1;;
esac
