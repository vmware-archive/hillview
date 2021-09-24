#!/bin/bash
# Part of the Hillview configuration for developers
# This is a shell script which only defines some variables used by other scripts.

TOMCATVERSION="9.0.4"

# Detect operating system
# Sets "INSTALL" to the program that can install software
# Sets "SUDO" to whatever necessary to install software with enough privileges
# Sets various variables to the names of the software packages needed

case "$OSTYPE" in
    linux*)
        LIBFORTRAN="libgfortran5"
        if [ "$(cat /etc/*-release | grep -Ec 'ubuntu|debian')" -ne 0 ]; then
            # Npm will be installed with node.js
            INSTALL="apt-get"; SUDO="sudo"; NODEJS="nodejs";
        elif [ "$(cat /etc/*-release | grep -Ec 'centos|rhel' )" -ne 0 ]; then
            INSTALL="yum"; SUDO="sudo"; NODEJS="nodejs";
        else
            echo "Unhandled operating system $OSTYPE"; exit 1;
        fi
        ;;
    darwin*) INSTALL="brew"; SUDO=""; NODEJS="node"; LIBFORTRAN=""; ;;
    *) echo "Unhandled operating system $OSTYPE"; exit 1; ;;
esac
