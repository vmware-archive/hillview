# Part of the Hillview configuration
# This is a shell script which only defines some variables used by other scripts.

TOMCATVERSION="9.0.4"

# Detect operating system
# Sets "INSTALL" to the program that can install software
# Sets "sudo" to whatever necessary to install software with enough priviledges

case "$OSTYPE" in
    linux*) INSTALL="apt-get"; SUDO="sudo" ;;
    darwin*) INSTALL="brew"; SUDO="" ;;
    *) echo "Unhandled operating system $OSTYPE"; exit 1;;
esac
