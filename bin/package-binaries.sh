#!/bin/bash

# Creates a tar archive with the Hillview binaries.
# Should be run after the binaries have been built.
# This tar archive has to be unpacked in the toplevel Hillview folder.

mydir="$(dirname "$0")"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi

set -e

tar cvfz hillview-bin.taz ${mydir}/../platform/target/hillview-server-jar-with-dependencies.jar ${mydir}/../web/target/web-1.0-SNAPSHOT.war
echo "Archive hillview-bin.taz should be unpacked in toplevel hillview directory."
