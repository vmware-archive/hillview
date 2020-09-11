#!/bin/sh

# Creates a zip archive with the Hillview binaries.
# Should be run after the binaries have been built.
# This archive has to be unpacked in the toplevel Hillview folder.

set -ex

ARCHIVE=hillview-bin.zip
#TARARCHIVE=hillview.tar.gz

#echo "Creating ${ARCHIVE} and ${TARARCHIVE} in toplevel directory."
echo "Creating ${ARCHIVE} in toplevel directory."
cd ..

FILES="platform/target/hillview-server-jar-with-dependencies.jar web/target/web-1.0-SNAPSHOT.war platform/target/DataUpload-jar-with-dependencies.jar bin/*.py bin/*.sh bin/*.bat bin/config.json bin/config-local.json"

rm -f ${ARCHIVE}
zip ${ARCHIVE} ${FILES}

#rm -f ${TARARCHIVE}
#tar cvfz ${TARARCHIVE} ${FILES}

cd bin
