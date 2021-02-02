#!/bin/bash
# This program installs the software needed to develop Hillview

# Bail out at first error
set -e
set -x

# Set to 0 if you don't want to install cassandra locally for tests
INSTALL_CASSANDRA=1
SAVEDIR=${PWD}
mydir="$(dirname -- "$0")"
if [[ ! -d "${mydir}" ]]; then mydir="${PWD}"; fi
# shellcheck source=./lib.sh
source ${mydir}/lib.sh

echo "Installing programs needed to build"

case "${OSTYPE}" in
    linux*)
        echo "Installing curl"
        ${SUDO} ${INSTALL} install curl -y
        if [ "$(cat /etc/*-release | grep -Ec 'ubuntu|debian')" -ne 0 ]; then
            curl -sL https://deb.nodesource.com/setup_12.x | ${SUDO} -E bash -
	elif [ "$(cat /etc/*-release | grep -c -e centos -e rhel )" -ne 0 ]; then
            curl -sL https://rpm.nodesource.com/setup_12.x | ${SUDO} -E bash -
	else
	    echo "Unhandled operating system $OSTYPE"; exit 1;
	fi
        ;;
esac

${SUDO} ${INSTALL} install wget maven ${NODEJS} ${LIBFORTRAN} unzip gzip python3
echo "Installing typescript compiler"
${SUDO} npm install -g typescript@3.9.7
pip install jproperties

# Download apache if not there.
pushd ..
if [ ! -d apache-tomcat-${TOMCATVERSION} ]; then
    echo "Installing apache Tomcat web server"
    wget http://archive.apache.org/dist/tomcat/tomcat-9/v${TOMCATVERSION}/bin/apache-tomcat-${TOMCATVERSION}.tar.gz
    tar xvfz apache-tomcat-${TOMCATVERSION}.tar.gz
    cd apache-tomcat-${TOMCATVERSION}/webapps
    rm -rf ROOT* examples docs manager host-manager
    ln -s ../../web/target/web-1.0-SNAPSHOT.war ROOT.war
    cd ../..
    rm -rf apache-tomcat-${TOMCATVERSION}.tar.gz
else
    echo "Tomcat already installed"
fi
popd

# Download some test data
echo "Downloading test data"
pushd ${mydir}/../data/ontime
./download.py
popd

# Generate differentially-private metadata
pushd ${mydir}/../data/metadata/differential-privacy/data/ontime_private
./gen_metadata.py
popd

# Install geographic metadata
pushd ${mydir}/../data/geo/us_states
./download.py
popd
pushd ${mydir}/../data/geo/airports
./download.py
popd

# Install the Javascript dependencies
pushd ${mydir}/../web/src/main/webapp
echo "Installing Javascript packages"
rm -f node_modules/typescript
npm install
npm link typescript
popd

# Install Cassandra and populate a test database
if [ ${INSTALL_CASSANDRA} -eq 1 ]; then
    ./${mydir}/install-cassandra.sh
    sudo apt install mysql-server
fi
