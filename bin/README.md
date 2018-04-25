# This folder contains various scripts for managing Hillview clusters

## Shell scripts for building and testing

* lib.sh: a small library of useful shell functions used by other scripts
* install-dependences.sh: Install all dependences needed to build Hillview
* rebuild.sh: build the Hillview front-end and back-end
* backend-start.sh: start the Hillview back-end service on the local machine
* frontend-start.sh: start the Hillview front-end service on the local machine
* demo-data-cleaner.sh: Downloads test data and preprocesses it
* redeploy.sh: stop services, rebuild the software, deploy it, and restart the service

## Python scripts for deploying Hillview on a cluster and managing data

* hillviewCommon.py: common library used by other python programs
* config-sample.py: sample configuration file describing a Hillview cluster installation
* upload-data.py: upload a set of files to a folder on a set of machines in a
                  round-robin fashion
* delete-data.py: delete a folder from all machines in a Hillview cluster
* run-on-all.py: run a command on a set of remote machines

* prepare.py: copy the binaries to all machines in a Hillview cluster
* start.py: start the Hillview service on a remote cluster
* stop.py: stop the Hillview service on a remote cluster
