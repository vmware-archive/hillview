# This folder contains various scripts and configuration files for managing Hillview clusters

## Linux/MacOS shell scripts for building and testing

* `backend-start.sh`: start the Hillview back-end service on the local machine
* `demo-data-cleaner.sh`: download a small test data and preprocesses it
* `force-gc.sh`: asks a Java process to execute GC
* `forever.sh`: runs another command in a loop forever
* `frontend-start.sh`: start the Hillview front-end service on the local machine
* `install-dependencies.sh`: install all dependencies needed to build Hillview
* `install.sh`: install the binary release of Hillview on a U*x system
* `lib.sh`: a small library of useful shell functions used by other scripts
* `package-binaries.sh`: used to build an archive with executables and scripts which
   is used for the code distribution
* `rebuild.sh`: build the Hillview front-end and back-end
* `redeploy.sh`: Performs four consecutive actions on a remote
  Hillview installation: stops the services, rebuilds the software,
  deploys it, and restarts the service
* `upload-file.sh`: Given a csv file it will guess a schema for it and
  upload it to a remote cluster chopped into small pieces.

The following are templates that are used to generate actual shell scripts
on a remoate cluster when Hillview is installed

* `hillview-aggregator-manager-template.sh`: used to generate a file
  called `hillview-aggregator-manager.sh` which can be used to start,
  stop, query a Hillview aggregation service.  The generated file is
  are installed on each aggregator machines.

* `hillview-webserver-manager-template.sh`: used to generate a file
  called `hillview-webserver-manager.sh` which can be used to start,
  stop, query a Hillview web server.  The generated file is installed
  on the remote Hillview web server machine.

* `hillview-worker-manager-template.sh`: used to generate a file
  called `hillview-worker-manager.sh` which can be used to start,
  stop, query a Hillview worker.  The generated file is installed on
  each remote worker machine.

## Windows scripts

* `install-hillview.ps1`: a PowerShell script used to download and
  install Hillview on a Windows machine.
* `detect-java.bat`: a Windows batch file which has a library that
  detects where Java is installed
* `hillview-start.bat`: a Windows batch file which starts Hillview on the local machine
* `hillview-stop.bat`: a Windows batch file which stops Hillview on the local machine

## Python scripts for deploying Hillview on a cluster and managing data

* `delete-data.py`: delete a folder from all machines in a Hillview cluster
* `deploy.py`: copy the Hillview binaries to all machines in a Hillview cluster
* `download-data.py`: downloads the specified files from all machines in a cluster
* `hillviewCommon.py`: common library used by other Python programs
* `run-on-all.py`: run a command on all machines in a Hillview cluster
* `start.py`: start the Hillview service on a remote cluster
* `status.py`: check the Hillview service on a remote cluster
* `stop.py`: stop the Hillview service on a remote cluster
* `upload-data.py`: upload a set of files to all machines in a Hillview cluster in a
   round-robin fashion

## Configuration files

* `config.json`: skeleton configuration file for a Hillview cluster
* `config-local.json`: description of a Hillview cluster that consists
  of just the local machine (used both as a web server and as a
  worker)

# Additional documentation

## Managing a Hillview cluster

* Copy the file `config.json` and modify it to describe your cluster.  Let's say you
  saved into `myconfig.json`
* To run Hillview on the local machine just use `config-local.json`
* You can install Hillview on your cluster by running `deploy.py myconfig.json`
* You can start the Hillview service on the cluster by running `start.py myconfig.json`
* You can stop the Hillview service on the cluster by running `stop.py myconfig.json`
* You can check the status of the Hillview service on the cluster by running `status.py myconfig.json`

## Managing files on a Hillview cluster

Several scripts can be used to manage data distributed as raw files on
a Hillview cluster.  The convention is that a dataset is stored in one
directory; the same directory is used on all machines, and each
machine holds a fragment of the entire dataset.

Let's say we have a very large file x.csv that we want to upload to a
cluster; we will chop it into pieces and install the pieces in the
directory "data/x" on each machine (below the hillview working
directory).  This is done with:

```
$ ./upload-file.sh -c myconfig.json -d data/x -h -f x.csv -o
```

The various flags have the following significance:
* `-c myconfig.json`: specifies cluster where data is uploaded
* `-d data/x`: specifies directory where data is uploaded on each machine
* `-h`: specifies the fact that the file `x.csv` has a header row
* `-f x.csv`: specifies the input file
* `-o`: specifies that the output should be saved as ORC files (a fast columnar format)

After uploading the file in this way it can be loaded by selecting
`Load / ORC files' and specifying:
* File name pattern: data/x/x*.orc
* Schema file: schema

Alternatively, you can split the file locally and upload the pieces
afterwards; the following splits the file into pieces in the `tmp`
directory and then uploads these pieces to the cluster using the
`upload-data.py` program:

```
$ ./upload-file.sh -d tmp -h -f x.csv -o
$ ./upload-data.py -d data/x -s schema mycluster.json tmp/*.orc
```

To list the files on the cluster you can use the `run-on-all.py` script, e.g.:

```
$ ./run-on-all.py mycluster.json "ls -l data/x"
```

You can delete a directory from all machines of a cluster:

```
$ ./delete-data.py mycluster.json data/x
```

Finally, you can download back data you have uploaded to the cluster:

```
$ ./download-data.py mycluster.json data/x
```

When downloading the files this utility will create locally a folder
for each machine in the cluster.