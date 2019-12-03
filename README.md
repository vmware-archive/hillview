![Hillview project logo](hillview-logo.png)

# Introduction

Hillview: a big data spreadsheet.  Hillview is a cloud-based
application for browsing large datasets.  The hillview user interface
executes in a browser.  Currently the software is alpha quality, under
active development.

# Documentation

There is a [Hillview user manual](docs/userManual.md).

A [short video](https://1drv.ms/v/s!AlywK8G1COQ_jeRQatBqla3tvgk4FQ)
shows the system in action in real-time.

You can [test](http://ec2-18-217-136-170.us-east-2.compute.amazonaws.com:8080/)
a demo of the system running on 15 small Amazon machines.

A [paper](https://arxiv.org/abs/1907.04827) describing the system in
some detail.  This is an extended version of the following publication
Mihai Budiu, Parikshit Gopalan, Lalith Suresh, Udi Wieder, Han
Kruiger, and Marcos K. Aguilera, Hillview: A trillion-cell spreadsheet
for big data, in PVLDB 2019, 12(11).

Documentation for the [internal APIs](docs/hillview-apis.pdf).

# Installing Hillview on a local machine

## Ubuntu or MacOS machines.

* Install Java 8.  At this point newer versions of Java will *not* work.
* clone this github repository
* run the script `bin/install-dependencies.sh`
* Download the Hillview release [zip
file](https://github.com/vmware/hillview/releases/download/v0.7-alpha/hillview-bin.zip).
  Save it in the top directory of Hillview.
* Unzip the release `unzip hillview-bin.zip`

## Windows machines

* Download and install Java 8.
* Choose a directory for installing Hillview
* Enable execution of powershell scripts; this can be done, for example, by
  running the following command in powershell as an administrator: `Set-ExecutionPolicy unrestricted`
* Download and install the [following script](bin/install-hillview.ps1) in the chosen directory
* Run the installation script using Windows powershell:

```
> install-hillview.ps1
```

# Running Hillview locally

## Windows machines

All Windows scripts are in the `bin` folder:

```
> cd bin
```

* Start Hillview processes:

```
> hillview-start.bat
```

* If needed give permissions to the application to communicate through the Windows firewall
* To stop hillview:

```
> hillview-stop.bat
```

## Ubuntu or MacOS machines

All the following scripts are in the `bin` folder.

```
$ cd bin
```

* Start the back-end service which performs all the data processing:

```
$ ./backend-start.sh &
```

* Start the web server
  (the default port of the web server is 8080; if you want to change it, change the setting
   in `apache-tomcat-9.0.4/conf/server.xml`).

```
$ ./frontend-start.sh
```

* start a web browser and open http://localhost:8080

* when you are done stop the two services by killing the
  `frontend-start.sh` and `backend-start.sh` jobs.

As an alternative, you can use the configuration service file
`bin/config-local.json` and use the instructions for [deploying
Hillview on a cluster](#deploying-the-hillview-service-on-a-cluster)
using this configuration file; this will run Hillview on the local
machine.

* (Optional, only if you have an installation for development, using
  the Java SDK) download and prepare the sample data:

```
$ ./rebuild.sh -a
$ ./demo-data-cleaner.sh
```

# Deploying the Hillview service on a cluster

Hillview uses `ssh` to deploy code on the cluster.  Prior to
deployment you must setup `ssh` on the cluster to use password-less
access to the cluster machines, as described here:
https://www.ssh.com/ssh/copy-id.  You must also install Java on all
machines in the cluster.

*Please note that Hillview allows arbitrary access to files on the
worker nodes from the client application running with the privileges
of the user specified in the configuration file.*

## Service configuration

The configuration of the Hillview service is described in a Json file;
two sample files are `bin/config.json`and `bin/config-local.json`.

```
// This file is a Json file that defines the configuration for a
// Hillview deployment.

{
  // Name of machine hosting the web server
  "webserver": "web.server.name",
  // Names of the machines hosting the workers; the web
  // server machine can also act as a worker
  "aggregators": [
    // The "aggregators" level is optional; if it is
    // missing, the configuration should contain just an array of workers
    {
      "name": "aggregator1.name",
      "workers": [
        "worker1.name",
        "worker2.name"
      ]
    }, {
      "name": "aggregator2.name",
      "workers": [
        "worker3.name",
        "worker4.name"
      ]
    }
  ],
  // Network port where the workers listen for requests
  "worker_port": 3569,
  // Network port where aggregators listen for requests
  "aggregator_port": 3570,
  // Java heap size for Hillview workers
  "default_heap_size": "25G",
  // User account for running the Hillview service, default is current user
  "user": "hillview",
  // Folder where the hillview service is installed on remote machines
  "service_folder": "/home/hillview",
  // Version of Apache Tomcat to deploy
  "tomcat_version": "9.0.4",
  // Tomcat installation folder name
  "tomcat": "apache-tomcat-9.0.4",
  // If true delete old log files, default is false
  "cleanup": false,
  // This can be used to override the default_heap_size for specific machines.
  "workers_heapsize": {
    "worker1.name": "20G"
  }
}
```

## Deployment scripts

All deployment scripts are written in Python, and are in the `bin` folder.

```
$ cd bin
```

Install the software on the machines:

```
$ ./deploy.py config.json
```

Start the Hillview services:

```
$ ./start.py config.json
```

To connect to the service open `http://<webserver>:8080` in your web
browser.

Stop the services:

```
$ ./stop.py config.json
```

Query the status of the services:

```
$ ./status config.json
```

## Data management

We provide some crude data management scripts and tools for clusters.
They are described [here](bin/README.md).

# Developing Hillview

## Software Dependencies

* Back-end: Ubuntu Linux > 16 or MacOS
* Java 8, Maven build system, various Java libraries
  (Maven will manage the libraries)
* Front-end: Typescript, webpack, Tomcat app server, node.js;
  some JavaScript libraries: d3, pako, and rx-js
* Cloud service management: Python3
* IDEA Intellij for development (optional)

## Installing Java

We use Java 8; newer versions will *not* work.

First, download a JDK (for Linux x64 or MacOS) from here:
http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
Note: it is not enough to have a Java VM installed, you need a JDK.

Make sure to download the tarball version of the JDK.

For Linux: Unpack the JDK, and set your `JAVA_HOME` environment variable to point
to the unpacked folder (e.g, <fully qualified path
to>/jdk/jdk1.8.0_101). To set your JAVA_HOME environment variable, add
the following to your ~/.bashrc or ~/.zshrc.

```
$ export JAVA_HOME="<path-to-jdk-folder>"
```

(For MacOS you do not need to set up JAVA_HOME.)

## Installing other software needed

The following shell script will install the other required dependencies
for building and testing.

On MacOS you first need to install [Homebrew](https://brew.sh/).  One way to do that is to run
```
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

To install all other dependencies you can run:

```
$ cd bin
$ ./install-dependencies.sh
```

For old versions of Ubuntu this may fail, so you may have to install the required
dependencies manually.

## Impala Java libraries

If you want to access the [Impala](https://impala.apache.org/)
database you will need to download and install the JDBC connectors for
Impala libraries from
[Cloudera](https://www.cloudera.com/documentation/other/connectors.html).
(These are not free software, so they are not available in Java Maven
repositories.)  You should install these in your local Maven
repository, e.g. in the ~/.m2/com/cloudera/impala folder.  You may
also need to adjust the version of the libraries in the file
platform/pom.xml.

## Building Hillview

* Build the software:

```
$ cd bin
$ ./rebuild.sh -a
```

### Build details

Hillview is currently split into two separate Maven projects.

* platform: pure Java, includes the entire back-end.  This produces a
JAR file `platform/target/hillview-jar-with-dependencies.jar`.  This
part can be built with:

```
$ cd platform
$ mvn clean install
$ cd ..
```

* web: the web server, web client and web services; this project links
to the result produced by the `platform` project.  This produces a WAR
(web archive) file `web/target/web-1.0-SNAPSHOT.war`.  This part can
be built with:

```
$ cd web
$ mvn package
$ cd ..
```

## Contributing code

You will need to sign a CLA (Contributor License Agreement) to
contribute code to Hillview under an Apache-2 license.  This is very
standard.

## Setup IntelliJ IDEA

Download and install Intellij IDEA: https://www.jetbrains.com/idea/.
You can just untar the Linux binary in a place of your choice and run
the shell script `ideaXXX/bin/idea.sh`.  The web projects uses
capabilities only available in the paid version of Intellij IDEA.

One solution is to load only the module that you want to contribute to: move to the
corresponding folder: `cd platform` or `cd web` and start
IntelliJ there.

Alternatively, if you have IntelliJ Ultimate you can create an empty project
in the hillview folder, and then import three modules (from File/Project structure/Modules,
add three modules: web/pom.xml, platform/pom.xml, and the root folder hillview itself).
After running `mvn install` in platform you should also mark the following directories 
as generated sources: `platform/target/generated-sources/protobuf/*` 

## Using git to contribute

* Fork the repository using the "fork" button on github, by following these instructions:
https://help.github.com/articles/fork-a-repo/
* Run IntelliJ code inspection (Analyze/Inspect code) before commit and solve all open issues.
* Submit them into your own forked repository and send us a pull request.

In more detail, here is a step-by-step guide to committing your changes:

1. Create a new branch for each fix; give it a nice suggestive name:
   - `git branch yourBranchName`
   - `git checkout yourBranchName`
   - The main benefit of using branches is that you can have multiple
     branches active at the same time, one for each independent fix.
2. `git add <files that changed>`
3. `git commit -m "Description of commit"`
4. `git fetch upstream`
5. `git rebase upstream/master`
6. Resolve conflicts, if any
   (rebase won't work if you don't; as you find conflicts you will need
    to `git add` the files you have merged, and then you may need to use
    `git rebase --continue` or `git rebase --skip`)
7. Test, analyze merged version.
8. `git push -f origin yourBranchName`.  You won't need the `-f` if you are
   not updating a previous push to this branch.
9. Create a pull request to merge your new branch into master (using the web ui).
10. Delete your branch after the merging has been done `git branch -D yourBranchName`
11. To run the program you should try the master branch:
  - `git checkout master`
  - `git fetch upstream`
  - `git rebase upstream/master`
  - `git push origin master`

## Guidance in writing code

* The pseudorandom generator is implemented in the class
  Randomness.java and uses Mersenne Twister.  Do not use the
  Java `Random` class, but this one.

* By default all pointers are assumed to be non-null; use the
  @Nullable annotation (from javax.annotation) for all pointers which
  can be null.  Use `Converters.checkNull` to cast a @Nullable to a
  @NonNull pointer.
