![Hillview project logo](hillview-logo.png)

Hillview: a big data spreadsheet.  Hillview is a cloud-based
service for visualizing interactively large datasets.  
The hillview user interface executes in a browser.  

Contents:

[1. Documentation](#1-Documentation)

[2. Local installation](#2-Installing-and-running-Hillview-on-a-local-machine)

[3. Cluster installation](#3-Deploying-the-Hillview-service-on-a-cluster)

[4. Developing Hillview](#4-Developing-Hillview)

# 1. Documentation

There is a [Hillview user manual](docs/userManual.md).

A [short video](https://1drv.ms/v/s!AlywK8G1COQ_jeRQatBqla3tvgk4FQ)
shows the system in action in real-time.

You can [try a demo](http://ec2-18-217-136-170.us-east-2.compute.amazonaws.com:8080/)
of the system running on 15 small Amazon machines.

A [paper](https://arxiv.org/abs/1907.04827) describing the system in
some detail.  This is an extended version of the following publication
Mihai Budiu, Parikshit Gopalan, Lalith Suresh, Udi Wieder, Han
Kruiger, and Marcos K. Aguilera, Hillview: A trillion-cell spreadsheet
for big data, in PVLDB 2019, 12(11).

Documentation for the [internal APIs](docs/hillview-apis.pdf).

Experimental use of Hillview using [differential privacy](privacy.md).

# 2. Installing and running Hillview on a local machine

For developing Hillview see [below](#4.-developing-hillview).

## 2.1 Linux of MacOS

### 2.1.1 Installing on Linux or MacOS

* Install Java 8.  At this point newer versions of Java will *not* work.
* clone this github repository
* run the script `bin/install.sh`

### 2.1.2 Running on Ubuntu or MacOS machines

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

## 2.2 Windows

### 2.2.1 Installing on Windows

* Download and install Java 8.
* Choose a directory for installing Hillview
* Enable execution of powershell scripts; this can be done, for example, by
  running the following command in powershell as an administrator: `Set-ExecutionPolicy unrestricted`
* Download and install the [following script](bin/install-hillview.ps1) in the chosen directory
* Run the installation script using Windows powershell:

```
> install-hillview.ps1
```

### 2.2.2 Running on Windows

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

# 3. Deploying the Hillview service on a cluster

Hillview uses `ssh` to deploy code on the cluster.  Prior to
deployment you must setup `ssh` on the cluster to use password-less
access to the cluster machines, as described here:
https://www.ssh.com/ssh/copy-id.  You must also install Java on all
machines in the cluster.

*Please note that Hillview allows arbitrary access to files on the
worker nodes from the client application running with the privileges
of the user specified in the configuration file.*

## 3.1 Service configuration

The configuration of the Hillview service is described in a Json file 
(enhanced with comments); two sample files are `bin/config.json`and 
`bin/config-local.json`.  The file `config-local.json` treats the local
machine as a one-machine cluster.

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

## 3.2 Deployment scripts

All deployment scripts are written in Python, and are in the `bin` folder.

```
$ cd bin
```

Install the software on all cluster machines:

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
$ ./status.py config.json
```

## 3.3. Data management

We provide some crude data management scripts and tools for clusters.
They are described [here](bin/README.md).

# 4. Developing Hillview

We only provide development instructions for Linux or MacOS, but there is
no reason Hillview could not be developed on Windows. 

## 4.1. Software Dependencies

* Back-end: Ubuntu Linux > 16 or MacOS.
On MacOS you first need to install [Homebrew](https://brew.sh/).  One way to do that is to run
```
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

* Java 8, Maven build system, various Java libraries
  (Maven will manage the libraries)
* Front-end: Typescript, webpack, Tomcat app server, node.js;
  some JavaScript libraries: d3, pako, and rx-js
* Cloud service management: Python3
* Once you have Java everything else is installed by scripts.

### 4.1.1 Installing Java

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

### 4.1.2. Installing other software needed

The following shell script will install the other required dependencies
for building and testing.

```
$ cd bin
$ ./install-dependencies.sh
```

For old versions of Ubuntu this may fail, so you may have to install the required
dependencies manually.

#### 4.1.2.1 Optional Impala Java libraries

If you want to access the [Impala](https://impala.apache.org/)
database you will need to download and install the JDBC connectors for
Impala libraries from
[Cloudera](https://www.cloudera.com/documentation/other/connectors.html).
(These are not free software, so they are not available in Java Maven
repositories.)  You should install these in your local Maven
repository, e.g. in the ~/.m2/com/cloudera/impala folder.  You may
also need to adjust the version of the libraries in the file
`platform/pom.xml`.

## 4.2. Building Hillview

* Build the software for the first time:

```
$ cd bin
$ ./rebuild.sh -a
$ ./demo-data-cleaner.sh
```

Subsequent builds can just run

```
$ bin/rebuild.sh
```

Hillview is currently split into two separate Maven projects.  One can 
also build the two projects separately, as follows:

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

## 4.3. Contributing code

You will need to sign a CLA (Contributor License Agreement) to
contribute code to Hillview under an Apache-2 license.  This is very
standard.

## 4.4. Setup IntelliJ IDEA

Download and install Intellij IDEA: https://www.jetbrains.com/idea/.
The web project typescript requires the (paid) Ultimate version of Intellij.

First run maven to generate the Java code automatically generated for gRPC: 

```
$ cd platform
$ mvn install
```

Create an empty project
in the hillview folder, and then import three modules (from File/Project structure/Modules,
add three modules: web/pom.xml, platform/pom.xml, and the root folder hillview itself).

## 4.5. Setup VS Code

Download and install Visual Studio Code: https://code.visualstudio.com/download. 
Here is a step-by-step guide to add the necessary extensions, run Maven commands, and attach a debugger:

1. Install these extensions and then restart the VS Code.
	- `Java Extension Pack`: installs 6 important Java extensions at once.
	- `JavaScript and TypeScript Nightly`: enables JavaScript and TypeScript IntelliSense.
	- `Language Support for Java(TM) by Red Hat
redhat.java`: recognize projects with Maven or Gradle build in the directory hierarchy.
	- `Maven for Java`: provides a project explorer and shortcuts to execute Maven commands.
2. Select `Add workspace folder...` at the Welcome page, then choose `hillview/platform/` directory. The platform module should be displayed in the `Explorer` view. 
3. Add `web` module to the workspace by clicking `File`->`Add Folder to Workspace...` and then choose `hillview/web/` directory. 
4. Save the workspace by clicking `File`->`Save Workspace As...` and store it in your personal folder outside `hillview/` root directory.
5. Next, about executing Maven commands; in the `Explorer` view, click `MAVEN PROJECTS`. There are two Maven folders correspond to `web` and `platform` modules; 
   click those folders to expand and display the Maven pom files. The Maven commands will be displayed by right clicking the pom files.
6. Finally, about attaching a debugger:
	- Bring up the `Run` view, select the `Run` icon in the `Activity Bar` on the left side of VS Code.
	- From the `Run` view, click `create a launch.json file`, you will see the `platform` and `web` modules listed. We will create two `launch.json` files, one for `platform` module and the other for `web` module. 
	- When configuring the `launch.json` for `platform` module, you must select `Java` option. Otherwise, choose `Chrome (preview)` option when configuring the `web` module. Then, delete the auto generated `configurations` 
	and specify the correct configuration to attach the debugger. The important fields are `url`, `hostname`, `port`, and `request`. More about this is here 
	[VS Code Debugging#launch-configuration](https://code.visualstudio.com/docs/editor/debugging#_launch-configurations) and [VS Code#Java-Debugging](https://code.visualstudio.com/docs/java/java-debugging#_attach).

## 4.6 Debugging

Debugging on a single machine can done as follows:
- you can start the back-end service under the debugger,
  by starting the HillviewBackend binary with command-line arguments 127.0.0.1:3569
- you can start the front-end service by attaching 
  to the Java process created by Java Tomcat.  The frontend-start.sh
  script has a line that sets up the environment variables to enable this.

## 4.7. Running the tests

* The unit tests are run by building with maven or by running `bin/rebuild.sh -t`.
* The UI tests are run by starting Hillview on a local machine and
then clicking the "Test/Run" menu button.

## 4.8. Using git to contribute

Fork the repository using the "fork" button on github, by following these instructions:
https://help.github.com/articles/fork-a-repo/

Here is a step-by-step guide to submitting contributions:

1. Create a new branch for each fix; give it a nice suggestive name:
   - `git branch yourBranchName`
   - `git checkout yourBranchName`
2. `git add <files that changed>`
3. `git commit -m "Description of commit"`
4. `git fetch upstream`
5. `git rebase upstream/master`
6. Resolve conflicts, if any
   (rebase won't work if you don't; as you find conflicts you will need
    to `git add` the files you have merged, and then you may need to use
    `git rebase --continue` or `git rebase --skip`)
7. Test, analyze merged version.
8. `git push -f origin yourBranchName`.
9. Create a pull request to merge your new branch into master (using the web ui).
10. Delete your branch after the merging has been done `git branch -D yourBranchName`

## 4.9. Guidance in writing code

* Use the IntelliJ code inspection feature (Analyze/Inspect code).

* The pseudorandom generator is implemented in the class
  [Randomness.java](platform/src/main/java/org/hillview/utils/Randomness.java) and uses Mersenne Twister.  Do not use the
  Java `Random`.

* By default all pointers are assumed to be non-null; use the
  `@Nullable` annotation (from javax.annotation) for all pointers which
  can be null.  Use `Converters.checkNull` to cast a @Nullable pointer to a
  non-null pointer.
  
* Some code executes on multiple machines or in multiple threads.  In particular,
  all classes that derive from `IMap` or `ISketch` should be immutable.
