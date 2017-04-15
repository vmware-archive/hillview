# Hiero

A big data spreadsheet.

## Dependences

* Back-end: Ubuntu Linux (Technically we don't depend on Linux, it's
  just that we have only tested this on Linux and Mac; the
  instructions here are just for Linux)
* Java 8, Maven build system, various Java libraries, in particular RxJava
  (Maven will manage the libraries for you)
* Front-end: typescript, webpack, Tomcat app server, node.js
* Some JavaScript libraries: d3 and rxjs
* IDEA Intellij for development (optional)

See (#installing-the-software) for detailed installation instructions.

## Project structure

Hiero is currently split into two separate Maven projects.

* hieroplatform: pure Java, includes the entire back-end.  `hieroplatform` can be
developed using the free (community edition) of Intellij IDEA.

* hieroweb: the web server, web client and web services; this project links to the
result produced by the `hieroweb` project.  To develop and debug this we have
used capabilities available only in the paid version of Intellij, Ultimate,
but only maven is needed to build.

## How to run the demo

* First install all software required as described
  [below](#installing-the-software).

* Download the data for the demo.  The download script will download
  and decompress some CSV files with flights data from FAA.  You can
  edit the script to change the range of data that will be downloaded;
  the default is to download 2 months of data.

```
$ cd data
$ ./download.sh
$ cd ..
```

* Install the Hiero distributed platform library:

```
$ cd hieroplatform
$ mvn install
```

* The dataset has 110 columns; we can use them all, but for the demo
  we have stripped the dataset to 15 columns to better fit on the
  screen.  The following command creates the smaller files from the
  downloaded data:

```
$ mvn exec:java
$ cd ..
```

If you run out of memory while doing this try to increase the java
heap size as follows:

```
$ MAVEN_OPTS="-Xmx2048M" mvn exec:java
```

* Build the web server and the front-end

```
$ cd hieroweb
$ mvn package
```

* Start the tomcat web server

```
$ ../apache-tomcat-8.5.8/bin/catalina.sh run
```

* start a web browser at http://localhost:8080 and browse the data!

## Contributing code

### Setup IntelliJ IDEA

Download and install Intellij IDEA: https://www.jetbrains.com/idea/.
You can just untar the linux binary in a place of your choice and run
the shell script `ideaXXX/bin/idea.sh`.  The hieroweb projects uses
capabilities only available in the paid version of Intellij IDEA.

### Loading into IntelliJ IDEA

To load the project that you want to contribute to, move to the
corresponding folder: `cd hieroplatform` or `cd hieroweb` and start
intellij there.

The first time you start Intellij you must import the project: on the
welcome screen, select the "import project" option, point to the
"pom.xml" file inside the hiero folder, click "next" a few times and
you're good to go.

### Using git to contribute

* Fork the repository using the "fork" button on github, by following these instructions:
https://help.github.com/articles/fork-a-repo/
* Run IntelliJ code inspection (Analyze/Inspect code) before commit and solve all open issues.
* Submit them into your own forked repository and send us a pull request.

In more detail, here is a step-by-step guide to committing your changes:

1. `git add <files that changed>`
2. `git commit -m "Description of commit"`
3. `git fetch upstream`
4. `git rebase upstream/master`
5. Resolve conflicts if any. If so, repeat 1-4.
6. Test, analyze merged version.
7. `git push -f origin master`
8. Create a pull request (using the web ui).

### Guidance in writing code

* The pseudorandom generator is implemented in the class
  Randomness.java and uses Mersenne Twister.  Do not use the
  Java `Random` class, but this one.

* By default all pointers are assumed to be non-null; use the
  @Nullable annotation (from javax.annotation) for all pointers which
  can be null.  Use `Converters.checkNull` to cast a @Nullable to a
  @NonNull pointer.

* (optional) Use "mvn site" to generate the FindBugs report in
  target/site/findbugs.html.  Make sure any new code checked in does
  not introduce any violations.  A subset of these checks is also
  done by the IDEA code inspection tool.

## Installing the software needed

Install Maven and node.js:

```
$ sudo apt-get install maven node
```

### Installing Java

We use Java 8.

First, download a JDK for Linux x64 from here:
http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

Make sure to download the tarball version of the JDK.

Unpack the JDK, and set your `JAVA_HOME` environment variable to point
to the unpacked folder (e.g, <fully qualified path
to>/jdk/jdk1.8.0_101). To set your JAVA_HOME environment variable, add
the following to your ~/.bashrc or ~/.zshrc.

```
$ export JAVA_HOME="<path-to-jdk-folder>"
```

## Install Apache Tomcat web application server

Use version 8.5.8 (other versions may work, but we only tested this
one).  Download the binaries from
[http://tomcat.apache.org/download-80.cgi] and untar in the hiero
toplevel folder.

```
$ cd hieroweb
$ cd apache-tomcat-8.5.8/webapps
$ rm -rf ROOT*
$ ln -s ../../hieroweb/target/hieroweb-1.0-SNAPSHOT.war ROOT.war
$ cd ..
```

## Install typescript and JavaScript libraries and tools

On Mac the following command seems to work correctly only without `sudo`.

```
$ sudo npm install -g typescript ts-loader webpack@1.14.0 typings
```

This installs the typescript compiler, the `webpack` tool, used to
bundle multiple JavaScript files together, the `ts-loader` tool for
webpack, which allows it to compile directly typescript into
javascript, and the `typings` tool, which can be used to install
typescript type definition files for some of the JavaScript libraries
that we are using.

Then install various JavaScript libraries: `rx`, `rx-dom` and `d3`,
together with the typescript type definitions for these libraries:

```
$ cd hieroweb/src/main/webapp
$ npm install rx rx-dom d3
$ npm install @types/d3 --save
$ typings install dt~rx-dom --save
$ cd ../../../..
```
