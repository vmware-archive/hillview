# Hiero
Big data spreadsheet

## Requirements

* Ubuntu Linux, Hadoop filesystem, Maven build system
* IDEA Intellij for development

> $: sudo apt-get install maven

## Using git to contribute

* Fork the repository using the "fork" button on github, by following these instructions:
https://help.github.com/articles/fork-a-repo/
* To merge your fork with the original use: `git fetch upstream; git merge upstream/master`
* Run IntelliJ code inspection (Analyze/Inspect code) before commit and solve all open issues
* When you make changes you can submit them into your own fork
* After committing changes, create a pull request (using the github web UI)

In more detail, here is a step-by-step guide to committing your changes:

1. git add <files that changed>
2. git commit -m "Description of commit" (Saves your work)
3. git fetch upstream (To get the upstream version)
4. git merge upstream/master
5. Resolve conflicts if any. If so, repeat 1-4.
6. Test, analyze merged version.
7. git push origin master.
8. Create a pull request.

## Installing Java

We use Java 8.

First, download a JDK for Linux x64 from here: http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

Make sure to download the tarball version of the JDK.

Unpack the JDK, and set your `JAVA_HOME` environment variable to point
to the unpacked folder (e.g, <fully qualified path
to>/jdk/jdk1.8.0_101). To set your JAVA_HOME environment variable, add
the following to your ~/.bashrc or ~/.zshrc.

> ```export JAVA_HOME="<path-to-jdk-folder>"```

## Install Apache Tomcat web application server

Download the binaries from http://tomcat.apache.org/download-80.cgi and untar in the project folder.

## Setup IntelliJ IDEA

First, download and install Intellij IDEA:
https://www.jetbrains.com/idea/.  You can just untar the linux binary
in a place of your choice and run the shell script
`ideaXXX/bin/idea.sh`.

## Import Hiero into IntelliJ IDEA

Next, import the hiero project inside IntelliJ.  On the welcome
screen, select the "import project" option, point to the "pom.xml"
file inside the hiero folder, click "next" a few times and you're good
to go.
To build the project from the commandline type:

> $: mvn package

This will build the project, run the tests, and then produce a folder
named "target/" with the hiero JAR inside it.

To run only the tests:

> $: mvn test

