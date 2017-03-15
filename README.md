# Hiero
Big data spreadsheet

## Dependences

* Ubuntu Linux, Maven build system, typescript, webpack, Tomcat app server, node.js
* IDEA Intellij for development (optional)

> $: sudo apt-get install maven node

### Installing Java

We use Java 8.

First, download a JDK for Linux x64 from here: http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

Make sure to download the tarball version of the JDK.

Unpack the JDK, and set your `JAVA_HOME` environment variable to point
to the unpacked folder (e.g, <fully qualified path
to>/jdk/jdk1.8.0_101). To set your JAVA_HOME environment variable, add
the following to your ~/.bashrc or ~/.zshrc.

> ```export JAVA_HOME="<path-to-jdk-folder>"```

## Project structure

Hiero is broken down into two separate projects.

* hieroplatform: pure Java, includes the entire back-end.  `hieroplatform` can be
developed using the free (community edition) of Intellij IDEA.  See
[hieroplatform/README.md](hieroplatform/README.md)

* hieroweb: the web server, web client and web services; this project links to the
result produced by the `hieroweb` project.  To develop and debug this we have
used capabilities available only in the paid version of Intellij, Ultimate, 
but only maven is needed to build.  See [hieroweb/README.md](hieroweb/README.md)

## Setup IntelliJ IDEA

Download and install Intellij IDEA:
https://www.jetbrains.com/idea/.  You can just untar the linux binary
in a place of your choice and run the shell script
`ideaXXX/bin/idea.sh`.

### Loading into IntelliJ IDEA

To load the project that you want to contribute to, move to the
corresponding folder: `cd hieroplatform` or `cd hieroweb` and start
intellij there.

The first time you start Intellij you must import the project: on the
welcome screen, select the "import project" option, point to the
"pom.xml" file inside the hiero folder, click "next" a few times and
you're good to go.

## Using git to contribute

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

## Guidance in writing code

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
