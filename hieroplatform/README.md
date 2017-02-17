# This folder contains Java code for implementing the Hiero back-end services.

## Building and testing:

To build the project from the commandline:

> $: mvn package

This will build the project, run the tests, and then produce a folder
named "target/" with the hiero JAR inside it.

To run only the tests:

> $: mvn test

To run tests from Intellij: run using the JUnit run configuration.

The package must be installed to be used by the hieroweb project:

> $: mvn install
