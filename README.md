# ActiveMQ Artemis

This file describes some minimum 'stuff one needs to know' to get started coding in this project.

## Source

For details about the modifying the code, building the project, running tests, IDE integration, etc. see
our [Hacking Guide](./docs/hacking-guide/en/SUMMARY.md).

## Build Status

Build Status: [![Build Status](https://travis-ci.org/apache/activemq-artemis.svg?branch=master)](https://travis-ci.org/apache/activemq-artemis)

## Building the ASYNC IO library

ActiveMQ Artemis provides two journal persistence types, NIO (which uses the Java NIO libraries), and ASYNCIO which interacts with the linux kernel libaio library.   The ASYNCIO journal type should be used where possible as it is far superior in terms of performance.

ActiveMQ Artemis does not ship with the Artemis Native ASYNCIO library in the source distribution.  These need to be built prior to running "mvn install", to ensure that the ASYNCIO journal type is available in the resulting build.  Don't worry if you don't want to use ASYNCIO or your system does not support libaio, ActiveMQ Artemis will check at runtime to see if the required libraries and system dependencies are available, if not it will default to using NIO.

To build the ActiveMQ Artemis ASYNCIO native libraries, please follow the instructions in the artemis-native/README.

## Documentation

Our documentation is always in sync with our releases at the [Apache ActiveMQ Artemis](https://activemq.apache.org/artemis/docs.html) website.

Or you can also look at the current master version on [github](https://github.com/apache/activemq-artemis/blob/master/docs/user-manual/en/SUMMARY.md).

## Examples

To run an example firstly make sure you have run

    $ mvn -Prelease install

If the project version has already been released then this is unnecessary.

Each individual example can be run using this command from its corresponding directory:

    $ mvn verify

If you wish to run groups of examples then use this command from a parent directory (e.g. examples/features/standard):

    $ mvn -Pexamples verify

### Recreating the examples

If you are trying to copy the examples somewhere else and modifying them. Consider asking Maven to explicitly list all the dependencies:

    # if trying to modify the 'topic' example:
    cd examples/jms/topic && mvn dependency:list

### Open Web Application Security Project (OWASP) Report

If you wish to generate the report for CCV dependencies, you may run it with the -Powasp profile

    $ mvn -Powasp verify

The output will be under ./target/dependency-check-report.html **for each** sub-module.