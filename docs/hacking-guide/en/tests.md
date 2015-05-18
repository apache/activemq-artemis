# Tests

## Running Tests

To run the unit tests:

    $ mvn -Ptests test

Generating reports from unit tests:

    $ mvn install site

Running tests individually

    $ mvn -Ptests -DfailIfNoTests=false -Dtest=<test-name> test

where &lt;test-name> is the name of the Test class without its package name

## Writing Tests

The broker is comprised of POJOs so it's simple to configure and run a broker instance and test particular functionality.
Even complex test-cases involving multiple clustered brokers are relatively easy to write. Almost every test in the 
test-suite follows this pattern - configure broker, start broker, test functionality, stop broker.

The test-suite uses JUnit to manage test execution and life-cycle.  Most tests extend [org.apache.activemq.artemis.tests.util.ServiceTestBase](../../../artemis-server/src/test/java/org/apache/activemq/artemis/tests/util/ServiceTestBase.java)
which contains JUnit setup and tear-down methods as well as a wealth of utility functions to configure, start, manage,
and stop brokers as well as perform other common tasks.