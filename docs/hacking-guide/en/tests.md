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

The test-suite uses JUnit to manage test execution and life-cycle.  Most tests extend [`org.apache.activemq.artemis.tests.util.ActiveMQTestBase`](https://github.com/apache/activemq-artemis/blob/master/artemis-server/src/test/java/org/apache/activemq/artemis/tests/util/ActiveMQTestBase.java)
which contains JUnit setup and tear-down methods as well as a wealth of utility functions to configure, start, manage,
and stop brokers as well as perform other common tasks.

Check out [`org.apache.activemq.artemis.tests.integration.SimpleTest`](https://github.com/apache/activemq-artemis/blob/master/tests/integration-tests/src/test/java/org/apache/activemq/artemis/tests/integration/SimpleTest.java).
It's a very simple test-case that extends `org.apache.activemq.artemis.tests.util.ActiveMQTestBase` and uses its methods
to configure a server, run a test, and then `super.tearDown()` cleans it up once the test completes. The test-case 
includes comments to explain everything. As the name implies, this is a simple test-case that demonstrates the most basic
functionality of the test-suite. A simple test like this takes less than a second to run on modern hardware.

Although `org.apache.activemq.artemis.tests.integration.SimpleTest` is simple it could be simpler still by extending
[`org.apache.activemq.artemis.tests.util.SingleServerTestBase`](https://github.com/apache/activemq-artemis/blob/master/artemis-server/src/test/java/org/apache/activemq/artemis/tests/util/SingleServerTestBase.java).
This class does all the setup of a simple server automatically and provides the test-case with a `ServerLocator`, 
`ClientSessionFactory`, and `ClientSession` instance. [`org.apache.activemq.artemis.tests.integration.SingleServerSimpleTest`](https://github.com/apache/activemq-artemis/blob/master//tests/integration-tests/src/test/java/org/apache/activemq/artemis/tests/integration/SingleServerSimpleTest.java)
is an example based on `org.apache.activemq.artemis.tests.integration.SimpleTest` but extends `org.apache.activemq.artemis.tests.util.SingleServerTestBase`
which eliminates all the setup and class variables which are provided by `SingleServerTestBase` itself.

## Keys for writing good tests

### Avoid leaks

An important task for any test-case is to clean up all the resources it creates when it runs. This includes the server
instance itself and any resources created to connect to it (e.g. instances of `ServerLocator`, `ClientSessionFactory`,
`ClientSession`, etc.). This task is typically completed in the test's `tearDown()` method.  However, `ActiveMQTestBase` 
(and other classes which extend it) simplifies this process. As [`org.apache.activemq.artemis.tests.integration.SimpleTest`](https://github.com/apache/activemq-artemis/blob/master/tests/integration-tests/src/test/java/org/apache/activemq/artemis/tests/integration/SimpleTest.java)
demonstrates, there are several methods you can use when creating your test which will ensure proper clean up _automatically_
when the test is torn down. These include:

- All the overloaded `org.apache.activemq.artemis.tests.util.ActiveMQTestBase.createServer(..)` methods. If you choose
_not_ to use one of these methods to create your `ActiveMQServer` instance then use the `addServer(ActiveMQServer)` 
method to add the instance to the test-suite's internal resource ledger.
- Methods from `org.apache.activemq.artemis.tests.util.ActiveMQTestBase` to create a `ServerLocator` like 
`createInVMNonHALocator` and `createNettyNonHALocator`. If you choose _not_ to use one of these methods then use 
`addServerLocator(ServerLocator)` to add the locator to the test-suite's internal resource ledger.
- `org.apache.activemq.artemis.tests.util.ActiveMQTestBase.createSessionFactory(ServerLocator)` for creating your session
factory. If you choose _not_ to use this method then use `org.apache.activemq.artemis.tests.util.ActiveMQTestBase.addSessionFactory`
to add the factory to the test-suite's internal resource ledger.

### Create configurations

There are numerous methods in `org.apache.activemq.artemis.tests.util.ActiveMQTestBase` to create a configuration. These
methods are named like create&#42;Config(..). Each one creates a slightly different configuration but there is a lot of 
overlap between them.

In any case, `org.apache.activemq.artemis.core.config.Configuration` is a [_fluent_](https://en.wikipedia.org/wiki/Fluent_interface)
interface so it's easy to customize however you need.

### Look at other test-cases

If you need ideas on how to configure something or test something try looking through the test-suite at other test-cases
which may be similar. This is one of the best ways to learn how the test-suite works and how you can leverage the
testing infrastructure to test your particular case.

    