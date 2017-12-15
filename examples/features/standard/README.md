Running the ActiveMQ Artemis Examples
============================

To run an individual example firstly cd into the example directory and run

```sh
mvn verify
```

Most examples offer a way to start them without creating and starting the broker (say if you want to do it manually)

```sh
mvn verify -PnoServer
```

If you are running against an un released version, i.e. from master branch, you will have to run `mvn install` on the root
pom.xml and the example/activemq-jms-examples-common/pom.xml first.

If you want to run all the examples (except those that need to be run standalone) you can run `mvn verify -Pexamples` in the examples
directory but before you do you will need to up the memory used by running:

```
export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=256m"
```
### Recreating the examples

If you are trying to copy the examples somewhere else and modifying them. Consider asking Maven to explicitly list all the dependencies:

```
# if trying to modify the 'topic' example:
cd examples/jms/topic && mvn dependency:list
```
