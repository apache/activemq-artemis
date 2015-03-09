Running the ActiveMQ Examples
============================

To run an individual example firstly cd into the example directory and run

```
mvn verify -Pexample
```

If you are running against an un released version, i.e. from master branch, you will have to run `mvn install` on the root
pom.xml and the example/activemq-jms-examples-common/pom.xml first.

If you want to run all the examples (except those that need to be run standalone) you can run `mvn verify -Pexample` in the examples
directory but before you do you will need to up the memory used by running:

```
export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=256m"
```

To run the javaee examples follow the instructions in examples/javaee/README.md

### Recreating the examples

If you are trying to copy the examples somewhere else and modifying them. Consider asking Maven to explicitly list all the dependencies:

```
# if trying to modify the 'topic' example:
cd examples/jms/topic && mvn dependency:list
```
