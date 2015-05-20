# ActiveMQ Artemis

This file describes some minimum 'stuff one needs to know' to get started coding in this project.

## Source

For details about the modifying the code, building the project, running tests, IDE integration, etc. see 
our [Hacking Guide](./docs/hacking-guide/en/SUMMARY.md).

## Examples

To run an example firstly make sure you have run

    $ mvn -Prelease install

If the project version has already been released then this is unnecessary.

then you will need to set the following maven options, on Linux by

    $ export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=512m"

and the finally run the examples by

    $ mvn verify

You can also run individual examples by running the same command from the directory of which ever example you want to run.
NB for this make sure you have installed examples/common.

### Recreating the examples

If you are trying to copy the examples somewhere else and modifying them. Consider asking Maven to explicitly list all the dependencies:

    # if trying to modify the 'topic' example:
    cd examples/jms/topic && mvn dependency:list
