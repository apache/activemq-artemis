# Tests

To run the unit tests:

    $ mvn -Ptests test

Generating reports from unit tests:

    $ mvn install site

Running tests individually

    $ mvn -Ptests -DfailIfNoTests=false -Dtest=<test-name> test

where &lt;test-name> is the name of the Test class without its package name