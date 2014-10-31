Running the Java EE examples
========================

To run a javaee example first make sure you have WildFly installed, the examples were tested against against [8.0.0.Final](http://wildfly.org/downloads/).

Then set the JBOSS_HOME property to your installation, something like:

export JBOSS_HOME=/home/user/wildfly-8.0.0.Final

Then simply cd into the directory of the example you want to run and 'mvn test'.

The examples use [Arquillian](http://www.jboss.org/arquillian.html) to start the JBoss server and to run the example itself.