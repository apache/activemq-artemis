# JMS Security Manager Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example is based on the "security" example and demonstrates how to implement a custom security manager. The custom security manager in this example simply logs details for authentication and authorization and then passes everything through to an instance of `org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager` (i.e. the default security manager).