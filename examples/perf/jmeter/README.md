Running the ActiveMQ Artemis JMeter Performance Testing Examples
============================

1. Create and run a sample broker for performance testing:

```sh
artemis create my-broker --queues exampleQueue --topics exampleTopic
my-broker/bin/artemis run
```
2. Download and Install JMeter's latest release: http://jmeter.apache.org/download_jmeter.cgi
 
3. Copy artemis-jms-client dependencies under $JMETER_HOME/lib folder:

- artemis-jms-client.jar
- artemis-core-client.jar
- jgroups.jar
- artemis-commons.jar
- jboss-logmanager.jar
- jboss-logging.jar
- commons-beanutils.jar
- commons-logging.jar (already present at JMeter lib folder - may not be needed)
- commons-collections:.ar (already present at JMeter lib folder - may not be needed)
- artemis-selector.jar
- artemis-journal.jar
- artemis-native.jar
- netty-all.jar
- geronimo-jms_2.0_spec.jar
- javax.inject.jar

4. Create a jndi.properties file with the connectionFactory Server Information:

```
connectionFactory.ConnectionFactory=tcp://localhost:61616
```

5. Pack jndi.properties file into a jar file and put it under $JMETER_HOME/lib folder:

```sh
jar -cf artemis-jndi.jar jndi.properties
```

6. Open jMeter and run the available Test Plan examples:

- 1.jms_p2p_test.jmx
- 2.pub_sub_test.jmx
