#Running the ActiveMQ Artemis JMeter Performance Testing Examples

##Create and run a sample broker for performance testing:

```sh
artemis create my-broker --queues exampleQueue --topics exampleTopic

my-broker/bin/artemis run
```
##Download and Install JMeter's latest release:
 
https://jmeter.apache.org/download_jmeter.cgi
 
##Copy artemis-jms-client dependencies under $JMETER_HOME/lib folder:

- artemis-jms-client-all.jar

######Create a jndi.properties file with the connectionFactory:

```
connectionFactory.ConnectionFactory=tcp://localhost:61616
```

######Pack jndi.properties file into a jar file and put it under $JMETER_HOME/lib folder:

```sh
jar -cf artemis-jndi.jar jndi.properties
```

######Open jMeter and run the available Test Plan examples:

- 1.jms_p2p_test.jmx
- 2.pub_sub_test.jmx
