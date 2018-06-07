# Unit Testing

The package `artemis-junit` provides tools to facilitate how to run Artemis resources inside JUnit Tests.

These are provided as JUnit "rules" and can make it easier to embed messaging functionality on your tests.


## Example

### Import this on your pom.xml

```xml
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>artemis-junit</artifactId>
   <!-- replace this for the version you are using -->
   <version>2.5.0</version>
   <scope>test</scope>
</dependency>
```

### Declare a rule on your JUnit Test

```java
import org.apache.activemq.artemis.junit.EmbeddedJMSResource;
import org.junit.Rule;
import org.junit.Test;

public class MyTest {

   @Rule
   public EmbeddedJMSResource resource = new EmbeddedJMSResource();

   @Test
   public void myTest() {

   }
}
```

This will start a server that will be available for your test:

```
[main] 17:00:16,644 INFO  [org.apache.activemq.artemis.core.server] AMQ221000: live Message Broker is starting with configuration Broker Configuration (clustered=false,journalDirectory=data/journal,bindingsDirectory=data/bindings,largeMessagesDirectory=data/largemessages,pagingDirectory=data/paging)
[main] 17:00:16,666 INFO  [org.apache.activemq.artemis.core.server] AMQ221045: libaio is not available, switching the configuration into NIO
[main] 17:00:16,688 INFO  [org.apache.activemq.artemis.core.server] AMQ221043: Protocol module found: [artemis-server]. Adding protocol support for: CORE
[main] 17:00:16,801 INFO  [org.apache.activemq.artemis.core.server] AMQ221007: Server is now live
[main] 17:00:16,801 INFO  [org.apache.activemq.artemis.core.server] AMQ221001: Apache ActiveMQ Artemis Message Broker version 1.5.0-SNAPSHOT [embedded-jms-server, nodeID=39e78380-842c-11e6-9e43-f45c8992f3c7] 
[main] 17:00:16,891 INFO  [org.apache.activemq.artemis.core.server] AMQ221002: Apache ActiveMQ Artemis Message Broker version 1.5.0-SNAPSHOT [39e78380-842c-11e6-9e43-f45c8992f3c7] stopped, uptime 0.272 seconds
```

### Ordering rules

This is actually a JUnit feature, but this could be helpful on pre-determining the order on which rules are executed. 

```java
ActiveMQDynamicProducerResource producer = new ActiveMQDynamicProducerResource(server.getVmURL());

@Rule
public RuleChain ruleChain = RuleChain.outerRule(new ThreadLeakCheckRule()).around(server).around(producer);
```

### Available Rules

Name | Description
--- | ---
EmbeddedActiveMQResource | Run a Server, without the JMS manager	
EmbeddedJMSResource | Run a Server, including the JMS Manager
ActiveMQConsumerResource | Automate the creation of a consumer		
ActiveMQProducerResource | Automate the creation of a producer
ThreadLeakCheckRule | Check that all threads have been finished after the test is finished
