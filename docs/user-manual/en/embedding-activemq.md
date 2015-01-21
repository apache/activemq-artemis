# Embedding ActiveMQ

ActiveMQ is designed as set of simple Plain Old Java Objects (POJOs).
This means ActiveMQ can be instantiated and run in any dependency
injection framework such as Spring or Google Guice. It also means that if you have an application that could use
messaging functionality internally, then it can *directly instantiate*
ActiveMQ clients and servers in its own application code to perform that
functionality. We call this *embedding* ActiveMQ.

Examples of applications that might want to do this include any
application that needs very high performance, transactional, persistent
messaging but doesn't want the hassle of writing it all from scratch.

Embedding ActiveMQ can be done in very few easy steps. Instantiate the
configuration object, instantiate the server, start it, and you have a
ActiveMQ running in your virtual machine. It's as simple and easy as
that.

## Simple Config File Embedding

The simplest way to embed ActiveMQ is to use the embedded wrapper
classes and configure ActiveMQ through its configuration files. There
are two different helper classes for this depending on whether your
using the ActiveMQ Core API or JMS.

## Core API Only

For instantiating a core ActiveMQ Server only, the steps are pretty
simple. The example requires that you have defined a configuration file
`activemq-configuration.xml` in your classpath:

``` java
import org.apache.activemq.core.server.embedded.EmbeddedActiveMQ;

...

EmbeddedActiveMQ embedded = new EmbeddedActiveMQ();

embedded.start();

ClientSessionFactory nettyFactory =  ActiveMQClient.createClientSessionFactory(
                                        new TransportConfiguration(
                                           InVMConnectorFactory.class.getName()));

ClientSession session = factory.createSession();

session.createQueue("example", "example", true);

ClientProducer producer = session.createProducer("example");

ClientMessage message = session.createMessage(true);

message.getBody().writeString("Hello");

producer.send(message);

session.start();

ClientConsumer consumer = session.createConsumer("example");

ClientMessage msgReceived = consumer.receive();

System.out.println("message = " + msgReceived.getBody().readString());

session.close();
```

The `EmbeddedActiveMQ` class has a few additional setter methods that
allow you to specify a different config file name as well as other
properties. See the javadocs for this class for more details.

## JMS API

JMS embedding is simple as well. This example requires that you have
defined the config files `activemq-configuration.xml`,
`activemq-jms.xml`, and a `activemq-users.xml` if you have security
enabled. Let's also assume that a queue and connection factory has been
defined in the `activemq-jms.xml` config file.

``` java
import org.apache.activemq.jms.server.embedded.EmbeddedJMS;

...

EmbeddedJMS jms = new EmbeddedJMS();
jms.start();

// This assumes we have configured activemq-jms.xml with the appropriate config information
ConnectionFactory connectionFactory = jms.lookup("ConnectionFactory");
Destination destination = jms.lookup("/example/queue");

... regular JMS code ...
```

By default, the `EmbeddedJMS` class will store component entries defined
within your `activemq-jms.xml` file in an internal concurrent hash map.
The `EmbeddedJMS.lookup()` method returns components stored in this map.
If you want to use JNDI, call the `EmbeddedJMS.setContext()` method with
the root JNDI context you want your components bound into. See the
javadocs for this class for more details on other config options.

## POJO instantiation - Embedding Programmatically

You can follow this step-by-step guide to programmatically embed the
core, non-JMS ActiveMQ Server instance:

Create the configuration object - this contains configuration
information for a ActiveMQ instance. The setter methods of this class
allow you to programmatically set configuration options as describe in
the [Server Configuration](configuration-index.md) section.

The acceptors are configured through `ConfigurationImpl`. Just add the
`NettyAcceptorFactory` on the transports the same way you would through
the main configuration file.

``` java
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;

...

Configuration config = new ConfigurationImpl();
HashSet<TransportConfiguration> transports = new HashSet<TransportConfiguration>();
      
transports.add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
transports.add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

config.setAcceptorConfigurations(transports);
```

You need to instantiate an instance of
`org.apache.activemq.api.core.server.embedded.EmbeddedActiveMQ` and add
the configuration object to it.

``` java
import org.apache.activemq.api.core.server.ActiveMQ;
import org.apache.activemq.core.server.embedded.EmbeddedActiveMQ;

...

EmbeddedActiveMQ server = new EmbeddedActiveMQ();
server.setConfiguration(config);

server.start();
```

You also have the option of instantiating `ActiveMQServerImpl` directly:

``` java
ActiveMQServer server = new ActiveMQServerImpl(config);
server.start();
```

For JMS POJO instantiation, you work with the EmbeddedJMS class instead
as described earlier. First you define the configuration
programmatically for your ConnectionFactory and Destination objects,
then set the JmsConfiguration property of the EmbeddedJMS class. Here is
an example of this:

``` java
// Step 1. Create ActiveMQ core configuration, and set the properties accordingly
Configuration configuration = new ConfigurationImpl();
configuration.setPersistenceEnabled(false);
configuration.setSecurityEnabled(false);
configuration.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));

// Step 2. Create the JMS configuration
JMSConfiguration jmsConfig = new JMSConfigurationImpl();

// Step 3. Configure the JMS ConnectionFactory
TransportConfiguration connectorConfig = new TransportConfiguration(NettyConnectorFactory.class.getName());
ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl("cf", connectorConfig, "/cf");
jmsConfig.getConnectionFactoryConfigurations().add(cfConfig);

// Step 4. Configure the JMS Queue
JMSQueueConfiguration queueConfig = new JMSQueueConfigurationImpl("queue1", null, false, "/queue/queue1");
jmsConfig.getQueueConfigurations().add(queueConfig);

// Step 5. Start the JMS Server using the ActiveMQ core server and the JMS configuration
EmbeddedJMS jmsServer = new EmbeddedJMS();
jmsServer.setConfiguration(configuration);
jmsServer.setJmsConfiguration(jmsConfig);
jmsServer.start();
```

Please see the examples for an example which shows how to setup and run ActiveMQ
embedded with JMS.

## Dependency Frameworks

You may also choose to use a dependency injection framework such as
The Spring Framework. See [Spring Integration](spring-integration.md) for more details on
Spring and ActiveMQ.

ActiveMQ standalone uses [Airline](https://github.com/airlift/airline) to bootstrap.
