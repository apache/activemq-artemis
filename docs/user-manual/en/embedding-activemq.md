# Embedding Apache ActiveMQ Artemis

Apache ActiveMQ Artemis is designed as set of simple Plain Old Java Objects (POJOs).
This means Apache ActiveMQ Artemis can be instantiated and run in any dependency
injection framework such as Spring or Google Guice. It also means that if you have an application that could use
messaging functionality internally, then it can *directly instantiate*
Apache ActiveMQ Artemis clients and servers in its own application code to perform that
functionality. We call this *embedding* Apache ActiveMQ Artemis.

Examples of applications that might want to do this include any
application that needs very high performance, transactional, persistent
messaging but doesn't want the hassle of writing it all from scratch.

Embedding Apache ActiveMQ Artemis can be done in very few easy steps. Instantiate the
configuration object, instantiate the server, start it, and you have a
Apache ActiveMQ Artemis running in your virtual machine. It's as simple and easy as
that.

## Simple Config File Embedding

The simplest way to embed Apache ActiveMQ Artemis is to use the embedded wrapper
classes and configure Apache ActiveMQ Artemis through its configuration files. There
are two different helper classes for this depending on whether your
using the Apache ActiveMQ Artemis Core API or JMS.

## Embeddeing Apache ActiveMQ Artemis Server

For instantiating a core Apache ActiveMQ Artemis Server, the steps are pretty
simple. The example requires that you have defined a configuration file
`broker.xml` in your classpath:

``` java
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

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

## POJO instantiation - Embedding Programmatically

You can follow this step-by-step guide to programmatically embed the
core, non-JMS Apache ActiveMQ Artemis Server instance:

Create the configuration object - this contains configuration
information for an Apache ActiveMQ Artemis instance. The setter methods of this class
allow you to programmatically set configuration options as describe in
the [Server Configuration](configuration-index.md) section.

The acceptors are configured through `ConfigurationImpl`. Just add the
`NettyAcceptorFactory` on the transports the same way you would through
the main configuration file.

``` java
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;

...

Configuration config = new ConfigurationImpl();
HashSet<TransportConfiguration> transports = new HashSet<TransportConfiguration>();

transports.add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
transports.add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

config.setAcceptorConfigurations(transports);
```

You need to instantiate an instance of
`org.apache.activemq.artemis.api.core.server.embedded.EmbeddedActiveMQ` and add
the configuration object to it.

``` java
import org.apache.activemq.artemis.api.core.server.ActiveMQ;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

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

## Dependency Frameworks

You may also choose to use a dependency injection framework such as
The Spring Framework. See [Spring Integration](spring-integration.md) for more details on
Spring and Apache ActiveMQ Artemis.

Apache ActiveMQ Artemis standalone uses [Airline](https://github.com/airlift/airline) to bootstrap.
