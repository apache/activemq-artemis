# Embedding Apache ActiveMQ Artemis

Apache ActiveMQ Artemis is designed as set of simple Plain Old Java Objects
(POJOs). This means Apache ActiveMQ Artemis can be instantiated and run in any
dependency injection framework such as Spring or Google Guice. It also means
that if you have an application that could use messaging functionality
internally then it can *directly instantiate* Apache ActiveMQ Artemis clients
and servers in its own application code to perform that functionality. We call
this *embedding* Apache ActiveMQ Artemis.

Examples of applications that might want to do this include any application
that needs very high performance, transactional, persistent messaging but
doesn't want the hassle of writing it all from scratch.

Embedding Apache ActiveMQ Artemis can be done in very few easy steps -
supply a `broker.xml` on the classpath or instantiate the configuration object,
instantiate the server, start it, and you have a Apache ActiveMQ Artemis running
in your JVM. It's as simple and easy as that.

## Embedding with XML configuration

The simplest way to embed Apache ActiveMQ Artemis is to use the embedded
wrapper class and configure Apache ActiveMQ Artemis through `broker.xml`.

Here's a simple example `broker.xml`:

```xml
<configuration xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="urn:activemq" xsi:schemaLocation="urn:activemq /schema/artemis-server.xsd">
   <core xmlns="urn:activemq:core">

      <persistence-enabled>false</persistence-enabled>

      <security-enabled>false</security-enabled>

      <acceptors>
         <acceptor name="in-vm">vm://0</acceptor>
      </acceptors>
   </core>
</configuration>
```


```java
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

...

EmbeddedActiveMQ embedded = new EmbeddedActiveMQ();
embedded.start();

ServerLocator serverLocator =  ActiveMQClient.createServerLocator("vm://0");
ClientSessionFactory factory =  serverLocator.createSessionFactory();
ClientSession session = factory.createSession();

session.createQueue(new QueueConfiguration("example"));

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

The `EmbeddedActiveMQ` class has a few additional setter methods that allow you
to specify a different config file name as well as other properties. See the
javadocs for this class for more details.

## Embedding with programmatic configuration

You can follow this step-by-step guide to programmatically embed a broker
instance.

Create the `Configuration` object. This contains configuration information for
an Apache ActiveMQ Artemis instance. The setter methods of this class allow you
to programmatically set configuration options as described in the [Server
Configuration](configuration-index.md) section.

The acceptors are configured through `Configuration`. Just add the acceptor URL
the same way you would through the main configuration file.

```java
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;

...

Configuration config = new ConfigurationImpl();

config.addAcceptorConfiguration("in-vm", "vm://0");
config.addAcceptorConfiguration("tcp", "tcp://127.0.0.1:61616");
```

You need to instantiate an instance of
`org.apache.activemq.artemis.api.core.server.embedded.EmbeddedActiveMQ` and add
the configuration object to it.

```java
import org.apache.activemq.artemis.api.core.server.ActiveMQ;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

...

EmbeddedActiveMQ server = new EmbeddedActiveMQ();
server.setConfiguration(config);

server.start();
```

You also have the option of instantiating `ActiveMQServerImpl` directly:

```java
ActiveMQServer server = new ActiveMQServerImpl(config);
server.start();
```

## Dependency Frameworks

You may also choose to use a dependency injection framework such as The Spring
Framework. See [Spring Integration](spring-integration.md) for more details on
Spring and Apache ActiveMQ Artemis.

Apache ActiveMQ Artemis standalone uses
[Airline](https://github.com/airlift/airline) to bootstrap.
