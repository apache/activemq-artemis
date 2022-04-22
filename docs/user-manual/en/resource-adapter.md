# Resource Adapter

For using Apache ActiveMQ Artemis in a JavaEE/JakartaEE environment, you can use the ResourceAdapter. Before you 
start, read carefully the [JMS chapter](using-jms.md) 

##In which case do you have to use a RA and in which not?
The main reason is the requirement of 
[XA](https://jakarta.ee/specifications/transactions/2.0/jakarta-transactions-spec-2.0.html), distributed 
transactions over multiple resources. If it is important for you no message get lost and multiple resources (e.g. 
database and messaging) must be in sync, there is no way around it.

##Versions
let's give you a short overview of the versions, to be sure you pick the correct *.rar.

|        | artemis-ra-rar  |     |
| ------ | --------------- | --- |
| JavaEE | JCA             | JMS |
| 8      | 1.7             | 2.0 |

| | artemis-jakarta-ra-rar |     |
| --------- | ------------ | --- |
| JakartaEE | JCA          | JMS |
| \>=9      | 2.0          | 3.0 | 

## Lets start
To use the RA, you have to build it by your own. This sounds harder than it is. But no worries, an 
[example](examples.md) 
is shipped with the distribution.

* [install maven](https://maven.apache.org/install.html)

```shell
cd examples/features/sub-modules/{artemis-jakarta-ra-rar,artemis-ra-rar}
mvn clean install
cd target
mv artemis*.rar artemis.rar
```
Now you can see the artemis.rar, and you are good to go. Follow the manual of your application server, to install the
ResourceAdapter.

## Details about the Resource Adapter configuration

Before you start with the configuration you have to know two basics: The configuration is split into two 
parts. First, the config to send messages to an address (outbound), and second, the config to get messages consumed 
from a destination (inbound). Each can be configured separately, or use both the ResourceAdapter settings.

Here are a few options listed. If you want an overview of all configuration options, consider
[ConnectionFactoryProperties](https://github.com/apache/activemq-artemis/blob/main/artemis-ra/src/main/java/org/apache/activemq/artemis/ra/ConnectionFactoryProperties.java)
as a base and additionally the specific classes for your object.

Consider also, the rar.xml file for options and explanations in your artemis.rar. There you can set the default options
for your ResourceAdapter. With the configuration of the ResourceAdapter in your application server, you are overriding rar.xml
defaults. With the configuration of the ConnectionFactory or the ActivationSpec, you can override the 
ResourceAdapter config.

### ResourceAdapter
Config options 
[ActiveMQRAProperties](https://github.com/apache/activemq-artemis/blob/main/artemis-ra/src/main/java/org/apache/activemq/artemis/ra/ActiveMQRAProperties.java)

- `connectionParameters` key value pairs, like host=localhost;port=61616,host=anotherHost;port=61617
- `userName` userName
- `password` password
- `clientID` clientID

### ConnectionFactory
Config options for the outbound ManagedConnectionFactory:
[ActiveMQRAMCFProperties](https://github.com/apache/activemq-artemis/blob/main/artemis-ra/src/main/java/org/apache/activemq/artemis/ra/ActiveMQRAMCFProperties.java)
The connection for the ManagedConnectionFactory is specified by the RA.

Config options for the inbound ConnectionFactory
[ActiveMQConnectionFactory](https://github.com/apache/activemq-artemis/blob/main/artemis-jms-client/src/main/java/org/apache/activemq/artemis/jms/client/ActiveMQConnectionFactory.java)
- `brokerUrl` url to broker.
- `cacheDestinations` by the jms session
#### ConnectionManager
You can't configure any properties.
### ActivationSpec
Config options 
[ActiveMQActivationSpec](https://github.com/apache/activemq-artemis/blob/main/artemis-ra/src/main/java/org/apache/activemq/artemis/ra/inflow/ActiveMQActivationSpec.java)

In the activation spec you can configure all the things you need to get messages consumed from artemis.
- `useJndi` true if you want lookup destinations via jndi.
- `connectionFactoryLookup` the jndiName of the connectionFactory, used by this activation spec. You can reference 
  an existing ManagedConnectionFactory or specify another.
- `jndiParams` for the InitialContext. key value pairs, like a=b;c=d;e=f
- `destination` name or jndi reference of the destination
- `destinationType` javax/jakarta.jms.Queue or javax/jakarta.jms.Topic
- `messageSelector` sql where clause syntax to filter messages to your message driven bean
- `maxSession` to consume messages in parallel from the broker
#### Only for topic message consumption
- `subscriptionDurability` Durable / NonDurable
- `subscriptionName` artemis hold all messages for this name if you use durable subscriptions 

## Logging
With the package `org.apache.activemq.artemis.ra` you catch all ResourceAdapter logging statements.
