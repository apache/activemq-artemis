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

| | artemis-ra-rar   |     |     |
| --------- | ------ | --- | --- |
| Artemis   | JavaEE | JCA | JMS |
| \>= 2.18.0 | \>=8      | 1.7 | 2.0 |
| <= 2.17.0 | 7      | 1.5 | 2.0 |

| | artemis-jakarta-ra-rar | | |
| --- | ------- | --- | --- |
| Artemis | JavaEE | JCA | JMS |
| \>= 2.18.0 | \>=9 | 2.0 | 3.0 |

## Lets start
To use the RA, you have to build it by your own. This sounds harder than it is. But no worries, an 
[examples](examples.md) 
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
#### Only for topic message consumption
- `subscriptionDurability` Durable / NonDurable
- `subscriptionName` artemis hold all messages for this name if you use durable subscriptions 

## Logging
With the package `org.apache.activemq.artemis.ra` you catch all ResourceAdapter logging statements.

## Example for WLP / OpenLiberty
sample for your server.xml, in this case, with the jms 3.0 API. For a runnable sample, take also a look in the 
examples/features/sub-modules/wlp directory. 
```xml
<server>
  <resourceAdapter id="artemis" location="${wlp.user.dir}shared/lib/artemis.rar">
      <properties.artemis />
  </resourceAdapter>
  
  <!-- dont't use the jmsConnectionFactory element -->
  <connectionFactory jndiName="jms/sampleArtemisConnectionFactory">
      <properties.artemis />
      <connectionManager /> <!-- not implemented by Artemis-->
  </connectionFactory>
  
  <jmsActivationSpec id="your-app/QueueListener">
      <properties.artemis useJndi="true" destination="jms/sampleQueue" destinationType="jakarta.jms.Queue"
          connectionFactoryLookup="jms/sampleArtemisConnectionFactory" />
  </jmsActivationSpec>
  
  <jmsActivationSpec id="your-app/TopicListener">
      <properties.artemis clientId="aClientId" useJndi="true" destination="jms/sampleTopic"
          destinationType="jakarta.jms.Topic" subscriptionDurability="Durable" subscriptionName="sampleDurableSubscriberName"
          connectionFactoryLookup="jms/asyncAppConnectionFactory" />
  </jmsActivationSpec>
  
  <jmsTopic jndiName="jms/sampleTopic">
      <properties.artemis Address="a_sample_adress" />
  </jmsTopic>
  
  <jmsQueue jndiName="jms/sampleQueue">
      <properties.artemis Address="another_sample_adress" />
  </jmsQueue>
</server>
```