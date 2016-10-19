# Artemis on Apache Karaf

Apache ActiveMQ Artemis is OSGi ready. Below you can find instruction on how to install and configure broker on Apache Karaf OSGi container.

## Installation

Apache ActiveMQ Artemis provides features that makes it easy to install the broker on Apache Karaf (4.x or later). First you need to define the feature URL, like 

```sh
karaf@root()> feature:repo-add mvn:org.apache.activemq/artemis-features/1.3.0-SNAPSHOT/xml/features
```
    
This will add Artemis related features   
```
karaf@root()> feature:list | grep artemis
artemis                       | 1.3.0.SNAPSHOT   |          | Uninstalled | artemis-1.3.0-SNAPSHOT   | Full ActiveMQ Artemis broker with default configuration
netty-core                    | 4.0.32.Final     |          | Uninstalled | artemis-1.3.0-SNAPSHOT   | Netty libraries
artemis-core                  | 1.3.0.SNAPSHOT   |          | Uninstalled | artemis-1.3.0-SNAPSHOT   | ActiveMQ Artemis broker libraries
artemis-amqp                  | 1.3.0.SNAPSHOT   |          | Uninstalled | artemis-1.3.0-SNAPSHOT   | ActiveMQ Artemis AMQP protocol libraries
artemis-stomp                 | 1.3.0.SNAPSHOT   |          | Uninstalled | artemis-1.3.0-SNAPSHOT   | ActiveMQ Artemis Stomp protocol libraries
artemis-mqtt                  | 1.3.0.SNAPSHOT   |          | Uninstalled | artemis-1.3.0-SNAPSHOT   | ActiveMQ Artemis MQTT protocol libraries
artemis-hornetq               | 1.3.0.SNAPSHOT   |          | Uninstalled | artemis-1.3.0-SNAPSHOT   | ActiveMQ Artemis HornetQ protocol libraries    
```

Feature named `artemis` contains full broker installation, so running    
    
    feature:install artemis

will install and run the broker.

## Configuration

The broker is installed as `org.apache.activemq.artemis` OSGi component, so it's configured through `${KARAF_BASE}/etc/org.apache.activemq.artemis.cfg` file. An example of the file looks like

	config=file:etc/artemis.xml
	name=local
	domain=karaf
	rolePrincipalClass=org.apache.karaf.jaas.boot.principal.RolePrincipal
	
| Name               | Description                                     | Default value                                      |
| ------------------ | ----------------------------------------------- | -------------------------------------------------- |
| config             | Location of the configuration file              | ${KARAF_BASE}/etc/artemis.xml                      |
| name               | Name of the broker                              | local                                              |
| domain             | JAAS domain to use for security                 | karaf                                              |
| rolePrincipalClass | Class name used for role authorization purposes | org.apache.karaf.jaas.boot.principal.RolePrincipal |
	
The default broker configuration file is located in `${KARAF_BASE}/etc/artemis.xml`	
