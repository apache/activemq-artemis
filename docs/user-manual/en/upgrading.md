# Upgrading the Broker

Apache ActiveMQ 5.x (and previous versions) is runnable out of the box by executing
the command: `./bin/activemq run`. The ActiveMQ Artemis broker follows a different
paradigm where the project distribution serves as the broker "home" and one or more
broker "instances" are created which reference the "home" for resources (e.g. jar files)
which can be safely shared between broker instances. Therefore, an instance of the broker
must be created before it can be run. This may seems like an overhead at first 
glance, but it becomes very practical when updating to a new Artemis version for example.

To create an Artemis broker instance navigate into the Artemis home folder and run:
`./bin/artemis create /path/to/myBrokerInstance` on the command line.

> **Note**
>
> It's recommended to choose a folder different than the on where Apache Artemis was 
> downloaded. This separation allows you run multiple broker instances with the same
> Artemis "home" for example. It also simplifies updating to newer versions of Artemis.

Because of this separation it's very easy to upgrade Artemis in most cases. 

## General Upgrade Procedure

Upgrading may require some specific steps noted in the [versions](versions.md), but the 
general process is as follows:

1. Navigate to the `etc` folder of the broker instance that's being upgraded
1. Open `artemis.profile` (`artemis.profile.cmd` on Windows). It contains a property 
   which is relevant for the upgrade:

   ```
   ARTEMIS_HOME='/path/to/apache-artemis-version'
   ```
 
The `ARTEMIS_HOME` property is used to link the instance with the home. 
_In most cases_ the instance can be upgraded to a newer version simply by changing the
value of this property to the location of the new broker home. Please refer to the
aforementioned [versions](versions.md) document for additional upgrade steps (if required).