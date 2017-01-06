# Updating Artemis

The standard Apache ActiveMQ is runnable out of the box. Just download it, 
go into the unzipped ActiveMQ folder and run this command: ./bin/activemq run. 
The ActiveMQ Artemis sub project needs an additional step to run the Message Queue.
Before running activemq run you have to create a new message broker instance. 
It looks like an overhead at first glance, but becomes very practically 
when updating to a new Artemis version for example. 
To create a artemis broker cd into the artemis folder and run: `./bin/artemis create $HOME/mybroker` on the command line.

> **Note**
>
> We recommend choosing a folder different than the downloaded apache-artemis one to separate both from each other.
> This separation allowes you run multiple brokers with the same artemis runtime for example. 
> It also simplifies updating to newer versions of Artemis.

Because of this separation it's very easy to update Artemis. 
You just need to cd into the `etc` folder of your created message broker and open the `artemis.profile` file.
It contains a property which is relevant for the update procedure:
 
    ARTEMIS_HOME='/Users/.../apache-artemis-X.X.X'
    
The `ARTEMIS_HOME` property is used to link the broker together with the Artemis runtime. 
In case you want to update your broker you can simply download the new version of ActiveMQ Artemis and change the `ARTEMIS_HOME` to the formerly downloaded, newer version.
That's all. There's no need to touch your broker, copy configuration files or stuff like that.