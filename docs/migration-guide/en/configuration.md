Configuration
=====================================

Once we download and install the broker we run into the first difference. With Artemis, you need to explicitly create a broker instance, while on ActiveMQ this step is optional. The whole idea of this step is to keep installation and configuration of the broker separate, which makes it easier to upgrade and maintain the broker in the future.

So in order to start with Artemis you need execute something like this

	$ bin/artemis create --user admin --password admin --role admins --allow-anonymous true /opt/artemis

No matter where you installed your broker binaries, the broker instance will be now in `/opt/artemis` directory. The content of this directory will be familiar to every ActiveMQ user:

 - `bin` - contains shell scripts for managing the broker(start, stop, etc.)
 - `data` - is where the broker state lives (message store)
 - `etc` - contains broker configuration file (it's what `conf` directory is in ActiveMQ)
 - `log` - Artemis stores logs in this separate directory, unlike ActiveMQ which keeps them in `data` directory
 - `tmp` - is utility directory for temporary files

 
Let's take a look now at the configuration in more details. The entry `etc/bootstrap.xml` file is here to set the basics, like the location of the main broker configuration file, utility apps like a web server and JAAS security.

The main configuration file is `etc/broker.xml`. Similarly to ActiveMQ's `conf/activemq.xml`, this is where you configure most of the aspects of the broker, like connector ports, destination names, security policies, etc. We will go through this file in details in the following articles.

The `etc/artemis.profile` file is similar to the `bin/env` file in ActiveMQ. Here you can configure environment variables for the broker, mostly regular JVM args related to SSL context, debugging, etc.

There's no much difference in logging configuration between two brokers, so anyone familiar with Java logging systems in general will find herself at home here. The `etc/logging.properties` file is where it's all configured.

Finally, we have JAAS configuration files (`login.config`, `artemis-users.properties` and `artemis-roles.properties`), which cover same roles as in ActiveMQ and we will go into more details on these in the article that covers security.

After this brief walk through the location of different configuration aspects of Artemis, we're ready to start the broker. If you wish to start the broker in the foreground, you should execute

```sh
$ bin/artemis run
```

This is the same as

```sh
$ bin/activemq console
```
command in ActiveMQ.

For running the broker as a service, Artemis provides a separate shell script `bin/artemis-service`. So you can run the broker in the background like
```sh
$ bin/artemis-service start
```

This is the same as running ActiveMQ with
```sh
$ bin/activemq start
```

After the start, you can check the broker status in `logs/artemis.log` file.

Congratulations, you have your Artemis broker up and running. By default, Artemis starts *Openwire* connector on the same port as ActiveMQ, so clients can connect. To test this you can go to your existing ActiveMQ instance and run the following commands.

````sh
$ bin/activemq producer
$ bin/activemq consumer
````

You should see the messages flowing through the broker. Finally, we can stop the broker with

```sh
$ bin/artemis-service stop
```

With this, our orienteering session around Artemis is finished. In the following articles we'll start digging deeper into the configuration details and differences between two brokers and see how that can affect your messaging applications.
