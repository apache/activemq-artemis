# Using the Server

This chapter will familiarise you with how to use the Apache ActiveMQ Artemis
server.

We'll show where it is, how to start and stop it, and we'll describe the
directory layout and what all the files are and what they do.

For the remainder of this chapter when we talk about the Apache ActiveMQ
Artemis server we mean the Apache ActiveMQ Artemis standalone server, in its
default configuration with a JMS Service enabled.

This document will refer to the full path of the directory where the ActiveMQ
distribution has been extracted to as `${ARTEMIS_HOME}` directory.

## Installation

After downloading the distribution, the following highlights some important
folders on the distribution:

             |___ bin
             |
             |___ examples
             |      |___ common
             |      |___ features
             |      |___ perf
             |      |___ protocols
             |
             |___ lib
             |      |___ client
             |
             |___ schema
             |
             |___ web
                    |___ api
                    |___ hacking-guide
                    |___ migration-guide
                    |___ user-manual


- `bin` - binaries and scripts needed to run ActiveMQ Artemis.

- `examples` - All manner of examples. Please refer to the [examples](examples.md)
  chapter for details on how to run them.

- `lib` - jars and libraries needed to run ActiveMQ Artemis

- `schema` - XML Schemas used to validate ActiveMQ Artemis configuration files

- `web` - The folder where the web context is loaded when the broker runs.

- `api` - The api documentation is placed under the web folder.

- `user-manual` - The user manual is placed under the web folder.


## Creating a Broker Instance

A broker instance is the directory containing all the configuration and runtime
data, such as logs and data files, associated with a broker process.  It is
recommended that you do *not* create the instance directory under
`${ARTEMIS_HOME}`.  This separation is encouraged so that you can more easily
upgrade when the next version of ActiveMQ Artemis is released.

On Unix systems, it is a common convention to store this kind of runtime data
under the `/var/lib` directory.  For example, to create an instance at
'/var/lib/mybroker', run the following commands in your command line shell:

```sh
cd /var/lib
${ARTEMIS_HOME}/bin/artemis create mybroker
```

A broker instance directory will contain the following sub directories:

- `bin`: holds execution scripts associated with this instance.
- `etc`: hold the instance configuration files
- `data`: holds the data files used for storing persistent messages
- `log`: holds rotating log files
- `tmp`: holds temporary files that are safe to delete between broker runs

At this point you may want to adjust the default configuration located in the
`etc` directory.

### Options

There are several options you can use when creating an instance.

For a full list of updated properties always use:

```
 $./artemis help create
 NAME
         artemis create - creates a new broker instance

 SYNOPSIS
        artemis create [--addresses <addresses>] [--aio] [--allow-anonymous]
                [--autocreate] [--blocking] [--cluster-password <clusterPassword>]
                [--cluster-user <clusterUser>] [--clustered] [--data <data>]
                [--default-port <defaultPort>] [--disable-persistence]
                [--encoding <encoding>] [--etc <etc>] [--failover-on-shutdown] [--force]
                [--global-max-size <globalMaxSize>] [--home <home>] [--host <host>]
                [--http-host <httpHost>] [--http-port <httpPort>]
                [--java-options <javaOptions>] [--mapped] [--max-hops <maxHops>]
                [--message-load-balancing <messageLoadBalancing>] [--name <name>]
                [--nio] [--no-amqp-acceptor] [--no-autocreate] [--no-autotune]
                [--no-fsync] [--no-hornetq-acceptor] [--no-mqtt-acceptor]
                [--no-stomp-acceptor] [--no-web] [--paging] [--password <password>]
                [--ping <ping>] [--port-offset <portOffset>] [--queues <queues>]
                [--replicated] [--require-login] [--role <role>] [--shared-store]
                [--silent] [--slave] [--ssl-key <sslKey>]
                [--ssl-key-password <sslKeyPassword>] [--ssl-trust <sslTrust>]
                [--ssl-trust-password <sslTrustPassword>] [--use-client-auth]
                [--user <user>] [--verbose] [--] <directory>

 OPTIONS
         --addresses <addresses>
             Comma separated list of addresses

         --aio
             Sets the journal as asyncio.

         --allow-anonymous
             Enables anonymous configuration on security, opposite of
             --require-login (Default: input)

         --autocreate
             Auto create addresses. (default: true)

         --blocking
             Block producers when address becomes full, opposite of --paging
             (Default: false)

         --cluster-password <clusterPassword>
             The cluster password to use for clustering. (Default: input)

         --cluster-user <clusterUser>
             The cluster user to use for clustering. (Default: input)

         --clustered
             Enable clustering

         --data <data>
             Directory where ActiveMQ data are stored. Paths can be absolute or
             relative to artemis.instance directory ('data' by default)

         --default-port <defaultPort>
             The port number to use for the main 'artemis' acceptor (Default:
             61616)

         --disable-persistence
             Disable message persistence to the journal

         --encoding <encoding>
             The encoding that text files should use

         --etc <etc>
             Directory where ActiveMQ configuration is located. Paths can be absolute or
             relative to artemis.instance directory ('etc' by default)

         --failover-on-shutdown
             Valid for shared store: will shutdown trigger a failover? (Default:
             false)

         --force
             Overwrite configuration at destination directory

         --global-max-size <globalMaxSize>
             Maximum amount of memory which message data may consume (Default:
             Undefined, half of the system's memory)

         --home <home>
             Directory where ActiveMQ Artemis is installed

         --host <host>
             The host name of the broker (Default: 0.0.0.0 or input if clustered)

         --http-host <httpHost>
             The host name to use for embedded web server (Default: localhost)

         --http-port <httpPort>
             The port number to use for embedded web server (Default: 8161)

         --java-options <javaOptions>
             Extra java options to be passed to the profile

         --mapped
             Sets the journal as mapped.

         --max-hops <maxHops>
             Number of hops on the cluster configuration

         --message-load-balancing <messageLoadBalancing>
             Load balancing policy on cluster. [ON_DEMAND (default) | STRICT |
             OFF]

         --name <name>
             The name of the broker (Default: same as host)

         --nio
             Sets the journal as nio.

         --no-amqp-acceptor
             Disable the AMQP specific acceptor.

         --no-autocreate
             Disable Auto create addresses.

         --no-autotune
             Disable auto tuning on the journal.

         --no-fsync
             Disable usage of fdatasync (channel.force(false) from java nio) on
             the journal

         --no-hornetq-acceptor
             Disable the HornetQ specific acceptor.

         --no-mqtt-acceptor
             Disable the MQTT specific acceptor.

         --no-stomp-acceptor
             Disable the STOMP specific acceptor.

         --no-web
             Remove the web-server definition from bootstrap.xml

         --paging
             Page messages to disk when address becomes full, opposite of
             --blocking (Default: true)

         --password <password>
             The user's password (Default: input)

         --ping <ping>
             A comma separated string to be passed on to the broker config as
             network-check-list. The broker will shutdown when all these
             addresses are unreachable.

         --port-offset <portOffset>
             Off sets the ports of every acceptor

         --queues <queues>
             Comma separated list of queues.

         --replicated
             Enable broker replication

         --require-login
             This will configure security to require user / password, opposite of
             --allow-anonymous

         --role <role>
             The name for the role created (Default: amq)

         --shared-store
             Enable broker shared store

         --silent
             It will disable all the inputs, and it would make a best guess for
             any required input

         --slave
             Valid for shared store or replication: this is a slave server?

         --ssl-key <sslKey>
             The key store path for embedded web server

         --ssl-key-password <sslKeyPassword>
             The key store password

         --ssl-trust <sslTrust>
             The trust store path in case of client authentication

         --ssl-trust-password <sslTrustPassword>
             The trust store password

         --use-client-auth
             If the embedded server requires client authentication

         --user <user>
             The username (Default: input)

         --verbose
             Adds more information on the execution

         --
             This option can be used to separate command-line options from the
             list of argument, (useful when arguments might be mistaken for
             command-line options

         <directory>
             The instance directory to hold the broker's configuration and data.
             Path must be writable.
```

Some of these properties may be mandatory in certain configurations and the
system may ask you for additional input.

```
./artemis create /usr/server
Creating ActiveMQ Artemis instance at: /user/server

--user: is a mandatory property!
Please provide the default username:
admin

--password: is mandatory with this configuration:
Please provide the default password:


--allow-anonymous | --require-login: is a mandatory property!
Allow anonymous access?, valid values are Y,N,True,False
y

Auto tuning journal ...
done! Your system can make 0.34 writes per millisecond, your journal-buffer-timeout will be 2956000

You can now start the broker by executing:

   "/user/server/bin/artemis" run

Or you can run the broker in the background using:

   "/user/server/bin/artemis-service" start
```


## Starting and Stopping a Broker Instance

Assuming you created the broker instance under `/var/lib/mybroker` all you need
to do start running the broker instance is execute:

```sh
/var/lib/mybroker/bin/artemis run
```

Now that the broker is running, you can optionally run some of the included
examples to verify the the broker is running properly.

To stop the Apache ActiveMQ Artemis instance you will use the same `artemis`
script, but with the `stop` argument.  Example:

```sh
/var/lib/mybroker/bin/artemis stop
```

Please note that Apache ActiveMQ Artemis requires a Java 7 or later runtime to
run.

By default the `etc/bootstrap.xml` configuration is used. The configuration can
be changed e.g. by running `./artemis run -- xml:path/to/bootstrap.xml` or
another config of your choosing.

Environment variables are used to provide ease of changing ports, hosts and
data directories used and can be found in `etc/artemis.profile` on linux and
`etc\artemis.profile.cmd` on Windows.

## Server JVM settings

The run scripts set some JVM settings for tuning the garbage collection policy
and heap size. We recommend using a parallel garbage collection algorithm to
smooth out latency and minimise large GC pauses.

By default Apache ActiveMQ Artemis runs in a maximum of 1GiB of RAM. To
increase the memory settings change the `-Xms` and `-Xmx` memory settings as
you would for any Java program.

If you wish to add any more JVM arguments or tune the existing ones, the run
scripts are the place to do it.

## Library Path

If you're using the [Asynchronous IO Journal](libaio.md) on Linux, you need to
specify `java.library.path` as a property on your Java options. This is done
automatically in the scripts.

If you don't specify `java.library.path` at your Java options then the JVM will
use the environment variable `LD_LIBRARY_PATH`.

You will need to make sure libaio is installed on Linux. For more information
refer to the [libaio chapter](libaio.html#runtime-dependencies).

## System properties

Apache ActiveMQ Artemis can take a system property on the command line for
configuring logging.

For more information on configuring logging, please see the section on
[Logging](logging.md).

## Configuration files

The configuration file used to bootstrap the server (e.g.  `bootstrap.xml` by
default) references the specific broker configuration files.

- `broker.xml`. This is the main ActiveMQ configuration file. All the
  parameters in this file are described [here](configuration-index.md)

It is also possible to use system property substitution in all the
configuration files. by replacing a value with the name of a system property.
Here is an example of this with a connector configuration:

```xml
<connector name="netty">tcp://${activemq.remoting.netty.host:localhost}:${activemq.remoting.netty.port:61616}</connector>
```

Here you can see we have replaced 2 values with system properties
`activemq.remoting.netty.host` and `activemq.remoting.netty.port`. These values
will be replaced by the value found in the system property if there is one, if
not they default back to localhost or 61616 respectively. It is also possible
to not supply a default. i.e.  `${activemq.remoting.netty.host}`, however the
system property *must* be supplied in that case.

### Bootstrap configuration file

The stand-alone server is basically a set of POJOs which are instantiated by
Airline commands.

The bootstrap file is very simple. Let's take a look at an example:

```xml
<broker xmlns="http://activemq.org/schema">

   <jaas-security domain="activemq"/>

   <server configuration="file:/path/to/broker.xml"/>

   <web bind="http://localhost:8161" path="web">
       <app url="activemq-branding" war="activemq-branding.war"/>
       <app url="artemis-plugin" war="artemis-plugin.war"/>
       <app url="console" war="console.war"/>
   </web>
</broker>
```

- `server` - Instantiates a core server using the configuration file from the
  `configuration` attribute. This is the main broker POJO necessary to do all
   the real messaging work.

- `jaas-security` - Configures security for the server. The `domain` attribute
   refers to the relevant login module entry in `login.config`.

- `web` - Configures an embedded Jetty instance to serve web applications like
  the admin console.

### Broker configuration file

The configuration for the Apache ActiveMQ Artemis core server is contained in
`broker.xml`. This is what the FileConfiguration bean uses to configure the
messaging server.

There are many attributes which you can configure Apache ActiveMQ Artemis. In
most cases the defaults will do fine, in fact every attribute can be defaulted
which means a file with a single empty `configuration` element is a valid
configuration file. The different configuration will be explained throughout
the manual or you can refer to the configuration reference
[here](configuration-index.md).

## Windows Server

On windows you will have the option to run ActiveMQ Artemis as a service.  Just
use the following command to install it:

```
 $ ./artemis-service.exe install
```

The create process should give you a hint of the available commands available
for the artemis-service.exe

## Adding Runtime Dependencies

Runtime dependencies like diverts, transformers, broker plugins, JDBC drivers,
password decoders, etc. must be accessible by the broker at runtime. Package
the dependency in a jar, and put it on the broker's classpath. This can be done
by placing the jar file in the `lib` directory of the broker distribution
itself or in the `lib` directory of the broker instance. A broker instance does
not have a `lib` directory by default so it may need to be created.  It should
be on the "top" level with the `bin`, `data`, `log`, etc. directories.
