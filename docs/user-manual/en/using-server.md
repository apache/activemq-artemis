# Using the Server

This chapter will familiarise you with how to use the Apache ActiveMQ Artemis
server.

We'll show where it is, how to start and stop it, and we'll describe the
directory layout and what all the files are and what they do.

This document will refer to the full path of the directory where the ActiveMQ
distribution has been extracted to as `${ARTEMIS_HOME}`.

## Installation

The following highlights some important folders on the distribution:
```
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
```

- `bin` - binaries and scripts needed to run ActiveMQ Artemis.

- `examples` - All manner of examples. Please refer to the [examples](examples.md)
  chapter for details on how to run them.

- `lib` - jars and libraries needed to run ActiveMQ Artemis

- `schema` - XML Schemas used to validate ActiveMQ Artemis configuration files

- `web` - The folder where the web context is loaded when the broker runs.

- `api` - The api documentation is placed under the web folder.

- `user-manual` - The user manual is placed under the web folder.


## Creating a Broker Instance

A broker *instance* is the directory containing all the configuration and runtime
data, such as logs and message journal, associated with a broker process. It is
recommended that you do *not* create the instance directory under
`${ARTEMIS_HOME}`.  This separation is encouraged so that you can more easily
upgrade when the next version of ActiveMQ Artemis is released.

On Unix systems, it is a common convention to store this kind of runtime data
under the `/var/lib` directory.  For example, to create an instance at
`/var/lib/mybroker`, run the following commands in your command line shell:

```sh
cd /var/lib
${ARTEMIS_HOME}/bin/artemis create mybroker
```

A broker instance directory will contain the following sub directories:

- `bin`: holds execution scripts associated with this instance.
- `data`: holds the data files used for storing persistent messages
- `etc`: hold the instance configuration files
- `lib`: holds any custom runtime Java dependencies like transformers,
  plugins, interceptors, etc.
- `log`: holds rotating log files
- `tmp`: holds temporary files that are safe to delete between broker runs

At this point you may want to adjust the default configuration located in the
`etc` directory.

### Options

There are several options you can use when creating an instance. For a full list
of options use the `help` command:

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
                [--java-options <javaOptions>] [--jdbc]
                [--jdbc-bindings-table-name <jdbcBindings>]
                [--jdbc-connection-url <jdbcURL>]
                [--jdbc-driver-class-name <jdbcClassName>]
                [--jdbc-large-message-table-name <jdbcLargeMessages>]
                [--jdbc-lock-expiration <jdbcLockExpiration>]
                [--jdbc-lock-renew-period <jdbcLockRenewPeriod>]
                [--jdbc-message-table-name <jdbcMessages>]
                [--jdbc-network-timeout <jdbcNetworkTimeout>]
                [--jdbc-node-manager-table-name <jdbcNodeManager>]
                [--jdbc-page-store-table-name <jdbcPageStore>]
                [--journal-device-block-size <journalDeviceBlockSize>] [--mapped]
                [--max-hops <maxHops>] [--message-load-balancing <messageLoadBalancing>]
                [--name <name>] [--nio] [--no-amqp-acceptor] [--no-autocreate]
                [--no-autotune] [--no-fsync] [--no-hornetq-acceptor]
                [--no-mqtt-acceptor] [--no-stomp-acceptor] [--no-web] [--paging]
                [--password <password>] [--ping <ping>] [--port-offset <portOffset>]
                [--queues <queues>] [--relax-jolokia] [--replicated] [--require-login]
                [--role <role>] [--security-manager <securityManager>] [--shared-store]
                [--silent] [--slave] [--ssl-key <sslKey>]
                [--ssl-key-password <sslKeyPassword>] [--ssl-trust <sslTrust>]
                [--ssl-trust-password <sslTrustPassword>] [--staticCluster <staticNode>]
                [--use-client-auth] [--user <user>] [--verbose] [--] <directory>

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
            Directory where ActiveMQ configuration is located. Paths can be
            absolute or relative to artemis.instance directory ('etc' by
            default)

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

        --jdbc
            It will activate jdbc

        --jdbc-bindings-table-name <jdbcBindings>
            Name of the jdbc bindings table

        --jdbc-connection-url <jdbcURL>
            The connection used for the database

        --jdbc-driver-class-name <jdbcClassName>
            JDBC driver classname

        --jdbc-large-message-table-name <jdbcLargeMessages>
            Name of the large messages table

        --jdbc-lock-expiration <jdbcLockExpiration>
            Lock expiration

        --jdbc-lock-renew-period <jdbcLockRenewPeriod>
            Lock Renew Period

        --jdbc-message-table-name <jdbcMessages>
            Name of the jdbc messages table

        --jdbc-network-timeout <jdbcNetworkTimeout>
            Network timeout

        --jdbc-node-manager-table-name <jdbcNodeManager>
            Name of the jdbc node manager table

        --jdbc-page-store-table-name <jdbcPageStore>
            Name of the page store messages table

        --journal-device-block-size <journalDeviceBlockSize>
            The block size by the device, default at 4096.

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
            Comma separated list of queues with the option to specify a routing
            type. (ex: --queues myqueue,mytopic:multicast)

        --relax-jolokia
            disable strict checking on jolokia-access.xml

        --replicated
            Enable broker replication

        --require-login
            This will configure security to require user / password, opposite of
            --allow-anonymous

        --role <role>
            The name for the role created (Default: amq)

        --security-manager <securityManager>
            Which security manager to use - jaas or basic (Default: jaas)

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

        --staticCluster <staticNode>
            Cluster node connectors list, separated by comma: Example
            "tcp://server:61616,tcp://server2:61616,tcp://server3:61616"

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

Some of these options may be mandatory in certain configurations and the
system may ask you for additional input, e.g.:

```sh
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
examples to verify the broker is running properly.

To stop the Apache ActiveMQ Artemis instance you will use the same `artemis`
script, but with the `stop` argument.  Example:

```sh
/var/lib/mybroker/bin/artemis stop
```

Please note that Apache ActiveMQ Artemis requires a Java 11 or later.

By default the `etc/bootstrap.xml` configuration is used. The configuration can
be changed e.g. by running `./artemis run -- xml:path/to/bootstrap.xml` or
another config of your choosing.

Environment variables are used to provide ease of changing ports, hosts and
data directories used and can be found in `etc/artemis.profile` on linux and
`etc\artemis.profile.cmd` on Windows.

## Library Path

If you're using the [Asynchronous IO Journal](libaio.md) on Linux, you need to
specify `java.library.path` as a property on your Java options. This is done
automatically in the scripts.

If you don't specify `java.library.path` at your Java options then the JVM will
use the environment variable `LD_LIBRARY_PATH`.

You will need to make sure libaio is installed on Linux. For more information
refer to the [libaio chapter](libaio.html#runtime-dependencies).

## Configuration files

These are the files you're likely to find in the `etc` directory of a default
broker instance with a short explanation of what they configure. Scroll down
further for additional details as appropriate.

 - `artemis.profile` - system properties and JVM arguments (e.g. `Xmx`, `Xms`, 
   etc.) 
 - `artemis-roles.properties` - user/role mapping for the default 
   [properties-based JAAS login module](security.md#propertiesloginmodule)
 - `artemis-users.properties` - user/password for the default 
   [properties-based JAAS login module](security.md#propertiesloginmodule)
 - `bootstrap.xml` - embedded web server, security, location of `broker.xml`
 - `broker.xml` - core broker configuration, e.g. acceptors, addresses, queues,
   diverts, clustering; [full reference](configuration-index.md).
 - `jolokia-access.xml` - [security for Jolokia](https://jolokia.org/reference/html/security.html),
   specifically Cross-Origin Resource Sharing (CORS)
 - `log4j2.properties` - [logging config](logging.md) like levels, log files
   locations, etc.
 - `login.config` - standard Java configuration for JAAS [security](security.md)
 - `management.xml` - remote connectivity and [security for JMX MBeans](management.md#role-based-authorisation-for-jmx)

### Bootstrap configuration file

The `bootstrap.xml` file is very simple. Let's take a look at an example:

```xml
<broker xmlns="http://activemq.apache.org/schema">

   <jaas-security domain="activemq"/>

   <server configuration="file:/path/to/broker.xml"/>

   <web path="web">
      <binding uri="http://localhost:8161">
         <app url="activemq-branding" war="activemq-branding.war"/>
         <app url="artemis-plugin" war="artemis-plugin.war"/>
         <app url="console" war="console.war"/>
      </binding>
   </web>
</broker>
```

- `jaas-security` - Configures JAAS-based security for the server. The
  `domain` attribute refers to the relevant login module entry in
  `login.config`. If different behavior is needed then a custom security
  manager can be configured by replacing `jaas-security` with
  `security-manager`. See the "Custom Security Manager" section in the
  [security chapter](security.md) for more details.

- `server` - Instantiates a core server using the configuration file from the
  `configuration` attribute. This is the main broker POJO necessary to do all
  the real messaging work.

- `web` - Configures an embedded web server for things like the admin console.

### Broker configuration file

The configuration for the Apache ActiveMQ Artemis core broker is contained in
`broker.xml`.

There are many attributes which you can configure for Apache ActiveMQ Artemis. In
most cases the defaults will do fine, in fact every attribute can be defaulted
which means a file with a single empty `configuration` element is a valid
configuration file. The different configuration will be explained throughout the
manual or you can refer to the configuration reference [here](configuration-index.md).

## System Property Substitution

It is possible to use system property substitution in all the configuration
files. by replacing a value with the name of a system property. Here is an
example of this with a connector configuration:

```xml
<connector name="netty">tcp://${activemq.remoting.netty.host:localhost}:${activemq.remoting.netty.port:61616}</connector>
```

Here you can see we have replaced 2 values with system properties
`activemq.remoting.netty.host` and `activemq.remoting.netty.port`. These values
will be replaced by the value found in the system property if there is one, if
not they default back to `localhost` or `61616` respectively. It is also possible
to not supply a default. i.e.  `${activemq.remoting.netty.host}`, however the
system property *must* be supplied in that case.

## Windows Server

On windows you will have the option to run ActiveMQ Artemis as a service.  Just
use the following command to install it:

```
 $ ./artemis-service.exe install
```

The create process should give you a hint of the available commands available
for the artemis-service.exe

## Adding Bootstrap Dependencies

Bootstrap dependencies like logging handlers must be accessible by the log
manager at boot time. Package the dependency in a jar and put it on the boot
classpath before of log manager jar. This can be done appending the jar at the
variable `JAVA_ARGS`, defined in `artemis.profile`, with the option `-Xbootclasspath/a`.

## Adding Runtime Dependencies

Runtime dependencies like diverts, transformers, broker plugins, JDBC drivers,
password decoders, etc. must be accessible by the broker at runtime. Package
the dependency in a jar, and put it on the broker's classpath. This can be done
by placing the jar file in the `lib` directory of the broker distribution
itself or in the `lib` directory of the broker instance. A broker instance does
not have a `lib` directory by default so it may need to be created.  It should
be on the "top" level with the `bin`, `data`, `log`, etc. directories.
