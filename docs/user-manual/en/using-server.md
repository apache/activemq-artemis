# Using the Server

This chapter will familiarise you with how to use the ActiveMQ server.

We'll show where it is, how to start and stop it, and we'll describe the
directory layout and what all the files are and what they do.

For the remainder of this chapter when we talk about the ActiveMQ server
we mean the ActiveMQ standalone server, in its default configuration
with a JMS Service enabled.

## Starting and Stopping the standalone server

In the distribution you will find a directory called `bin`.

`cd` into that directory and you will find a Unix/Linux script called
`activemq` and a Windows script called `activemq.cmd`.

To start the ActiveMQ instance on Unix/Linux type `./activemq run`

To start the ActiveMQ instance on Windows type `activemq.cmd run`

These scripts are very simple and basically just set-up the classpath
and some JVM parameters and bootstrap the server using
[Airline](https://github.com/airlift/airline).

To stop the ActiveMQ instance you will use the same `activemq` script.

To run on Unix/Linux type `./activemq stop`

To run on Windows type `activemq.cmd stop`

Please note that ActiveMQ requires a Java 6 or later runtime to run.

By default the `config/non-clustered/bootstrap.xml` configuration is
used. The configuration can be changed e.g. by running
`./activemq run -- xml:../config/clustered/bootstrap.xml` or another
config of your choosing.

## Server JVM settings

The run scripts set some JVM settings for tuning the garbage collection
policy and heap size. We recommend using a parallel garbage collection
algorithm to smooth out latency and minimise large GC pauses.

By default ActiveMQ runs in a maximum of 1GiB of RAM. To increase the
memory settings change the `-Xms` and `-Xmx` memory settings as you
would for any Java program.

If you wish to add any more JVM arguments or tune the existing ones, the
run scripts are the place to do it.

## Pre-configured Options

The distribution contains several standard configuration sets for
running:

-   Non clustered stand-alone.

-   Clustered stand-alone

-   Replicated stand-alone

-   Shared-store stand-alone

You can of course create your own configuration and specify any
configuration when running the run script.

## Library Path

If you're using the [Asynchronous IO Journal](#aio-journal) on Linux,
you need to specify `java.library.path` as a property on your Java
options. This is done automatically in the scripts.

If you don't specify `java.library.path` at your Java options then the
JVM will use the environment variable `LD_LIBRARY_PATH`.

## System properties

ActiveMQ can take a system property on the command line for configuring
logging.

For more information on configuring logging, please see ?.

## Configuration files

The configuration file used to bootstrap the server (e.g.
`bootstrap.xml` by default) references the specific broker configuration
files.

-   `activemq-configuration.xml`. This is the main ActiveMQ
    configuration file. All the parameters in this file are described in
    ?. Please see ? for more information on this file.

-   `activemq-queues.xml`. This file contains predefined queues, queue
    settings and security settings. The file is optional - all this
    configuration can also live in `activemq-configuration.xml`. In
    fact, the default configuration sets do not have a
    `activemq-queues.xml` file. The purpose of allowing queues to be
    configured in these files is to allow you to manage your queue
    configuration over many files instead of being forced to maintain it
    in a single file. There can be many `activemq-queues.xml` files on
    the classpath. All will be loaded if found.

-   `activemq-users.xml` ActiveMQ ships with a basic security manager
    implementation which obtains user credentials from the
    `activemq-users.xml` file. This file contains user, password and
    role information. For more information on security, please see ?.

-   `activemq-jms.xml` The distro configuration by default includes a
    server side JMS service which mainly deploys JMS Queues, Topics and
    ConnectionFactorys from this file into JNDI. If you're not using
    JMS, or you don't need to deploy JMS objects on the server side,
    then you don't need this file. For more information on using JMS,
    please see ?.

> **Note**
>
> The property `file-deployment-enabled` in the
> `activemq-configuration.xml` configuration when set to false means
> that the other configuration files are not loaded. This is true by
> default.

It is also possible to use system property substitution in all the
configuration files. by replacing a value with the name of a system
property. Here is an example of this with a connector configuration:

    <connector name="netty">
       <factory-class>org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory</factory-class>
       <param key="host" value="${activemq.remoting.netty.host:localhost}"/>
       <param key="port" value="${activemq.remoting.netty.port:5445}"/>
    </connector>

Here you can see we have replaced 2 values with system properties
`activemq.remoting.netty.host` and `activemq.remoting.netty.port`. These
values will be replaced by the value found in the system property if
there is one, if not they default back to localhost or 5445
respectively. It is also possible to not supply a default. i.e.
`${activemq.remoting.netty.host}`, however the system property *must* be
supplied in that case.

## Bootstrap File

The stand-alone server is basically a set of POJOs which are
instantiated by Airline commands.

The bootstrap file is very simple. Let's take a look at an example:

    <broker xmlns="http://activemq.org/schema">

       <file:core configuration="${activemq.home}/config/stand-alone/non-clustered/activemq-configuration.xml"></core>
       <file:jms configuration="${activemq.home}/config/stand-alone/non-clustered/activemq-jms.xml"></jms>

       <basic-security/>

    </broker>

-   core - Instantiates a core server using the configuration file from the
    `configuration` attribute. This is the main broker POJO necessary to
    do all the real messaging work.

-   jms - This deploys any JMS Objects such as JMS Queues, Topics and
    ConnectionFactory instances from the `activemq-jms.xml` file
    specified. It also provides a simple management API for manipulating
    JMS Objects. On the whole it just translates and delegates its work
    to the core server. If you don't need to deploy JMS Queues, Topics
    and ConnectionFactories from server side configuration and don't
    require the JMS management interface this can be disabled.

## The main configuration file.

The configuration for the ActiveMQ core server is contained in
`activemq-configuration.xml`. This is what the FileConfiguration bean
uses to configure the messaging server.

There are many attributes which you can configure ActiveMQ. In most
cases the defaults will do fine, in fact every attribute can be
defaulted which means a file with a single empty `configuration` element
is a valid configuration file. The different configuration will be
explained throughout the manual or you can refer to the configuration
reference [here](#configuration-index).
