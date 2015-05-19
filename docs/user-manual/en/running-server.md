Prerequisites
=============

> **Note**
>
> ActiveMQ Artemis only runs on Java 7 or later.

By default, ActiveMQ Artemis server runs with 1GiB of memory. If your computer
has less memory, or you want to run it with more available RAM, modify
the value in `bin/run.sh` accordingly.

If you are on Linux you may want to enable libaio For persistence,
ActiveMQ Artemis uses its own fast journal, which you can
configure to use libaio (which is the default when running on Linux) or
Java NIO. In order to use the libaio module on Linux, you'll need to
install libaio, if it's not already installed.

If you're not running on Linux then you don't need to worry about this.

You can install libaio using the following steps as the root user:

Using yum, (e.g. on Fedora or Red Hat Enterprise Linux):

    yum install libaio

Using aptitude, (e.g. on Ubuntu or Debian system):

    apt-get install libaio

Installation
============

After downloading the distribution, the following highlights some important folders on the distribution:

             |___ bin
             |
             |___ web
             |      |___ user-manual
             |      |___ api
             |
             |___ examples
             |      |___ core
             |      |___ javaee
             |      |___ jms
             |
             |___ lib
             |
             |___ schema


-   `bin` -- binaries and scripts needed to run ActiveMQ Artemis.

-   `web` -- The folder where the web context is loaded when ActiveMQ Artemis runs.

-   `user-manual` -- The user manual is placed under the web folder.

-   `api` -- The api documentation is placed under the web folder

-   `examples` -- JMS and Java EE examples. Please refer to the 'running
    examples' chapter for details on how to run them.

-   `lib` -- jars and libraries needed to run ActiveMQ Artemis

-   `licenses` -- licenses for ActiveMQ Artemis

-   `schemas` -- XML Schemas used to validate ActiveMQ Artemis configuration
    files

Creating a Broker Instance
==========================


A broker instance is the directory containing all the configuration and runtime
data, such as logs and data files, associated with a broker process.  It is recommended that
you do *not* create the instance directory under `${ARTEMIS_HOME}`.  This separation is
encouraged so that you can more easily upgrade when the next version of ActiveMQ Artemis is released.

On Unix systems, it is a common convention to store this kind of runtime data under
the `/var/lib` directory.  For example, to create an instance at '/var/lib/mybroker', run
the following commands in your command line shell:

    cd /var/lib
    ${ARTEMIS_HOME}/bin/activemq create mybroker

A broker instance directory will contain the following sub directories:

 * `bin`: holds execution scripts associated with this instance.
 * `etc`: hold the instance configuration files
 * `data`: holds the data files used for storing persistent messages
 * `log`: holds rotating log files
 * `tmp`: holds temporary files that are safe to delete between broker runs

At this point you may want to adjust the default configuration located in
the `etc` directory.

Environment variables are used to provide ease of changing ports, hosts and
data directories used and can be found in `etc/activemq.profile` on linux and
`etc\activemq.profile.cmd` on Windows.


Options
=======

There are several options you can use when creating an instance.

For a full list of updated properties always use:

```
 $./artemis help create
 NAME
         artemis create - creates a new broker instance

 SYNOPSIS
         artemis create [--allow-anonymous]
                 [--cluster-password <clusterPassword>] [--cluster-user <clusterUser>]
                 [--clustered] [--data <data>] [--encoding <encoding>] [--force]
                 [--home <home>] [--host <host>] [--java-options <javaOptions>]
                 [--password <password>] [--port-offset <portOffset>] [--replicated]
                 [--role <role>] [--shared-store] [--silent-input] [--user <user>] [--]
                 <directory>

 OPTIONS
         --allow-anonymous
             Enables anonymous configuration on security (Default: input)

         --cluster-password <clusterPassword>
             The cluster password to use for clustering. (Default: input)

         --cluster-user <clusterUser>
             The cluster user to use for clustering. (Default: input)

         --clustered
             Enable clustering

         --data <data>
             Directory where ActiveMQ Data is used. Path are relative to
             artemis.instance/bin

         --encoding <encoding>
             The encoding that text files should use

         --force
             Overwrite configuration at destination directory

         --home <home>
             Directory where ActiveMQ Artemis is installed

         --host <host>
             The host name of the broker (Default: 0.0.0.0 or input if clustered)

         --java-options <javaOptions>
             Extra java options to be passed to the profile

         --password <password>
             The user's password (Default: input)

         --port-offset <portOffset>
             Off sets the default ports

         --replicated
             Enable broker replication

         --role <role>
             The name for the role created (Default: amq)

         --shared-store
             Enable broker shared store

         --silent-input
             It will disable all the inputs, and it would make a best guess for
             any required input

         --user <user>
             The username (Default: input)

         --
             This option can be used to separate command-line options from the
             list of argument, (useful when arguments might be mistaken for
             command-line options

         <directory>
             The instance directory to hold the broker's configuration and data
```

Some of these properties may be mandatory in certain configurations and the system may ask you for additional input.

```
    ./artemis create /usr/server
    Creating ActiveMQ Artemis instance at: /user/server

    --user: is mandatory with this configuration:
    Please provide the default username:
    admin

    --password: is mandatory with this configuration:
    Please provide the default password:


    --allow-anonymous: is mandatory with this configuration:
    Allow anonymous access? (Y/N):
    y

    You can now start the broker by executing:

       "/user/server/bin/artemis" run

    Or you can run the broker in the background using:

       "/user/server/bin/artemis-service" start
```


Starting and Stopping a Broker Instance
=======================================

Assuming you created the broker instance under `/var/lib/mybroker` all you need
to do start running the broker instance is execute:

    /var/lib/mybroker/bin/activemq run

Now that the broker is running, you can optionally run some of the included
examples to verify the the broker is running properly.

To stop the Apache ActiveMQ Artemis instance you will use the same `activemq` script, but with
the `stop argument`.  Example:

    /var/lib/mybroker/bin/activemq stop

By default the `etc/bootstrap.xml` configuration is
used. The configuration can be changed e.g. by running
`./activemq run -- xml:path/to/bootstrap.xml` or another
config of your choosing.


Windows Server
==============

On windows you will have the option to run ActiveMQ Artemis as a service.
Just use the following command to install it:

```
 $ ./artemis-service.exe install
```


The create process should give you a hint of the available commands available for the artemis-service.exe