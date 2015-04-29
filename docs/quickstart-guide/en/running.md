Creating a Broker Instance
==========================


A broker instance is the directory containing all the configuration and runtime
data, such as logs and data files, associated with a broker process.  It is recommended that
you do *not* create the instance directory under `${ARTEMIS_HOME}`.  This separation is
encouraged so that you can more easily upgrade when the next version of ActiveMQ is released.

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


