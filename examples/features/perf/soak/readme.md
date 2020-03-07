# Soak Test For Manual Reconnection of JMS Clients *

## Run The Server Standalone

Use the Profile server

    mvn -Pserver verify

That will create a broker under ./target/server0

You can define the property server.dir under the same Profile to create other servers. or you could do it manually if desired using the regular ./artemis create

    mvn -Dserver.dir=server1 -Pserver verify

server1 should contain a copy of configuration equivalent to that found under the server0 director with different
settings.

To run a broker with the same configuration but on a different host.  Check out this source on the host machine and
change:

* `activemq.remoting.netty.host` property in broker.xml

    mvn verify -P server

To run the broker just start it manually

## Configure Server Dump

The broker can "dump" info at regular interval. In broker.xml, set

    <server-dump-interval>10000</server-dump-interval>

to have infos every 10s:

    **** Server Dump ****
    date:            Mon Aug 17 18:19:07 CEST 2009
    free memory:      500,79 MiB
    max memory:       1,95 GiB
    total memory:     507,13 MiB
    available memory: 99,68%
    total paging memory:   0,00 B
    # of thread:     19
    # of conns:      0
    ********************

## Run The Clients

The clients can be run separate from the broker using:

    mvn verify -Premote

Parameters are specified in soak.properties.

The duration of the tests is configured by duration-in-minutes (defaults to 2 minutes, set to -1 to run the test indefinitely).

To configure the soak properties different to the defaults for the clients, use the system property to specify the JNDI broker to connect to, use the system property `jndi.address`:

    mvn verify -Premote -Dsoak.props=<path to properties> -Pjndi.address=jnp:remote.host:1099

Every 1000th message, the clients will display their recent activity:

    INFO: received 10000 messages in 5,71s (total: 55s)

At the end of the run, the sender and receiver will sum up their activity:

    INFO: Received 223364 messages in 2,01 minutes

## Kill The Server And Check Manual Reconnection

You can kill the broker (ctl+c or kill -9), the clients are configured to reconnect indefinitely to the same single broker (even in case of clean shutdown) Once the broker restarts, all the clients will resume their activities after reconnecting to the server.
