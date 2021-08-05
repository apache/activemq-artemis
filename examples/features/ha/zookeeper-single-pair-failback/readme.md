# Zookeeper Single Pair Failback Example

## Configuring Zookeeper with Docker

This example demonstrates two servers coupled as a primary-backup pair for high availability (HA) using 
pluggable quorum vote replication Reference Implementation based on [Apache Curator](https://curator.apache.org/) to use 
[Apache Zookeeper](https://zookeeper.apache.org/) as external quorum service.

The example shows a client connection failing over from live to backup when the live broker is crashed and 
then back to the original live when it is restarted (i.e. "failback").

To run the example, simply type **mvn verify** from this directory after running a Zookeeper node at `localhost:2181`.

If no Zookeeper node is configured, can use the commands below (see [Official Zookeeper Docker Image Site](https://hub.docker.com/_/zookeeper) 
for more details on how configure it).

Run Zookeeper `3.6.3` with:
```
$ docker run --name artemis-zk --network host --restart always -d zookeeper:3.6.3
```
By default, the official docker image exposes `2181 2888 3888 8080` as client, follower, election and AdminServer ports.

Verify Zookeeper server is correctly started by running:
```
$ docker logs --follow artemis-zk
```
It should print the Zookeeper welcome ASCII logs:
```
ZooKeeper JMX enabled by default
Using config: /conf/zoo.cfg
2021-08-05 14:29:29,431 [myid:] - INFO  [main:QuorumPeerConfig@174] - Reading configuration from: /conf/zoo.cfg
2021-08-05 14:29:29,434 [myid:] - INFO  [main:QuorumPeerConfig@451] - clientPort is not set
2021-08-05 14:29:29,434 [myid:] - INFO  [main:QuorumPeerConfig@464] - secureClientPort is not set
2021-08-05 14:29:29,434 [myid:] - INFO  [main:QuorumPeerConfig@480] - observerMasterPort is not set
2021-08-05 14:29:29,435 [myid:] - INFO  [main:QuorumPeerConfig@497] - metricsProvider.className is org.apache.zookeeper.metrics.impl.DefaultMetricsProvider
2021-08-05 14:29:29,438 [myid:] - ERROR [main:QuorumPeerConfig@722] - Invalid configuration, only one server specified (ignoring)
2021-08-05 14:29:29,441 [myid:1] - INFO  [main:DatadirCleanupManager@78] - autopurge.snapRetainCount set to 3
2021-08-05 14:29:29,441 [myid:1] - INFO  [main:DatadirCleanupManager@79] - autopurge.purgeInterval set to 0
2021-08-05 14:29:29,441 [myid:1] - INFO  [main:DatadirCleanupManager@101] - Purge task is not scheduled.
2021-08-05 14:29:29,441 [myid:1] - WARN  [main:QuorumPeerMain@138] - Either no config or no quorum defined in config, running in standalone mode
2021-08-05 14:29:29,444 [myid:1] - INFO  [main:ManagedUtil@44] - Log4j 1.2 jmx support found and enabled.
2021-08-05 14:29:29,449 [myid:1] - INFO  [main:QuorumPeerConfig@174] - Reading configuration from: /conf/zoo.cfg
2021-08-05 14:29:29,449 [myid:1] - INFO  [main:QuorumPeerConfig@451] - clientPort is not set
2021-08-05 14:29:29,449 [myid:1] - INFO  [main:QuorumPeerConfig@464] - secureClientPort is not set
2021-08-05 14:29:29,449 [myid:1] - INFO  [main:QuorumPeerConfig@480] - observerMasterPort is not set
2021-08-05 14:29:29,450 [myid:1] - INFO  [main:QuorumPeerConfig@497] - metricsProvider.className is org.apache.zookeeper.metrics.impl.DefaultMetricsProvider
2021-08-05 14:29:29,450 [myid:1] - ERROR [main:QuorumPeerConfig@722] - Invalid configuration, only one server specified (ignoring)
2021-08-05 14:29:29,451 [myid:1] - INFO  [main:ZooKeeperServerMain@122] - Starting server
2021-08-05 14:29:29,459 [myid:1] - INFO  [main:ServerMetrics@62] - ServerMetrics initialized with provider org.apache.zookeeper.metrics.impl.DefaultMetricsProvider@525f1e4e
2021-08-05 14:29:29,461 [myid:1] - INFO  [main:FileTxnSnapLog@124] - zookeeper.snapshot.trust.empty : false
2021-08-05 14:29:29,467 [myid:1] - INFO  [main:ZookeeperBanner@42] - 
2021-08-05 14:29:29,467 [myid:1] - INFO  [main:ZookeeperBanner@42] -   ______                  _                                          
2021-08-05 14:29:29,467 [myid:1] - INFO  [main:ZookeeperBanner@42] -  |___  /                 | |                                         
2021-08-05 14:29:29,467 [myid:1] - INFO  [main:ZookeeperBanner@42] -     / /    ___     ___   | | __   ___    ___   _ __     ___   _ __   
2021-08-05 14:29:29,468 [myid:1] - INFO  [main:ZookeeperBanner@42] -    / /    / _ \   / _ \  | |/ /  / _ \  / _ \ | '_ \   / _ \ | '__|
2021-08-05 14:29:29,468 [myid:1] - INFO  [main:ZookeeperBanner@42] -   / /__  | (_) | | (_) | |   <  |  __/ |  __/ | |_) | |  __/ | |    
2021-08-05 14:29:29,468 [myid:1] - INFO  [main:ZookeeperBanner@42] -  /_____|  \___/   \___/  |_|\_\  \___|  \___| | .__/   \___| |_|
2021-08-05 14:29:29,468 [myid:1] - INFO  [main:ZookeeperBanner@42] -                                               | |                     
2021-08-05 14:29:29,468 [myid:1] - INFO  [main:ZookeeperBanner@42] -                                               |_|                     
2021-08-05 14:29:29,468 [myid:1] - INFO  [main:ZookeeperBanner@42] - 
```
Alternatively, this command could be executed:
```
$ docker run -it --rm --network host zookeeper:3.6.3 zkCli.sh -server localhost:2181
```
Zookeeper server can be reached using localhost:2181 if it output something like:
```
2021-08-05 14:56:03,739 [myid:localhost:2181] - INFO  [main-SendThread(localhost:2181):ClientCnxn$SendThread@1448] - Session establishment complete on server localhost/0:0:0:0:0:0:0:1:2181, session id = 0x100078b8cfc0002, negotiated timeout = 30000

```
Type 
```
[zk: localhost:2181(CONNECTED) 0] quit
```
to quit the client instance.


## Configuring zookeeper bare metal

It is possible to run zooKeeper in a bare metal instance for this example as well.

Simply download [Zookeeper](https://zookeeper.apache.org/releases.html), and use the following zoo.cfg under ./apache-zookeeper/conf:

```shell
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just
# example sakes.
dataDir=/tmp/datazookeeper
# the port at which the clients will connect
clientPort=2181
```

And use one of the shells to start Zookeeper such as:

```shell
# From the bin folder under the apache-zookeeper distribution folder 
$ ./zkServer.sh start-foreground
```

And zookeeper would run normally:

```
2021-08-05 14:10:16,902 [myid:] - INFO  [main:DigestAuthenticationProvider@47] - ACL digest algorithm is: SHA1
2021-08-05 14:10:16,902 [myid:] - INFO  [main:DigestAuthenticationProvider@61] - zookeeper.DigestAuthenticationProvider.enabled = true
2021-08-05 14:10:16,905 [myid:] - INFO  [main:FileTxnSnapLog@124] - zookeeper.snapshot.trust.empty : false
2021-08-05 14:10:16,917 [myid:] - INFO  [main:ZookeeperBanner@42] - 
2021-08-05 14:10:16,917 [myid:] - INFO  [main:ZookeeperBanner@42] -   ______                  _                                          
2021-08-05 14:10:16,917 [myid:] - INFO  [main:ZookeeperBanner@42] -  |___  /                 | |                                         
2021-08-05 14:10:16,917 [myid:] - INFO  [main:ZookeeperBanner@42] -     / /    ___     ___   | | __   ___    ___   _ __     ___   _ __   
2021-08-05 14:10:16,917 [myid:] - INFO  [main:ZookeeperBanner@42] -    / /    / _ \   / _ \  | |/ /  / _ \  / _ \ | '_ \   / _ \ | '__|
2021-08-05 14:10:16,917 [myid:] - INFO  [main:ZookeeperBanner@42] -   / /__  | (_) | | (_) | |   <  |  __/ |  __/ | |_) | |  __/ | |    
2021-08-05 14:10:16,918 [myid:] - INFO  [main:ZookeeperBanner@42] -  /_____|  \___/   \___/  |_|\_\  \___|  \___| | .__/   \___| |_|
2021-08-05 14:10:16,918 [myid:] - INFO  [main:ZookeeperBanner@42] -                                               | |                     
2021-08-05 14:10:16,918 [myid:] - INFO  [main:ZookeeperBanner@42] -                                               |_|                     
2021-08-05 14:10:16,918 [myid:] - INFO  [main:ZookeeperBanner@42] - 
```


## Configured the brokers

The 2 brokers of this example are already configured to connect to a single Zookeeper node at the mentioned address, thanks to the XML configuration of their `manager`:
```xml
<manager>
   <properties>
      <property key="connect-string" value="localhost:2181"/>
      <property key="namespace" value="examples"/>
      <property key="session-ms" value="18000"/>
  </properties>
</manager>
```
**NOTE** the `namespace` parameter is used to separate the pair information from others if the Zookeeper node is shared with other applications.

**WARNING** As already recommended on the [High Availability section](https://activemq.apache.org/components/artemis/documentation/latest/ha.html), a production environment needs >= 3 nodes to protect against network partitions.


##Running the example

After Zookeeper is started accordingly to any of the portrayed steps here, this example can be run with
```shell
$ mvn verify
```

```
ZookeeperSinglePairFailback-primary-out:2021-08-05 14:15:50,052 INFO  [org.apache.activemq.artemis.core.server] AMQ221020: Started KQUEUE Acceptor at localhost:61616 for protocols [CORE,MQTT,AMQP,HORNETQ,STOMP,OPENWIRE]
server tcp://localhost:61616 started
Started primary
Got message: This is text message 20 (redelivered?: false)
Got message: This is text message 21 (redelivered?: false)
Got message: This is text message 22 (redelivered?: false)
Got message: This is text message 23 (redelivered?: false)
Got message: This is text message 24 (redelivered?: false)
Got message: This is text message 25 (redelivered?: false)
Got message: This is text message 26 (redelivered?: false)
Got message: This is text message 27 (redelivered?: false)
Got message: This is text message 28 (redelivered?: false)
Got message: This is text message 29 (redelivered?: false)
Acknowledged 3d third of messages
**********************************
Killing server java.lang.UNIXProcess@dd025d9
**********************************
**********************************
Killing server java.lang.UNIXProcess@3bea478e
**********************************
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  36.629 s
[INFO] Finished at: 2021-08-05T14:15:56-04:00
[INFO] ------------------------------------------------------------------------
clebertsuconic@MacBook-Pro zookeeper-single-pair-failback % 
```