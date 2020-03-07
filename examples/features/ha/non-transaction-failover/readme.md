# JMS Non Transaction Failover Example

To run the example, simply type **mvn verify** from this directory.

This example demonstrates two servers coupled as a live-backup pair for high availability (HA), and a client connection failing over from live to backup when the live broker is crashed.

Failover behavior differs whether the JMS session is transacted or not.

When a _non-transacted_ JMS session is used, once and only once delivery is not guaranteed and it is possible some messages will be lost or delivered twice, depending when the failover to the backup broker occurs.

It is up to the client to deal with such cases. To ensure once and only once delivery, the client must use transacted JMS sessions (as shown in the "transaction-failover" example).

For more information on ActiveMQ Artemis failover and HA, and clustering in general, please see the clustering section of the user manual.