# JMS Multiple Failover using Replication Example

To run the example, simply type **mvn verify** from this directory.

This example demonstrates three servers coupled as a live-backup-backup group for high availability (HA) using replication, and a client connection failing over from live to backup when the live broker is crashed and then to the second backup once the new live fails.

For more information on ActiveMQ Artemis failover and HA, and clustering in general, please see the clustering section of the user manual.