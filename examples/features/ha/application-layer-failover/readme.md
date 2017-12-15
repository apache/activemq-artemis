# JMS Application-Layer Failover Example

To run the example, simply type **mvn verify** from this directory. This example will always spawn and stop multiple brokers.

ActiveMQ Artemis implements fully transparent **automatic** failover of connections from a live node to a backup node which requires no special coding. This is described in a different example and requires broker replication.

However, ActiveMQ Artemis also supports **Application-Layer** failover which is useful in the case where replication is not enabled.

With Application-Layer failover, it's up to the application to register a JMS ExceptionListener with ActiveMQ Artemis. This listener will then be called by ActiveMQ Artemis in the event that connection failure is detected.

User code in the ExceptionListener can then recreate any JMS Connection, Session, etc on another node and the application can continue.

Application-Layer failover is an alternative approach to High Availability (HA).

Application-Layer failover differs from automatic failover in that some client side coding is required in order to implement this. Also, with Application-Layer failover, since the old Session object dies and a new is created, any uncommitted work in the old Session will be lost, and any unacknowledged messages might be redelivered.

For more information on ActiveMQ Artemis failover and HA, and clustering in general, please see the clustering section of the user manual.