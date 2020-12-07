# Restart Sequence

Apache ActiveMQ Artemis ships with 2 architectures for providing HA features.
The master and slave brokers can be configured either using network replication
or using shared storage. This document will share restart sequences for the
brokers under various circumstances when the client applications are 
connected to it.

## Restarting 1 broker at a time
When restarting the brokers one at a time at regular intervals, it is not
important to follow any sequence. We just need to make sure that atleast
1 broker in the master/slave pair is live to take up the connections from 
the client applications.

#### Note on restarting
> While restarting the brokers while the client applications are connected 
kindly make sure that atleast one broker is always live to serve the connected 
clients.

## Completely shutting down the brokers and starting
If there is situation that we need to completely shutdown the brokers and 
start them again, please follow the following procedure:

1. Shut down all the slave brokers.
2. Shut down all the master brokers.
3. Start all the master brokers.
4. Start all the slave brokers.

This sequence is particularly important in case of network replication for 
the following reasons:
If the master broker is shutdown first, the slave broker will come live and accept 
all the client connections. Then when the slave broker is stopped, the clients will 
remain connected to the last live connection i.e. slave. Now, when we start the slave 
and master brokers, the clients will keep trying to connecting to the last connection 
i.e. with slave and will never be able to connect until we restart the client applications. 
To avoid the hassle of restarting of client applications, we must follow the sequence 
as suggested above.

## Split-brain situation
The following procedure helps the cluster to recover from the split-brain situation 
and getting the client connections auto-reconnected to the cluster.
With this sequence, client applications do not need to be restarted in order to make 
connection with the brokers.

During the split brain situation both the master and slave brokers are live and there is 
no replication that is happening from the master broker to the slave.

In such situation, there can be some client applications that are connected to the master 
broker and other connected to the slave broker. Now after we restart the brokers and the 
cluster is properly formed.

Here, the clients that were connected to the master broker during the split brain situation 
are auto-connected to the cluster and start processing the messages. But the clients that got 
connected to the slave broker are still trying to make connection with the broker. This happens 
because the slave broker has restarted in 'back up' mode.

Thus, not all the clients get connected to the brokers and function properly.

To avoid such mishap, kindly follow the below sequence:
1. Stop the slave broker
2. Start the slave broker. Observe the logs for the message "Waiting for the master"
3. Stop the master broker.
4. Start the master broker.
   Observe the master broker logs for "Server is live"
   Observe the slave broker logs for "backup announced"
5. Stop the master broker again. Wait until the slave broker becomes live. Observe that all the 
   clients are connected to the slave broker.
6. Start the master broker. This time, all the connections will be switched to master broker again,

#### Note on delta message loss on the slave broker

> During the split brain situation, messages are produced on the slave broker since it is live. 
While resolving the split brain situation, if there are some delta messages that are not produced 
on the slave broker. Those messages cannot be auto-recovered. There will be manual intervention 
required to retrieve the messages, sometime it is almost impossible to recover the messages.
> The above mentioned sequence helps in forming the cluster that was broken due to split brain 
and getting all the client applications to auto connected to the cluster without any need for 
client applications to be restarted. 
