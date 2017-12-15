# JMS XA Heuristic Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to make an XA heuristic decision through the ActiveMQ Artemis Management Interface.

A heuristic decision is a unilateral decision to commit or rollback an XA transaction branch after it has been prepared.

In this example we simulate a transaction manager to control the transactions. First we create an XASession and enlist it in a transaction through its XAResource. We then send a text message, 'hello' and end/prepare the transaction on the XAResource, but neither commit nor roll back the transaction. Another transaction is created and associated with the same XAResource, and a second message, 'world' is sent on behalf of the second transaction. Again we leave the second transaction in prepare state. Then we get the MBeanServerConnection object to manipulate the prepared transactions. To illustrate, we roll back the first transaction but commit the second. This will result in that only the message 'world' is received.

This example uses JMX to manipulate transactions in a ActiveMQ Artemis Server. For details on JMX facilities with ActiveMQ Artemis, please look at the JMX Example.