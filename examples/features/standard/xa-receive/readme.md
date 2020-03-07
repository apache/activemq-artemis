# JMS XA Receive Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates receiving a message within the scope of an XA transaction. When using an XA transaction the message will only be acknowledged and removed from the queue when the transaction is committed. If the transaction is not committed the message maybe redelivered after rollback or during XA recovery.

ActiveMQ Artemis is JTA aware, meaning you can use ActiveMQ Artemis in an XA transactional environment and participate in XA transactions. It provides the javax.transaction.xa.XAResource interface for that purpose. Users can get a XAConnectionFactory to create XAConnections and XASessions.

In this example we simulate a transaction manager to control the transactions. First we create an XASession for receiving and a normal session for sending. Then we start a new xa transaction and enlist the receiving XASession through its XAResource. We then send two words, 'hello' and 'world', receive them, and let the transaction roll back. The received messages are cancelled back to the queue. Next we start a new transaction with the same XAResource enlisted, but this time we commit the transaction after receiving the messages. Then we check that no more messages are to be received.