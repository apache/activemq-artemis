# AMQP Broker Connection with Senders

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to create and start the broker manually.

This example demonstrates how you can create a broker connection from one broker towards another broker, and send messages from that broker towards the target server.

You basically configured the broker connection on broker.xml and this example will give you two working servers where you send messages in one broker and receive it on another broker.
