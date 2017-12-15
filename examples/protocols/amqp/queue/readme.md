# Proton qpid java example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

ActiveMQ Artemis is a multi protocol broker. It will inspect the initial handshake of clients to determine what protocol to use.

All you need to do is to connect a client into activemq's configured port and you should be able connect.

To run this example simply run the command **mvn verify -Pexample**, execute the `compile.sh` script and start the executable called `./hello`.

You don't need to do anything special to configure the ActiveMQ Artemis broker to accept AMQP clients.

Just for the sake of documentation though we are setting the port of ActiveMQ Artemis on this example as 5672 which is the port qpid have by default.

This is totally optional and you don't need to follow this convention. You can use any port you chose including ActiveMQ's 61616 default port

    <acceptor name="proton-acceptor">tcp://localhost:5672</acceptor>