# AMQP CPP example

ActiveMQ Artemis is a multi protocol broker. It will inspect the initial handshake of clients to determine what protocol to use.

All you need to do is to connect a client into ActiveMQ's configured port and you should be able connect.

To run this example simply run the command **mvn verify -Pexample**, execute the compile.sh script and start the executable called ./hello

    # first make sure you have the dependencies you need to compile and run the client
    # You will have to adapt this step according to your platform. Consult the [qpid docs](http://qpid.apache.org/releases/qpid-0.30/programming/book/) for more information.
    # There is a list of [packages](http://qpid.apache.org/packages.html) you can install as well.
    [proton-cpp]$ sudo yum install qpid-cpp-client-devel

    # on a first window
    [proton-cpp]$ mvn verify -Pexample

    # on a second window
    # That goes without saying but you will of course need g++ (the C++ compiler) installed
    [proton-cpp]$ ./compile.sh
    [proton-cpp]$ ./hello

You don't need to do anything special to configure the ActiveMQ Artemis broker to accept AMQP clients.

Just for the sake of documentation though we are setting the port of ActiveMQ Artemis on this example as 5672 which is the port qpid have by default.

This is totally optional and you don't need to follow this convention. You can use any port you chose including ActiveMQ's 61616 default port:

    <acceptor name="proton-acceptor">tcp://localhost:5672</acceptor>

## Example step-by-step

We are using qpid cpp client on this example. There are several libraries you may chose from for AMQP. We have ellect one that we consider simple enough for users.

This example is based on [qpid's hello world example](http://qpid.apache.org/releases/qpid-0.30/messaging-api/cpp/examples/hello_world.cpp.html).

1.  qpid-cpp-client-devel installed.

Assuming you are on Linux Fedora, you will need to run this following command

    yum install qpid-cpp-client-devel

Consult the [qpid documentation](http://qpid.apache.org/releases/qpid-0.30/programming/book/), and [the required](http://qpid.apache.org/packages.html) packages for information on other platoforms.

5.  Make the proper C++ imports
6.  Create an amqp qpid 1.0 connection.

    std::string broker = argc > 1 ? argv[1] : "localhost:61616";
    std::string address = argc > 2 ? argv[2] : "jms.queue.exampleQueue";
    std::string connectionOptions = argc > 3 ? argv[3] : "{protocol:amqp1.0}"; // Look at the [docs](http://qpid.apache.org/releases/qpid-0.30/programming/book/connections.html#connection-options) for more options

    Connection connection(broker, connectionOptions);
    connection.open();

8.  Create a session

    Session session = connection.createSession();

10.  Create a sender

    Sender sender = session.createSender(address);

12.  send a simple message

    sender.send(Message("Hello world!"));

14.  create a receiver

    Receiver receiver = session.createReceiver(address);

16.  receive the simple message

    Message message = receiver.fetch(Duration::SECOND * 1);

18.  acknowledge the message

    session.acknowledge();

20.  close the connection

    connection.close();