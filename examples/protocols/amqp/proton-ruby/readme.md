# Proton Ruby Example

ActiveMQ Artemis can be configured to accept requests from any AMQP client that supports the 1.0 version of the protocol. This example shows a simply proton ruby client that sends and receives messages.

To run the example you will need the following packages installed, alsa-lib.i686 libXv.i686 libXScrnSaver.i686 qt.i686 qt-x11.i686 qtwebkit-2.2.2-2.fc18.i686, gcc, ruby.

On fedora you can install these via the `yum install alsa-lib.i686 libXv.i686 libXScrnSaver.i686 qt.i686 qt-x11.i686 qtwebkit-2.2.2-2.fc18.i686, gcc, ruby` command.

you will also need the qpid-proton libraries installed, again `yum install qpid-proton`.

lastly you wull have to create the gems `gem install qpid_proton`.

To configure ActiveMQ Artemis to accept AMQP client connections you need to add an Acceptor like so:

    <acceptor name="proton-acceptor">tcp://localhost:5672?protocols=AMQP</acceptor>

## Example step-by-step

Firstly create the broker by running the command **mvn verify**.

Start the broker manually under ./target/server1/bin by calling `./artemis run`.

Then in a separate window you can run the send ruby script by running the command `ruby src/main/scripts/send.rb`.

You can then receive the message via the receive ruby script by running `ruby src/main/scripts/receive.rb`.