# Large Message Example

To run the example, simply type **mvn verify** from this directory, This example will start and stop the broker since it will look for a failure.

This example shows you how to send and receive very large messages with ActiveMQ Artemis.

ActiveMQ Artemis supports the sending and receiving of huge messages, much larger than can fit in available RAM on the client or server. Effectively the only limit to message size is the amount of disk space you have on the server.

Large messages are persisted on the broker so they can survive a broker restart. In other words ActiveMQ Artemis doesn't just do a simple socket stream from the sender to the consumer.

In order to do this ActiveMQ Artemis provides an extension to JMS where you can use an InputStream or OutputStream as the source or destination for your messages respectively. You can send messages as large as it would fit in your disk.

You may also choose to read LargeMessages using the regular ByteStream or ByteMessage methods, but using the InputStream or OutputStream will provide you a much better performance.

## Example step-by-step

In this example we limit both the broker and the client to be running in a maximum of 50MB of RAM, and we send a message with a body of size 2GiB.

ActiveMQ Artemis can support much large message sizes but we choose these sizes and limit RAM so the example runs more quickly.

We create a file on disk representing the message body, create a FileInputStream on that file and set that InputStream as the body of the message before sending.

The message is sent, then we stop the server, and restart it. This demonstrates the large message will survive a restart of the server.

Once the broker is restarted we receive the message and stream it's body to another file on disk.