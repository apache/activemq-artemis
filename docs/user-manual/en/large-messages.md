# Large Messages

Apache ActiveMQ Artemis supports sending and receiving of huge messages, even when the
client and server are running with limited memory. The only realistic
limit to the size of a message that can be sent or consumed is the
amount of disk space you have available. We have tested sending and
consuming messages up to 8 GiB in size with a client and server running
in just 50MiB of RAM!

To send a large message, the user can set an `InputStream` on a message
body, and when that message is sent, Apache ActiveMQ Artemis will read the
`InputStream`. A `FileInputStream` could be used for example to send a
huge message from a huge file on disk.

As the `InputStream` is read the data is sent to the server as a stream
of fragments. The server persists these fragments to disk as it receives
them and when the time comes to deliver them to a consumer they are read
back of the disk, also in fragments and sent down the wire. When the
consumer receives a large message it initially receives just the message
with an empty body, it can then set an `OutputStream` on the message to
stream the huge message body to a file on disk or elsewhere. At no time
is the entire message body stored fully in memory, either on the client
or the server.

## Configuring the server

Large messages are stored on a disk directory on the server side, as
configured on the main configuration file.

The configuration property `large-messages-directory` specifies where
large messages are stored.  For JDBC persistence the `large-message-table`
should be configured.

    <configuration xmlns="urn:activemq"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="urn:activemq /schema/artemis-server.xsd">
    ...
    <large-messages-directory>/data/large-messages</large-messages-directory>
    ...
    </configuration

By default the large message directory is `data/largemessages` and `large-message-table` is
configured as "LARGE_MESSAGE_TABLE".

For the best performance we recommend using file store with large messages directory stored
on a different physical volume to the message journal or paging directory.

## Configuring Parameters

Any message larger than a certain size is considered a large message.
Large messages will be split up and sent in fragments. This is
determined by the URL parameter `minLargeMessageSize`

> **Note**
>
> Apache ActiveMQ Artemis messages are encoded using 2 bytes per character so if the
> message data is filled with ASCII characters (which are 1 byte) the
> size of the resulting Apache ActiveMQ Artemis message would roughly double. This is
> important when calculating the size of a "large" message as it may
> appear to be less than the `minLargeMessageSize` before it is sent,
> but it then turns into a "large" message once it is encoded.

The default value is 100KiB.

[Configuring the transport directly from the client side](configuring-transports.md)
will provide more information on how to instantiate the core session factory
or JMS connection factory.

### Compressed Large Messages

You can choose to send large messages in compressed form using
`compressLargeMessages` URL parameter.

#### `compressLargeMessages`

If you specify the boolean URL parameter `compressLargeMessages` as true,
The system will use the ZIP algorithm to compress the message body as
the message is transferred to the server's side. Notice that there's no
special treatment at the server's side, all the compressing and uncompressing
is done at the client.

If the compressed size of a large message is below `minLargeMessageSize`,
it is sent to server as regular messages. This means that the message won't
be written into the server's large-message data directory, thus reducing the
disk I/O.

## Streaming large messages

Apache ActiveMQ Artemis supports setting the body of messages using input and output
streams (`java.lang.io`)

These streams are then used directly for sending (input streams) and
receiving (output streams) messages.

When receiving messages there are 2 ways to deal with the output stream;
you may choose to block while the output stream is recovered using the
method `ClientMessage.saveOutputStream` or alternatively using the
method `ClientMessage.setOutputstream` which will asynchronously write
the message to the stream. If you choose the latter the consumer must be
kept alive until the message has been fully received.

You can use any kind of stream you like. The most common use case is to
send files stored in your disk, but you could also send things like JDBC
Blobs, `SocketInputStream`, things you recovered from `HTTPRequests`
etc. Anything as long as it implements `java.io.InputStream` for sending
messages or `java.io.OutputStream` for receiving them.

### Streaming over Core API

The following table shows a list of methods available at `ClientMessage`
which are also available through JMS by the use of object properties.

<table summary="org.hornetq.api.core.client.ClientMessage API" border="1">
    <colgroup>
        <col/>
        <col/>
        <col/>
        <col/>
    </colgroup>
    <thead>
    <tr>
        <th>Name</th>
        <th>Description</th>
        <th>JMS Equivalent</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>setBodyInputStream(InputStream)</td>
        <td>Set the InputStream used to read a message body when sending it.</td>
        <td>JMS_AMQ_InputStream</td>
    </tr>
    <tr>
        <td>setOutputStream(OutputStream)</td>
        <td>Set the OutputStream that will receive the body of a message. This method does not block.</td>
        <td>JMS_AMQ_OutputStream</td>
    </tr>
    <tr>
        <td>saveOutputStream(OutputStream)</td>
        <td>Save the body of the message to the `OutputStream`. It will block until the entire content is transferred to the `OutputStream`.</td>
        <td>JMS_AMQ_SaveStream</td>
    </tr>
    </tbody>
</table>

To set the output stream when receiving a core message:

``` java
ClientMessage msg = consumer.receive(...);


// This will block here until the stream was transferred
msg.saveOutputStream(someOutputStream);

ClientMessage msg2 = consumer.receive(...);

// This will not wait the transfer to finish
msg2.setOutputStream(someOtherOutputStream);
```

Set the input stream when sending a core message:

``` java
ClientMessage msg = session.createMessage();
msg.setInputStream(dataInputStream);
```

Notice also that for messages with more than 2GiB the getBodySize() will
return invalid values since this is an integer (which is also exposed to
the JMS API). On those cases you can use the message property
_AMQ_LARGE_SIZE.

### Streaming over JMS

When using JMS, Apache ActiveMQ Artemis maps the streaming methods on the core API (see
ClientMessage API table above) by setting object properties . You can use the method
`Message.setObjectProperty` to set the input and output streams.

The `InputStream` can be defined through the JMS Object Property
JMS_AMQ_InputStream on messages being sent:

``` java
BytesMessage message = session.createBytesMessage();

FileInputStream fileInputStream = new FileInputStream(fileInput);

BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);

message.setObjectProperty("JMS_AMQ_InputStream", bufferedInput);

someProducer.send(message);
```

The `OutputStream` can be set through the JMS Object Property
JMS_AMQ_SaveStream on messages being received in a blocking way.

``` java
BytesMessage messageReceived = (BytesMessage)messageConsumer.receive(120000);

File outputFile = new File("huge_message_received.dat");

FileOutputStream fileOutputStream = new FileOutputStream(outputFile);

BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutputStream);

// This will block until the entire content is saved on disk
messageReceived.setObjectProperty("JMS_AMQ_SaveStream", bufferedOutput);
```

Setting the `OutputStream` could also be done in a non blocking way
using the property JMS_AMQ_OutputStream.

``` java
// This won't wait the stream to finish. You need to keep the consumer active.
messageReceived.setObjectProperty("JMS_AMQ_OutputStream", bufferedOutput);
```

> **Note**
>
> When using JMS, Streaming large messages are only supported on
> `StreamMessage` and `BytesMessage`.

## Streaming Alternative

If you choose not to use the `InputStream` or `OutputStream` capability
of Apache ActiveMQ Artemis You could still access the data directly in an alternative
fashion.

On the Core API just get the bytes of the body as you normally would.

``` java
ClientMessage msg = consumer.receive();

byte[] bytes = new byte[1024];
for (int i = 0 ;  i < msg.getBodySize(); i += bytes.length)
{
   msg.getBody().readBytes(bytes);
   // Whatever you want to do with the bytes
}
```

If using JMS API, `BytesMessage` and `StreamMessage` also supports it
transparently.
``` java
BytesMessage rm = (BytesMessage)cons.receive(10000);

byte data[] = new byte[1024];

for (int i = 0; i < rm.getBodyLength(); i += 1024)
{
   int numberOfBytes = rm.readBytes(data);
   // Do whatever you want with the data
}
```

## Large message example

Please see the [examples](examples.md) chapter for an example which shows
how large message is configured and used with JMS.
