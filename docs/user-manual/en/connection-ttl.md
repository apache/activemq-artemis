# Detecting Dead Connections

In this section we will discuss connection time-to-live (TTL) and
explain how ActiveMQ deals with crashed clients and clients which have
exited without cleanly closing their resources.

## Cleaning up Dead Connection Resources on the Server

Before a ActiveMQ client application exits it is considered good
practice that it should close its resources in a controlled manner,
using a `finally` block.

Here's an example of a well behaved core client application closing its
session and session factory in a finally block:

``` java
ServerLocator locator = null;
ClientSessionFactory sf = null;
ClientSession session = null;

try
{
   locator = ActiveMQClient.createServerLocatorWithoutHA(..);

   sf = locator.createClientSessionFactory();;

   session = sf.createSession(...);

   ... do some stuff with the session...
}
finally
{
   if (session != null)
   {
      session.close();
   }

   if (sf != null)
   {
      sf.close();
   }

   if(locator != null)
   {
      locator.close();
   }
}
```

And here's an example of a well behaved JMS client application:

``` java
Connection jmsConnection = null;

try
{
   ConnectionFactory jmsConnectionFactory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(...);

   jmsConnection = jmsConnectionFactory.createConnection();

   ... do some stuff with the connection...
}
finally
{
   if (connection != null)
   {
      connection.close();
   }
}
```

Unfortunately users don't always write well behaved applications, and
sometimes clients just crash so they don't have a chance to clean up
their resources!

If this occurs then it can leave server side resources, like sessions,
hanging on the server. If these were not removed they would cause a
resource leak on the server and over time this result in the server
running out of memory or other resources.

We have to balance the requirement for cleaning up dead client resources
with the fact that sometimes the network between the client and the
server can fail and then come back, allowing the client to reconnect.
ActiveMQ supports client reconnection, so we don't want to clean up
"dead" server side resources too soon or this will prevent any client
from reconnecting, as it won't be able to find its old sessions on the
server.

ActiveMQ makes all of this configurable. For each `ClientSessionFactory`
we define a *connection TTL*. Basically, the TTL determines how long the
server will keep a connection alive in the absence of any data arriving
from the client. The client will automatically send "ping" packets
periodically to prevent the server from closing it down. If the server
doesn't receive any packets on a connection for the connection TTL time,
then it will automatically close all the sessions on the server that
relate to that connection.

If you're using JMS, the connection TTL is defined by the
`ConnectionTTL` attribute on a `ActiveMQConnectionFactory` instance, or
if you're deploying JMS connection factory instances direct into JNDI on
the server side, you can specify it in the xml config, using the
parameter `connectionTtl`.

The default value for connection ttl on an "unreliable" connection (e.g.
a Netty connection) is `60000`ms, i.e. 1 minute. The default value for
connection ttl on a "reliable" connection (e.g. an in-vm connection) is
`-1`. A value of `-1` for `ConnectionTTL` means the server will never
time out the connection on the server side.

If you do not wish clients to be able to specify their own connection
TTL, you can override all values used by a global value set on the
server side. This can be done by specifying the
`connection-ttl-override` attribute in the server side configuration.
The default value for `connection-ttl-override` is `-1` which means "do
not override" (i.e. let clients use their own values).

## Closing core sessions or JMS connections that you have failed to close

As previously discussed, it's important that all core client sessions
and JMS connections are always closed explicitly in a `finally` block
when you are finished using them.

If you fail to do so, ActiveMQ will detect this at garbage collection
time, and log a warning similar to the following in the logs (If you are
using JMS the warning will involve a JMS connection not a client
session):

    [Finalizer] 20:14:43,244 WARNING [org.apache.activemq.core.client.impl.DelegatingSession]  I'm closing a ClientSession you left open. Please make sure you close all ClientSessions explicitly before let
    ting them go out of scope!
    [Finalizer] 20:14:43,244 WARNING [org.apache.activemq.core.client.impl.DelegatingSession]  The session you didn't close was created here:
    java.lang.Exception
       at org.apache.activemq.core.client.impl.DelegatingSession.<init>(DelegatingSession.java:83)
       at org.acme.yourproject.YourClass (YourClass.java:666)

ActiveMQ will then close the connection / client session for you.

Note that the log will also tell you the exact line of your user code
where you created the JMS connection / client session that you later did
not close. This will enable you to pinpoint the error in your code and
correct it appropriately.

## Detecting failure from the client side.

In the previous section we discussed how the client sends pings to the
server and how "dead" connection resources are cleaned up by the server.
There's also another reason for pinging, and that's for the *client* to
be able to detect that the server or network has failed.

As long as the client is receiving data from the server it will consider
the connection to be still alive.

If the client does not receive any packets for
`client-failure-check-period` milliseconds then it will consider the
connection failed and will either initiate failover, or call any
`FailureListener` instances (or `ExceptionListener` instances if you are
using JMS) depending on how it has been configured.

If you're using JMS it's defined by the `ClientFailureCheckPeriod`
attribute on a `ActiveMQConnectionFactory` instance..

The default value for client failure check period on an "unreliable"
connection (e.g. a Netty connection) is `30000`ms, i.e. 30 seconds. The
default value for client failure check period on a "reliable" connection
(e.g. an in-vm connection) is `-1`. A value of `-1` means the client
will never fail the connection on the client side if no data is received
from the server. Typically this is much lower than connection TTL to
allow clients to reconnect in case of transitory failure.

## Configuring Asynchronous Connection Execution

Most packets received on the server side are executed on the remoting
thread. These packets represent short-running operations and are always
executed on the remoting thread for performance reasons.

However, by default some kinds of packets are executed using a thread
from a thread pool so that the remoting thread is not tied up for too
long. Please note that processing operations asynchronously on another
thread adds a little more latency. These packets are:

-   `org.apache.activemq.core.protocol.core.impl.wireformat.RollbackMessage`

-   `org.apache.activemq.core.protocol.core.impl.wireformat.SessionCloseMessage`

-   `org.apache.activemq.core.protocol.core.impl.wireformat.SessionCommitMessage`

-   `org.apache.activemq.core.protocol.core.impl.wireformat.SessionXACommitMessage`

-   `org.apache.activemq.core.protocol.core.impl.wireformat.SessionXAPrepareMessage`

-   `org.apache.activemq.core.protocol.core.impl.wireformat.SessionXARollbackMessage`

To disable asynchronous connection execution, set the parameter
`async-connection-execution-enabled` in `activemq-configuration.xml` to
`false` (default value is `true`).
