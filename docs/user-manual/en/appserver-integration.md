# Application Server Integration and Java EE

ActiveMQ can be easily installed in JBoss Application Server 4 or later.
For details on installing ActiveMQ in the JBoss Application Server
please refer to quick-start guide.

Since ActiveMQ also provides a JCA adapter, it is also possible to
integrate ActiveMQ as a JMS provider in other JEE compliant app servers.
For instructions on how to integrate a remote JCA adaptor into another
application sever, please consult the other application server's
instructions.

A JCA Adapter basically controls the inflow of messages to
Message-Driven Beans (MDBs) and the outflow of messages sent from other
JEE components, e.g. EJBs and Servlets.

This section explains the basics behind configuring the different JEE
components in the AS.

## Configuring Message-Driven Beans

The delivery of messages to an MDB using ActiveMQ is configured on the
JCA Adapter via a configuration file `ra.xml` which can be found under
the `jms-ra.rar` directory. By default this is configured to consume
messages using an InVM connector from the instance of ActiveMQ running
within the application server. The configuration properties are listed
later in this chapter.

All MDBs however need to have the destination type and the destination
configured. The following example shows how this can be done using
annotations:

``` java
@MessageDriven(name = "MDBExample", activationConfig =
{
   @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
   @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue")
})
@ResourceAdapter("activemq-ra.rar")
public class MDBExample implements MessageListener
{
   public void onMessage(Message message)...
}
```

In this example you can see that the MDB will consume messages from a
queue that is mapped into JNDI with the binding `queue/testQueue`. This
queue must be preconfigured in the usual way using the ActiveMQ
configuration files.

The `ResourceAdapter` annotation is used to specify which adaptor should
be used. To use this you will need to import
`org.jboss.ejb3.annotation.ResourceAdapter` for JBoss AS 5.X and later
version which can be found in the `jboss-ejb3-ext-api.jar` which can be
found in the JBoss repository. For JBoss AS 4.X, the annotation to use
is `org.jboss.annotation.ejb.ResourceAdaptor`.

Alternatively you can add use a deployment descriptor and add something
like the following to `jboss.xml`

    <message-driven>
       <ejb-name>ExampleMDB</ejb-name>
       <resource-adapter-name>activemq-ra.rar</resource-adapter-name>
    </message-driven>

You can also rename the activemq-ra.rar directory to jms-ra.rar and
neither the annotation or the extra descriptor information will be
needed. If you do this you will need to edit the `jms-ds.xml` datasource
file and change `rar-name` element.

> **Note**
>
> ActiveMQ is the default JMS provider for JBoss AS 6. Starting with
> this AS version, ActiveMQ resource adapter is named `jms-ra.rar` and
> you no longer need to annotate the MDB for the resource adapter name.

All the examples shipped with the ActiveMQ distribution use the
annotation.

### Using Container-Managed Transactions

When an MDB is using Container-Managed Transactions (CMT), the delivery
of the message is done within the scope of a JTA transaction. The commit
or rollback of this transaction is controlled by the container itself.
If the transaction is rolled back then the message delivery semantics
will kick in (by default, it will try to redeliver the message up to 10
times before sending to a DLQ). Using annotations this would be
configured as follows:

``` java
@MessageDriven(name = "MDB_CMP_TxRequiredExample", activationConfig =
{
   @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
   @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue")
})
@TransactionManagement(value= TransactionManagementType.CONTAINER)
@TransactionAttribute(value= TransactionAttributeType.REQUIRED)
@ResourceAdapter("activemq-ra.rar")
public class MDB_CMP_TxRequiredExample implements MessageListener
{
   public void onMessage(Message message)...
}
```

The `TransactionManagement` annotation tells the container to manage the
transaction. The `TransactionAttribute` annotation tells the container
that a JTA transaction is required for this MDB. Note that the only
other valid value for this is `TransactionAttributeType.NOT_SUPPORTED`
which tells the container that this MDB does not support JTA
transactions and one should not be created.

It is also possible to inform the container that it must rollback the
transaction by calling `setRollbackOnly` on the `MessageDrivenContext`.
The code for this would look something like:

``` java
@Resource
MessageDrivenContextContext ctx;

public void onMessage(Message message)
{
   try
   {
      //something here fails
   }
   catch (Exception e)
   {
      ctx.setRollbackOnly();
   }
}
```

If you do not want the overhead of an XA transaction being created every
time but you would still like the message delivered within a transaction
(i.e. you are only using a JMS resource) then you can configure the MDB
to use a local transaction. This would be configured as such:

``` java
@MessageDriven(name = "MDB_CMP_TxLocalExample", activationConfig =
{
      @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
      @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue"),
      @ActivationConfigProperty(propertyName = "useLocalTx", propertyValue = "true")
})
@TransactionManagement(value = TransactionManagementType.CONTAINER)
@TransactionAttribute(value = TransactionAttributeType.NOT_SUPPORTED)
@ResourceAdapter("activemq-ra.rar")
public class MDB_CMP_TxLocalExample implements MessageListener
{
   public void onMessage(Message message)...
}
```

### Using Bean-Managed Transactions

Message-driven beans can also be configured to use Bean-Managed
Transactions (BMT). In this case a User Transaction is created. This
would be configured as follows:

``` java
@MessageDriven(name = "MDB_BMPExample", activationConfig =
{
   @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
   @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue"),
   @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Dups-ok-acknowledge")
})
@TransactionManagement(value= TransactionManagementType.BEAN)
@ResourceAdapter("activemq-ra.rar")
public class MDB_BMPExample implements MessageListener
{
   public void onMessage(Message message)
}
```

When using Bean-Managed Transactions the message delivery to the MDB
will occur outside the scope of the user transaction and use the
acknowledge mode specified by the user with the `acknowledgeMode`
property. There are only 2 acceptable values for this `Auto-acknowledge`
and `Dups-ok-acknowledge`. Please note that because the message delivery
is outside the scope of the transaction a failure within the MDB will
not cause the message to be redelivered.

A user would control the life-cycle of the transaction something like
the following:

``` java
@Resource
MessageDrivenContext ctx;

public void onMessage(Message message)
{
   UserTransaction tx;
   try
   {
      TextMessage textMessage = (TextMessage)message;

      String text = textMessage.getText();

      UserTransaction tx = ctx.getUserTransaction();

      tx.begin();

      //do some stuff within the transaction

      tx.commit();

   }
   catch (Exception e)
   {
      tx.rollback();
   }
}
```

### Using Message Selectors with Message-Driven Beans

It is also possible to use MDBs with message selectors. To do this
simple define your message selector as follows:

``` java
@MessageDriven(name = "MDBMessageSelectorExample", activationConfig =
{
   @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
   @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue"),
   @ActivationConfigProperty(propertyName = "messageSelector", propertyValue = "color = 'RED'")
})
@TransactionManagement(value= TransactionManagementType.CONTAINER)
@TransactionAttribute(value= TransactionAttributeType.REQUIRED)
@ResourceAdapter("activemq-ra.rar")
public class MDBMessageSelectorExample implements MessageListener
{
   public void onMessage(Message message)....
}
```

## Sending Messages from within JEE components

The JCA adapter can also be used for sending messages. The Connection
Factory to use is configured by default in the `jms-ds.xml` file and is
mapped to `java:/JmsXA`. Using this from within a JEE component will
mean that the sending of the message will be done as part of the JTA
transaction being used by the component.

This means that if the sending of the message fails the overall
transaction would rollback and the message be re-sent. Heres an example
of this from within an MDB:

``` java
@MessageDriven(name = "MDBMessageSendTxExample", activationConfig =
{
   @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
   @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue")
})
@TransactionManagement(value= TransactionManagementType.CONTAINER)
@TransactionAttribute(value= TransactionAttributeType.REQUIRED)
@ResourceAdapter("activemq-ra.rar")
public class MDBMessageSendTxExample implements MessageListener
{
   @Resource(mappedName = "java:/JmsXA")
   ConnectionFactory connectionFactory;

   @Resource(mappedName = "queue/replyQueue")
   Queue replyQueue;

   public void onMessage(Message message)
   {
      Connection conn = null;
      try
      {
         //Step 9. We know the client is sending a text message so we cast
         TextMessage textMessage = (TextMessage)message;

         //Step 10. get the text from the message.
         String text = textMessage.getText();

         System.out.println("message " + text);

         conn = connectionFactory.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = sess.createProducer(replyQueue);

         producer.send(sess.createTextMessage("this is a reply"));

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if(conn != null)
         {
            try
            {
               conn.close();
            }
            catch (JMSException e)
            { 
            }
         }
      }
   }
}
```

In JBoss Application Server you can use the JMS JCA adapter for sending
messages from EJBs (including Session, Entity and Message-Driven Beans),
Servlets (including jsps) and custom MBeans.

## MDB and Consumer pool size

Most application servers, including JBoss, allow you to configure how
many MDB's there are in a pool. In JBoss this is configured via the
`MaxPoolSize` parameter in the ejb3-interceptors-aop.xml file.
Configuring this has no actual effect on how many sessions/consumers
there actually are created. This is because the Resource Adaptor
implementation knows nothing about the application servers MDB
implementation. So even if you set the MDB pool size to 1, 15
sessions/consumers will be created (this is the default). If you want to
limit how many sessions/consumers are created then you need to set the
`maxSession` parameter either on the resource adapter itself or via an
an Activation Config Property on the MDB itself

``` java
@MessageDriven(name = "MDBMessageSendTxExample", activationConfig =
{
   @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
   @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue"),
   @ActivationConfigProperty(propertyName = "maxSession", propertyValue = "1")
})
@TransactionManagement(value= TransactionManagementType.CONTAINER)
@TransactionAttribute(value= TransactionAttributeType.REQUIRED)
@ResourceAdapter("activemq-ra.rar")
public class MyMDB implements MessageListener
{ ....}
```
          

## Configuring the JCA Adaptor

The Java Connector Architecture (JCA) Adapter is what allows ActiveMQ to
be integrated with JEE components such as MDBs and EJBs. It configures
how components such as MDBs consume messages from the ActiveMQ server
and also how components such as EJBs or Servlets can send messages.

The ActiveMQ JCA adapter is deployed via the `jms-ra.rar` archive. The
configuration of the adapter is found in this archive under
`META-INF/ra.xml`.

The configuration will look something like the following:

    <resourceadapter>
       <resourceadapter-class>org.apache.activemq.ra.ActiveMQResourceAdapter</resourceadapter-class>
       <config-property>
          <description>The transport type. Multiple connectors can be configured by using a comma separated list,
             i.e. org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory,org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory.</description>
          <config-property-name>ConnectorClassName</config-property-name>
          <config-property-type>java.lang.String</config-property-type>
          <config-property-value>org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory</config-property-value>
       </config-property>
       <config-property>
          <description>The transport configuration. These values must be in the form of key=val;key=val;,
             if multiple connectors are used then each set must be separated by a comma i.e. host=host1;port=5445,host=host2;port=5446.
             Each set of parameters maps to the connector classname specified.</description>
          <config-property-name>ConnectionParameters</config-property-name>
          <config-property-type>java.lang.String</config-property-type>
          <config-property-value>server-id=0</config-property-value>
       </config-property>

       <outbound-resourceadapter>
          <connection-definition>
             <managedconnectionfactory-class>org.apache.activemq.ra.ActiveMQRAManagedConnection
             Factory</managedconnectionfactory-class>

             <config-property>
                <description>The default session type</description>
                <config-property-name>SessionDefaultType</config-property-name>
                <config-property-type>java.lang.String</config-property-type>
                <config-property-value>javax.jms.Queue</config-property-value>
             </config-property>
             <config-property>
                <description>Try to obtain a lock within specified number of seconds; less
                than or equal to 0 disable this functionality</description>
                <config-property-name>UseTryLock</config-property-name>
                <config-property-type>java.lang.Integer</config-property-type>
                <config-property-value>0</config-property-value>
             </config-property>

             <connectionfactory-interface>org.apache.activemq.ra.ActiveMQRAConnectionFactory
             </connectionfactory-interface>
             <connectionfactororg.apache.activemq.ra.ActiveMQConnectionFactoryImplonFactoryImpl
             </connectionfactory-impl-class>
             <connection-interface>javax.jms.Session</connection-interface>
             <connection-impl-class>org.apache.activemq.ra.ActiveMQRASession
             </connection-impl-class>
          </connection-definition>
          <transaction-support>XATransaction</transaction-support>
          <authentication-mechanism>
             <authentication-mechanism-type>BasicPassword
             </authentication-mechanism-type>
             <credential-interface>javax.resource.spi.security.PasswordCredential
             </credential-interface>
          </authentication-mechanism>
          <reauthentication-support>false</reauthentication-support>
       </outbound-resourceadapter>

       <inbound-resourceadapter>
          <messageadapter>
             <messagelistener>
                <messagelistener-type>javax.jms.MessageListener</messagelistener-type>
                <activationspec>
                   <activationspec-class>org.apache.activemq.ra.inflow.ActiveMQActivationSpec
                   </activationspec-class>
                   <required-config-property>
                       <config-property-name>destination</config-property-name>
                   </required-config-property>
                </activationspec>
             </messagelistener>
          </messageadapter>
       </inbound-resourceadapter>
    </resourceadapter>

There are three main parts to this configuration.

1.  A set of global properties for the adapter

2.  The configuration for the outbound part of the adapter. This is used
    for creating JMS resources within EE components.

3.  The configuration of the inbound part of the adapter. This is used
    for controlling the consumption of messages via MDBs.

### Global Properties

The first element you see is `resourceadapter-class` which should be
left unchanged. This is the ActiveMQ resource adapter class.

After that there is a list of configuration properties. This will be
where most of the configuration is done. The first two properties
configure the transport used by the adapter and the rest configure the
connection factory itself.

> **Note**
>
> All connection factory properties will use the defaults if they are
> not provided, except for the `reconnectAttempts` which will default to
> -1. This signifies that the connection should attempt to reconnect on
> connection failure indefinitely. This is only used when the adapter is
> configured to connect to a remote server as an InVM connector can
> never fail.

The following table explains what each property is for.

  Property Name                                                               Property Type   Property Description
  --------------------------------------------------------------------------- --------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  ConnectorClassName                                                          String          The Connector class name (see [Configuring the Transport](configuring-transports.md) for more information). If multiple connectors are needed this should be provided as a comma separated list.
  ConnectionParameters                                                        String          The transport configuration. These parameters must be in the form of `key1=val1;key2=val2;` and will be specific to the connector used. If multiple connectors are configured then parameters should be supplied for each connector separated by a comma.
  ha                                                                          boolean         True if high availability is needed.
  useLocalTx                                                                  boolean         True will enable local transaction optimisation.
  UserName                                                                    String          The user name to use when making a connection
  Password                                                                    String          The password to use when making a connection
  [DiscoveryAddress](#configuration.discovery-group.group-address)            String          The discovery group address to use to auto-detect a server
  [DiscoveryPort](#configuration.discovery-group.group-port)                  Integer         The port to use for discovery
  [DiscoveryRefreshTimeout](#configuration.discovery-group.refresh-timeout)   Long            The timeout, in milliseconds, to refresh.
  DiscoveryInitialWaitTimeout                                                 Long            The initial time to wait for discovery.
  ConnectionLoadBalancingPolicyClassName                                      String          The load balancing policy class to use.
  ConnectionTTL                                                               Long            The time to live (in milliseconds) for the connection.
  CallTimeout                                                                 Long            the call timeout (in milliseconds) for each packet sent.
  DupsOKBatchSize                                                             Integer         the batch size (in bytes) between acknowledgements when using DUPS\_OK\_ACKNOWLEDGE mode
  TransactionBatchSize                                                        Integer         the batch size (in bytes) between acknowledgements when using a transactional session
  ConsumerWindowSize                                                          Integer         the window size (in bytes) for consumer flow control
  ConsumerMaxRate                                                             Integer         the fastest rate a consumer may consume messages per second
  ConfirmationWindowSize                                                      Integer         the window size (in bytes) for reattachment confirmations
  ProducerMaxRate                                                             Integer         the maximum rate of messages per second that can be sent
  MinLargeMessageSize                                                         Integer         the size (in bytes) before a message is treated as large
  BlockOnAcknowledge                                                          Boolean         whether or not messages are acknowledged synchronously
  BlockOnNonDurableSend                                                       Boolean         whether or not non-durable messages are sent synchronously
  BlockOnDurableSend                                                          Boolean         whether or not durable messages are sent synchronously
  AutoGroup                                                                   Boolean         whether or not message grouping is automatically used
  PreAcknowledge                                                              Boolean         whether messages are pre acknowledged by the server before sending
  ReconnectAttempts                                                           Integer         maximum number of retry attempts, default for the resource adapter is -1 (infinite attempts)
  RetryInterval                                                               Long            the time (in milliseconds) to retry a connection after failing
  RetryIntervalMultiplier                                                     Double          multiplier to apply to successive retry intervals
  FailoverOnServerShutdown                                                    Boolean         If true client will reconnect to another server if available
  ClientID                                                                    String          the pre-configured client ID for the connection factory
  ClientFailureCheckPeriod                                                    Long            the period (in ms) after which the client will consider the connection failed after not receiving packets from the server
  UseGlobalPools                                                              Boolean         whether or not to use a global thread pool for threads
  ScheduledThreadPoolMaxSize                                                  Integer         the size of the *scheduled thread* pool
  ThreadPoolMaxSize                                                           Integer         the size of the thread pool
  SetupAttempts                                                               Integer         Number of attempts to setup a JMS connection (default is 10, -1 means to attempt infinitely). It is possible that the MDB is deployed before the JMS resources are available. In that case, the resource adapter will try to setup several times until the resources are available. This applies only for inbound connections
  SetupInterval                                                               Long            Interval in milliseconds between consecutive attempts to setup a JMS connection (default is 2000m). This applies only for inbound connections

  : Global Configuration Properties

### Adapter Outbound Configuration

The outbound configuration should remain unchanged as they define
connection factories that are used by Java EE components. These
Connection Factories can be defined inside a configuration file that
matches the name `*-ds.xml`. You'll find a default `jms-ds.xml`
configuration under the `activemq` directory in the JBoss AS deployment.
The connection factories defined in this file inherit their properties
from the main `ra.xml` configuration but can also be overridden. The
following example shows how to override them.

> **Note**
>
> Please note that this configuration only applies when ActiveMQ
> resource adapter is installed in JBoss Application Server. If you are
> using another JEE application server please refer to your application
> servers documentation for how to do this.

    <tx-connection-factory>
       <jndi-name>RemoteJmsXA</jndi-name>
       <xa-transaction/>
       <rar-name>jms-ra.rar</rar-name>
       <connection-definition>org.apache.activemq.ra.ActiveMQRAConnectionFactory
    </connection-definition>
    <config-property name="SessionDefaultType" type="String">javax.jms.Topic</config-property>
       <config-property name="ConnectorClassName" type="String">
          org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory
       </config-property>
       <config-property name="ConnectionParameters" type="String">
          port=5445</config-property>
       <max-pool-size>20</max-pool-size>
    </tx-connection-factory>

> **Warning**
>
> If the connector class name is overridden the all parameters must also
> be supplied.

In this example the connection factory will be bound to JNDI with the
name `RemoteJmsXA` and can be looked up in the usual way using JNDI or
defined within the EJB or MDB as such:

    @Resource(mappedName="java:/RemoteJmsXA")
    private ConnectionFactory connectionFactory;

The `config-property` elements are what overrides those in the `ra.xml`
configuration file. Any of the elements pertaining to the connection
factory can be overridden here.

The outbound configuration also defines additional properties in
addition to the global configuration properties.

  Property Name        Property Type   Property Description
  -------------------- --------------- -------------------------------------------------------------------------------------------------------------
  SessionDefaultType   String          the default session type
  UseTryLock           Integer         try to obtain a lock within specified number of seconds. less than or equal to 0 disable this functionality

  : Outbound Configuration Properties

### Adapter Inbound Configuration

The inbound configuration should again remain unchanged. This controls
what forwards messages onto MDBs. It is possible to override properties
on the MDB by adding an activation configuration to the MDB itself. This
could be used to configure the MDB to consume from a different server.

The inbound configuration also defines additional properties in addition
to the global configuration properties.

  Property Name            Property Type   Property Description
  ------------------------ --------------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Destination              String          JNDI name of the destination
  DestinationType          String          type of the destination, either `javax.jms.Queue` or `javax.jms.Topic` (default is javax.jms.Queue)
  AcknowledgeMode          String          The Acknowledgment mode, either `Auto-acknowledge` or `Dups-ok-acknowledge` (default is Auto-acknowledge). `AUTO_ACKNOWLEDGE` and `DUPS_OK_ACKNOWLEDGE` are acceptable values.
  JndiParams               String          A semicolon (';') delimited string of name=value pairs which represent the properties to be used for the destination JNDI look up. The properties depends on the JNDI implementation of the server hosting ActiveMQ. Typically only be used when the MDB is configured to consume from a remote destination and needs to look up a JNDI reference rather than the ActiveMQ name of the destination. Only relevant when `useJNDI` is `true` (default is an empty string).
  MaxSession               Integer         Maximum number of session created by this inbound configuration (default is 15)
  MessageSelector          String          the message selector of the consumer
  SubscriptionDurability   String          Type of the subscription, either `Durable` or `NonDurable`
  ShareSubscriptions       Boolean         When true, multiple MDBs can share the same `Durable` subscription
  SubscriptionName         String          Name of the subscription
  TransactionTimeout       Long            The transaction timeout in milliseconds (default is 0, the transaction does not timeout)
  UseJNDI                  Boolean         Whether or not use JNDI to look up the destination (default is true)

  : Inbound Configuration Properties

### Configuring the adapter to use a standalone ActiveMQ Server

Sometime you may want your messaging server on a different machine or
separate from the application server. If this is the case you will only
need the activemq client libs installed. This section explains what
config to create and what jar dependencies are needed.

There are two configuration files needed to do this, one for the
incoming adapter used for MDB's and one for outgoing connections managed
by the JCA managed connection pool used by outgoing JEE components
wanting outgoing connections.

#### Configuring the Incoming Adaptor

Firstly you will need to create directory under the `deploy` directory
ending in `.rar.` For this example we will name the directory
`activemq-ra.rar`. This detail is important as the name of directory is
referred to by the MDB's and the outgoing configuration.

> **Note**
>
> The jboss default for this is `jms-ra.rar`, If you don't want to have
> to configure your MDB's you can use this but you may need to remove
> the generic adaptor that uses this.

Under the `activemq-ra.rar` directory you will need to create a
`META-INF` directory into which you should create an `ra.xml`
configuration file. You can find a template for the `ra.xml` under the
config directory of the ActiveMQ distribution.

To configure MDB's to consume messages from a remote ActiveMQ server you
need to edit the `ra.xml` file under `deploy/activemq-ra.rar/META-INF`
and change the transport type to use a netty connector (instead of the
invm connector that is defined) and configure its transport parameters.
Heres an example of what this would look like:

    <resourceadapter-class>org.apache.activemq.ra.ActiveMQResourceAdapter</resourceadapter-class>
       <config-property>
          <description>The transport type</description>
          <config-property-name>ConnectorClassName</config-property-name>
          <config-property-type>java.lang.String</config-property-type>
          <config-property-value>org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory</config-property-value>
       </config-property>
          <config-property>
          <description>The transport configuration. These values must be in the form of key=val;key=val;</description>
          <config-property-name>ConnectionParameters</config-property-name>
          <config-property-type>java.lang.String</config-property-type>
       <config-property-value>host=127.0.0.1;port=5446</config-property-value>
    </config-property>

If you want to provide a list of servers that the adapter can connect to
you can provide a list of connectors, each separated by a comma.

    <resourceadapter-class>org.apache.activemq.ra.ActiveMQResourceAdapter</resourceadapter-class>
       <config-property>
          <description>The transport type</description>
          <config-property-name>ConnectorClassName</config-property-name>
          <config-property-type>java.lang.String</config-property-type>
          <config-property-value>org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory,org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory</config-property-value>
       </config-property>
          <config-property>
          <description>The transport configuration. These values must be in the form of key=val;key=val;</description>
          <config-property-name>ConnectionParameters</config-property-name>
          <config-property-type>java.lang.String</config-property-type>
       <config-property-value>host=127.0.0.1;port=5446,host=127.0.0.2;port=5447</config-property-value>
    </config-property>

> **Warning**
>
> Make sure you provide parameters for each connector configured. The
> position of the parameters in the list maps to each connector
> provided.

This configures the resource adapter to connect to a server running on
localhost listening on port 5446

#### Configuring the outgoing adaptor

You will also need to configure the outbound connection by creating a
`activemq-ds.xml` and placing it under any directory that will be
deployed under the `deploy` directory. In a standard ActiveMQ jboss
configuration this would be under `activemq` or `activemq.sar` but you
can place it where ever you like. Actually as long as it ends in
`-ds.xml` you can call it anything you like. You can again find a
template for this file under the config directory of the ActiveMQ
distribution but called `jms-ds.xml` which is the jboss default.

The following example shows a sample configuration

    <tx-connection-factory>
       <jndi-name>RemoteJmsXA</jndi-name>
       <xa-transaction/>
       <rar-name>activemq-ra.rar</rar-name>
       <connection-definition>org.apache.activemq.ra.ActiveMQRAConnectionFactory</connection-definition>
       <config-property name="SessionDefaultType" type="java.lang.String">javax.jms.Topic</config-property>
       <config-property name="ConnectorClassName" type="java.lang.String">org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory</config-property>
       <config-property name="ConnectionParameters" type="java.lang.String">host=127.0.0.1;port=5446</config-property>
       <max-pool-size>20</max-pool-size>
    </tx-connection-factory>

Again you will see that this uses the netty connector type and will
connect to the ActiveMQ server running on localhost and listening on
port 5446. JEE components can access this by using JNDI and looking up
the connection factory using JNDI using `java:/RemoteJmsXA`, you can see
that this is defined under the`jndi-name` attribute. You will also note
that the outgoing connection will be created by the resource adaptor
configured under the directory `activemq-ra.rar` as explained in the
last section.

Also if you want to configure multiple connectors do this as a comma
separated list as in the ra configuration.

#### Jar dependencies

This is a list of the ActiveMQ and third party jars needed

  Jar Name                   Description                             Location
  -------------------------- --------------------------------------- -------------------------------------------------------------------------------------------------
  activemq-ra.jar            The ActiveMQ resource adaptor classes   deploy/activemq-ra.rar or equivalent
  activemq-core-client.jar   The ActiveMQ core client classes        either in the config lib, i.e. default/lib or the common lib dir, i.e. \$JBOSS\_HOME/common lib
  activemq-jms-client.jar    The ActiveMQ JMS classes                as above
  netty.jar                  The Netty transport classes             as above

  : Jar Dependencies

## Configuring the JBoss Application Server to connect to Remote ActiveMQ Server

This is a step by step guide on how to configure a JBoss application
server that doesn't have ActiveMQ installed to use a remote instance of
ActiveMQ

### Configuring JBoss 5

Firstly download and install JBoss AS 5 as per the JBoss installation
guide and ActiveMQ as per the ActiveMQ installation guide. After that
the following steps are required

-   Copy the following jars from the ActiveMQ distribution to the `lib`
    directory of which ever JBossAs configuration you have chosen, i.e.
    `default`.

    -   activemq-core-client.jar

    -   activemq-jms-client.jar

    -   activemq-ra.jar (this can be found inside the `activemq-ra.rar`
        archive)

    -   netty.jar

-   create the directories `activemq-ra.rar` and
    `activemq-ra.rar/META-INF` under the `deploy` directory in your
    JBoss config directory

-   under the `activemq-ra.rar/META-INF` create a `ra.xml` file or copy
    it from the ActiveMQ distribution (again it can be found in the
    `activemq-ra.rar` archive) and configure it as follows

        <?xml version="1.0" encoding="UTF-8"?>

        <connector xmlns="http://java.sun.com/xml/ns/j2ee"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xsi:schemaLocation="http://java.sun.com/xml/ns/j2ee
                   http://java.sun.com/xml/ns/j2ee/connector_1_5.xsd"
                   version="1.5">

           <description>ActiveMQ 2.0 Resource Adapter Alternate Configuration</description>
           <display-name>ActiveMQ 2.0 Resource Adapter Alternate Configuration</display-name>

           <vendor-name>Red Hat Middleware LLC</vendor-name>
           <eis-type>JMS 1.1 Server</eis-type>
           <resourceadapter-version>1.0</resourceadapter-version>

           <license>
              <description>
        Copyright 2009 Red Hat, Inc.
         Red Hat licenses this file to you under the Apache License, version
         2.0 (the "License"); you may not use this file except in compliance
         with the License.  You may obtain a copy of the License at
           http://www.apache.org/licenses/LICENSE-2.0
         Unless required by applicable law or agreed to in writing, software
         distributed under the License is distributed on an "AS IS" BASIS,
         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
         implied.  See the License for the specific language governing
         permissions and limitations under the License.
              </description>
              <license-required>true</license-required>
           </license>

           <resourceadapter>
              <resourceadapter-class>org.apache.activemq.ra.ActiveMQResourceAdapter</resourceadapter-class>
              <config-property>
                 <description>The transport type</description>
                 <config-property-name>ConnectorClassName</config-property-name>
                 <config-property-type>java.lang.String</config-property-type>
                 <config-property-value>org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory</config-property-value>
              </config-property>
              <config-property>
                 <description>The transport configuration. These values must be in the form of key=val;key=val;</description>
                 <config-property-name>ConnectionParameters</config-property-name>
                 <config-property-type>java.lang.String</config-property-type>
                 
              </config-property>

              <outbound-resourceadapter>
                 <connection-definition>
                    <managedconnectionfactory-class>org.apache.activemq.ra.ActiveMQRAManagedConnectionFactory</managedconnectionfactory-class>

                    <config-property>
                       <description>The default session type</description>
                       <config-property-name>SessionDefaultType</config-property-name>
                       <config-property-type>java.lang.String</config-property-type>
                       <config-property-value>javax.jms.Queue</config-property-value>
                    </config-property>
                    <config-property>
                       <description>Try to obtain a lock within specified number of seconds; less than or equal to 0 disable this functionality</description>
                       <config-property-name>UseTryLock</config-property-name>
                       <config-property-type>java.lang.Integer</config-property-type>
                       <config-property-value>0</config-property-value>
                    </config-property>

                    <connectionfactory-interface>org.apache.activemq.ra.ActiveMQRAConnectionFactory</connectionfactory-interface>
                    <connectionfactory-impl-class>org.apache.activemq.ra.ActiveMQRAConnectionFactoryImpl</connectionfactory-impl-class>
                    <connection-interface>javax.jms.Session</connection-interface>
                    <connection-impl-class>org.apache.activemq.ra.ActiveMQRASession</connection-impl-class>
                 </connection-definition>
                 <transaction-support>XATransaction</transaction-support>
                 <authentication-mechanism>
                    <authentication-mechanism-type>BasicPassword</authentication-mechanism-type>
                    <credential-interface>javax.resource.spi.security.PasswordCredential</credential-interface>
                 </authentication-mechanism>
                 <reauthentication-support>false</reauthentication-support>
              </outbound-resourceadapter>

              <inbound-resourceadapter>
                 <messageadapter>
                    <messagelistener>
                       <messagelistener-type>javax.jms.MessageListener</messagelistener-type>
                       <activationspec>
                          <activationspec-class>org.apache.activemq.ra.inflow.ActiveMQActivationSpec</activationspec-class>
                          <required-config-property>
                              <config-property-name>destination</config-property-name>
                          </required-config-property>
                       </activationspec>
                    </messagelistener>
                 </messageadapter>
              </inbound-resourceadapter>

           </resourceadapter>
        </connector>

    The important part of this configuration is the part in bold, i.e.
    \<config-property-value\>host=127.0.0.1;port=5445\</config-property-value\>.
    This should be configured to the host and port of the remote
    ActiveMQ server.

At this point you should be able to now deploy MDB's that consume from
the remote server. You will however, have to make sure that your MDB's
have the annotation `@ResourceAdapter("activemq-ra.rar")` added, this is
illustrated in the Configuring Message-Driven Beans section. If you don't want to add this annotation
then you can delete the generic resource adapter `jms-ra.rar` and rename
the `activemq-ra.rar` to this.

If you also want to use the remote ActiveMQ server for outgoing
connections, i.e. sending messages, then do the following:

-   Create a file called `activemq-ds.xml` in the `deploy` directory (in
    fact you can call this anything you want as long as it ends in
    `-ds.xml`). Then add the following:

        <connection-factories>
          <!--
           JMS XA Resource adapter, use this for outbound JMS connections.
           Inbound connections are defined at the @MDB activation or at the resource-adapter properties.
          -->
          <tx-connection-factory>
             <jndi-name>RemoteJmsXA</jndi-name>
             <xa-transaction/>
             <rar-name>activemq-ra.rar</rar-name>
             <connection-definition>org.apache.activemq.ra.ActiveMQRAConnectionFactory</connection-definition>
             <config-property name="SessionDefaultType" type="java.lang.String">javax.jms.Topic</config-property>
             <config-property name="ConnectorClassName" type="java.lang.String">org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory</config-property>
             <config-property name="ConnectionParameters" type="java.lang.String">host=127.0.0.1;port=5445</config-property>
             <max-pool-size>20</max-pool-size>
          </tx-connection-factory>


        </connection-factories>

    Again you will see that the host and port are configured here to
    match the remote ActiveMQ servers configuration. The other important
    attributes are:

    -   jndi-name - This is the name used to look up the JMS connection
        factory from within your JEE client

    -   rar-name - This should match the directory that you created to
        hold the Resource Adapter configuration

Now you should be able to send messages using the JCA JMS connection
pooling within an XA transaction.

### Configuring JBoss 5

The steps to do this are exactly the same as for JBoss 4, you will have
to create a jboss.xml definition file for your MDB with the following
entry

    <message-driven>
        <ejb-name>MyMDB</ejb-name>
        <resource-adapter-name>jms-ra.rar</resource-adapter-name>
     </message-driven>

Also you will need to edit the `standardjboss.xml` and uncomment the
section with the following 'Uncomment to use JMS message inflow from
jmsra.rar' and then comment out the invoker-proxy-binding called
'message-driven-bean'

## XA Recovery

*XA recovery* deals with system or application failures to ensure that
of a transaction are applied consistently to all resources affected by
the transaction, even if any of the application processes or the machine
hosting them crash or lose network connectivity. For more information on
XA Recovery,please refer to [JBoss
Transactions](http://www.jboss.org/community/wiki/JBossTransactions).

When ActiveMQ is integrated with JBoss AS, it can take advantage of
JBoss Transactions to provide recovery of messaging resources. If
messages are involved in a XA transaction, in the event of a server
crash, the recovery manager will ensure that the transactions are
recovered and the messages will either be committed or rolled back
(depending on the transaction outcome) when the server is restarted.

### XA Recovery Configuration

To enable ActiveMQ's XA Recovery, the Recovery Manager must be
configured to connect to ActiveMQ to recover its resources. The
following property must be added to the `jta` section of
`conf/jbossts-properties.xml` of JBoss AS profiles:

    <properties depends="arjuna" name="jta">
       ...
                         
       <property name="com.arjuna.ats.jta.recovery.XAResourceRecovery.ActiveMQ1"
                    value="org.apache.activemq.jms.server.recovery.ActiveMQXAResourceRecovery;[connection configuration]"/>
       <property name="com.arjuna.ats.jta.xaRecoveryNode" value="1"/>
    </properties>

The `[connection configuration]` contains all the information required
to connect to ActiveMQ node under the form `[connector factory class
                    name],[user name], [password], [connector parameters]`.

-   `[connector factory class name]` corresponds to the name of the
    `ConnectorFactory` used to connect to ActiveMQ. Values can be
    `org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory`
    or
    `org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory`

-   `[user name]` is the user name to create a client session. It is
    optional

-   `[password]` is the password to create a client session. It is
    mandatory only if the user name is specified

-   `[connector parameters]` is a list of comma-separated key=value pair
    which are passed to the connector factory (see [Configuring the transport](configuring-transports.md) for a list of the
    transport parameters).

Also note the `com.arjuna.ats.jta.xaRecoveryNode` parameter. If you want
recovery enabled then this must be configured to what ever the tx node
id is set to, this is configured in the same file by the
`com.arjuna.ats.arjuna.xa.nodeIdentifier` property.

> **Note**
>
> ActiveMQ must have a valid acceptor which corresponds to the connector
> specified in `conf/jbossts-properties.xml`.

#### Configuration Settings

If ActiveMQ is configured with a default in-vm acceptor:

    <acceptor name="in-vm">
       <factory-class>org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory</factory-class>
    </acceptor>

the corresponding configuration in `conf/jbossts-properties.xml` is:

    <property name="com.arjuna.ats.jta.recovery.XAResourceRecovery.ACTIVEMQ1"
       value="org.apache.activemq.jms.server.recovery.ActiveMQXAResourceRecovery;org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory"/>

If it is now configured with a netty acceptor on a non-default port:

    <acceptor name="netty">
       <factory-class>org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory</factory-class>
       <param key="port" value="8888"/>
    </acceptor>

the corresponding configuration in `conf/jbossts-properties.xml` is:

    <property name="com.arjuna.ats.jta.recovery.XAResourceRecovery.ACTIVEMQ1"
           value="org.apache.activemq.jms.server.recovery.ActiveMQXAResourceRecovery;org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory, , , port=8888"/>

> **Note**
>
> Note the additional commas to skip the user and password before
> connector parameters

If the recovery must use `admin, adminpass`, the configuration would
have been:

    <property name="com.arjuna.ats.jta.recovery.XAResourceRecovery.ACTIVEMQ1"
          value="org.apache.activemq.jms.server.recovery.ActiveMQXAResourceRecovery;org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory, admin, adminpass, port=8888"/>

Configuring ActiveMQ with an invm acceptor and configuring the Recovery
Manager with an invm connector is the recommended way to enable XA
Recovery.

## Example

See ? which shows how to configure XA Recovery and recover messages
after a server crash.
