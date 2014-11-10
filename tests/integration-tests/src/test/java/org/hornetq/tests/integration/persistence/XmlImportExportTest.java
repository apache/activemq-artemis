/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.persistence;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.hornetq.tools.XmlDataConstants;
import org.hornetq.tools.XmlDataExporter;
import org.hornetq.tools.XmlDataImporter;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.UUIDGenerator;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * A test of the XML export/import functionality
 *
 * @author Justin Bertram
 */
public class XmlImportExportTest extends ServiceTestBase
{
   public static final int CONSUMER_TIMEOUT = 5000;
   private static final String QUEUE_NAME = "A1";
   private ServerLocator locator;
   private HornetQServer server;
   private JMSServerManager jmsServer;
   private ClientSessionFactory factory;
   private InVMContext namingContext;

   @Test
   public void testMessageProperties() throws Exception
   {
      ClientSession session = basicSetUp();

      session.createQueue(QUEUE_NAME, QUEUE_NAME, true);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      StringBuilder international = new StringBuilder();
      for (char x = 800; x < 1200; x++)
      {
         international.append(x);
      }

      String special = "\"<>'&";

      for (int i = 0; i < 5; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeString("Bob the giant pig " + i);
         msg.putBooleanProperty("myBooleanProperty", Boolean.TRUE);
         msg.putByteProperty("myByteProperty", new Byte("0"));
         msg.putBytesProperty("myBytesProperty", new byte[]{0, 1, 2, 3, 4});
         msg.putDoubleProperty("myDoubleProperty", i * 1.6);
         msg.putFloatProperty("myFloatProperty", i * 2.5F);
         msg.putIntProperty("myIntProperty", i);
         msg.putLongProperty("myLongProperty", Long.MAX_VALUE - i);
         msg.putObjectProperty("myObjectProperty", i);
         msg.putObjectProperty("myNullObjectProperty", null);
         msg.putShortProperty("myShortProperty", new Integer(i).shortValue());
         msg.putStringProperty("myStringProperty", "myStringPropertyValue_" + i);
         msg.putStringProperty("myNullStringProperty", null);
         msg.putStringProperty("myNonAsciiStringProperty", international.toString());
         msg.putStringProperty("mySpecialCharacters", special);
         msg.putStringProperty(new SimpleString("mySimpleStringProperty"), new SimpleString("mySimpleStringPropertyValue_" + i));
         msg.putStringProperty(new SimpleString("myNullSimpleStringProperty"), null);
         producer.send(msg);
      }

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      for (int i = 0; i < 5; i++)
      {
         ClientMessage msg = consumer.receive(CONSUMER_TIMEOUT);
         byte[] body = new byte[msg.getBodySize()];
         msg.getBodyBuffer().readBytes(body);
         assertTrue(new String(body).contains("Bob the giant pig " + i));
         assertEquals(msg.getBooleanProperty("myBooleanProperty"), Boolean.TRUE);
         assertEquals(msg.getByteProperty("myByteProperty"), new Byte("0"));
         byte[] bytes = msg.getBytesProperty("myBytesProperty");
         for (int j = 0; j < 5; j++)
         {
            assertEquals(j, bytes[j]);
         }
         assertEquals(i * 1.6, msg.getDoubleProperty("myDoubleProperty"), 0.000001);
         assertEquals(i * 2.5F, msg.getFloatProperty("myFloatProperty"), 0.000001);
         assertEquals(i, msg.getIntProperty("myIntProperty").intValue());
         assertEquals(Long.MAX_VALUE - i, msg.getLongProperty("myLongProperty").longValue());
         assertEquals(i, msg.getObjectProperty("myObjectProperty"));
         assertEquals(null, msg.getObjectProperty("myNullObjectProperty"));
         assertEquals(new Integer(i).shortValue(), msg.getShortProperty("myShortProperty").shortValue());
         assertEquals("myStringPropertyValue_" + i, msg.getStringProperty("myStringProperty"));
         assertEquals(null, msg.getStringProperty("myNullStringProperty"));
         assertEquals(international.toString(), msg.getStringProperty("myNonAsciiStringProperty"));
         assertEquals(special, msg.getStringProperty("mySpecialCharacters"));
         assertEquals(new SimpleString("mySimpleStringPropertyValue_" + i), msg.getSimpleStringProperty(new SimpleString("mySimpleStringProperty")));
         assertEquals(null, msg.getSimpleStringProperty(new SimpleString("myNullSimpleStringProperty")));
      }
   }

   /**
    * @return ClientSession
    * @throws Exception
    */
   private ClientSession basicSetUp() throws Exception
   {
      server = createServer(true);
      server.getConfiguration().getConnectorConfigurations().put("in-vm1", new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      server.getConfiguration().getConnectorConfigurations().put("in-vm2", new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      jmsServer = new JMSServerManagerImpl(server);
      addHornetQComponent(jmsServer);
      namingContext = new InVMContext();
      jmsServer.setContext(namingContext);
      jmsServer.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      return addClientSession(factory.createSession(false, true, true));
   }

   @Test
   public void testMessageTypes() throws Exception
   {

      ClientSession session = basicSetUp();

      session.createQueue(QUEUE_NAME, QUEUE_NAME, true);

      ClientProducer producer = session.createProducer(QUEUE_NAME);


      ClientMessage msg = session.createMessage(Message.BYTES_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.DEFAULT_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.MAP_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.OBJECT_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.STREAM_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.TEXT_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(true);
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.BYTES_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.DEFAULT_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.MAP_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.OBJECT_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.STREAM_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.TEXT_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.DEFAULT_TYPE, msg.getType());
   }

   @Test
   public void testMessageAttributes() throws Exception
   {

      ClientSession session = basicSetUp();

      session.createQueue(QUEUE_NAME, QUEUE_NAME, true);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(Message.BYTES_TYPE, true);
      msg.setExpiration(Long.MAX_VALUE);
      msg.setPriority((byte) 0);
      msg.setTimestamp(Long.MAX_VALUE - 1);
      msg.setUserID(UUIDGenerator.getInstance().generateUUID());
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Long.MAX_VALUE, msg.getExpiration());
      assertEquals((byte) 0, msg.getPriority());
      assertEquals(Long.MAX_VALUE - 1, msg.getTimestamp());
      assertNotNull(msg.getUserID());
   }

   @Test
   public void testBindingAttributes() throws Exception
   {
      ClientSession session = basicSetUp();

      session.createQueue("addressName1", "queueName1", true);
      session.createQueue("addressName1", "queueName2", "bob", true);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();

      ClientSession.QueueQuery queueQuery = session.queueQuery(new SimpleString("queueName1"));

      assertEquals("addressName1", queueQuery.getAddress().toString());
      assertNull(queueQuery.getFilterString());

      queueQuery = session.queueQuery(new SimpleString("queueName2"));

      assertEquals("addressName1", queueQuery.getAddress().toString());
      assertEquals("bob", queueQuery.getFilterString().toString());
      assertEquals(true, queueQuery.isDurable());
   }

   @Test
   public void testJmsConnectionFactoryBinding() throws Exception
   {
      final String clientId = "myClientId";
      final long clientFailureCheckPeriod = 1;
      final long connectionTTl = 2;
      final long callTimeout = 3;
      final long callFailoverTimeout = 4;
      final boolean cacheLargeMessagesClient = true;
      final int minLargeMessageSize = 5;
      final boolean compressLargeMessages = true;
      final int consumerWindowSize = 6;
      final int consumerMaxRate = 7;
      final int confirmationWindowSize = 8;
      final int producerWindowSize = 9;
      final int producerMaxrate = 10;
      final boolean blockOnAcknowledge = true;
      final boolean blockOnDurableSend = false;
      final boolean blockOnNonDurableSend = true;
      final boolean autoGroup = true;
      final boolean preacknowledge = true;
      final String loadBalancingPolicyClassName = "myPolicy";
      final int transactionBatchSize = 11;
      final int dupsOKBatchSize = 12;
      final boolean useGlobalPools = true;
      final int scheduledThreadPoolMaxSize = 13;
      final int threadPoolMaxSize = 14;
      final long retryInterval = 15;
      final double retryIntervalMultiplier = 10.0;
      final long maxRetryInterval = 16;
      final int reconnectAttempts = 17;
      final boolean failoverOnInitialConnection = true;
      final String groupId = "myGroupId";
      final String name = "myFirstConnectionFactoryName";
      final String jndi_binding1 = name + "Binding1";
      final String jndi_binding2 = name + "Binding2";
      final JMSFactoryType type = JMSFactoryType.CF;
      final boolean ha = true;
      final List connectors = Arrays.asList("in-vm1", "in-vm2");

      ClientSession session = basicSetUp();

      jmsServer.createConnectionFactory(name,
            ha,
            type,
            connectors,
            clientId,
            clientFailureCheckPeriod,
            connectionTTl,
            callTimeout,
            callFailoverTimeout,
            cacheLargeMessagesClient,
            minLargeMessageSize,
            compressLargeMessages,
            consumerWindowSize,
            consumerMaxRate,
            confirmationWindowSize,
            producerWindowSize,
            producerMaxrate,
            blockOnAcknowledge,
            blockOnDurableSend,
            blockOnNonDurableSend,
            autoGroup,
            preacknowledge,
            loadBalancingPolicyClassName,
            transactionBatchSize,
            dupsOKBatchSize,
            useGlobalPools,
            scheduledThreadPoolMaxSize,
            threadPoolMaxSize,
            retryInterval,
            retryIntervalMultiplier,
            maxRetryInterval,
            reconnectAttempts,
            failoverOnInitialConnection,
            groupId,
            jndi_binding1,
            jndi_binding2);

      jmsServer.createConnectionFactory("mySecondConnectionFactoryName", false, JMSFactoryType.CF, Arrays.asList("in-vm1", "in-vm2"), "mySecondConnectionFactoryName1", "mySecondConnectionFactoryName2");

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();

      ConnectionFactory cf1 = (ConnectionFactory) namingContext.lookup(jndi_binding1);
      assertNotNull(cf1);
      HornetQConnectionFactory hcf1 = (HornetQConnectionFactory) cf1;
      assertEquals(ha, hcf1.isHA());
      assertEquals(type.intValue(), hcf1.getFactoryType());
      assertEquals(clientId, hcf1.getClientID());
      assertEquals(clientFailureCheckPeriod, hcf1.getClientFailureCheckPeriod());
      assertEquals(connectionTTl, hcf1.getConnectionTTL());
      assertEquals(callTimeout, hcf1.getCallTimeout());
//      Assert.assertEquals(callFailoverTimeout, hcf1.getCallFailoverTimeout());  // this value isn't currently persisted by org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl.encode()
//      Assert.assertEquals(cacheLargeMessagesClient, hcf1.isCacheLargeMessagesClient()); // this value isn't currently supported by org.hornetq.api.jms.management.JMSServerControl.createConnectionFactory(java.lang.String, boolean, boolean, int, java.lang.String, java.lang.String, java.lang.String, long, long, long, long, int, boolean, int, int, int, int, int, boolean, boolean, boolean, boolean, boolean, java.lang.String, int, int, boolean, int, int, long, double, long, int, boolean, java.lang.String)
      assertEquals(minLargeMessageSize, hcf1.getMinLargeMessageSize());
//      Assert.assertEquals(compressLargeMessages, hcf1.isCompressLargeMessage());  // this value isn't currently handled properly by org.hornetq.jms.server.impl.JMSServerManagerImpl.createConnectionFactory(java.lang.String, boolean, org.hornetq.api.jms.JMSFactoryType, java.util.List<java.lang.String>, java.lang.String, long, long, long, long, boolean, int, boolean, int, int, int, int, int, boolean, boolean, boolean, boolean, boolean, java.lang.String, int, int, boolean, int, int, long, double, long, int, boolean, java.lang.String, java.lang.String...)()
      assertEquals(consumerWindowSize, hcf1.getConsumerWindowSize());
      assertEquals(consumerMaxRate, hcf1.getConsumerMaxRate());
      assertEquals(confirmationWindowSize, hcf1.getConfirmationWindowSize());
      assertEquals(producerWindowSize, hcf1.getProducerWindowSize());
      assertEquals(producerMaxrate, hcf1.getProducerMaxRate());
      assertEquals(blockOnAcknowledge, hcf1.isBlockOnAcknowledge());
      assertEquals(blockOnDurableSend, hcf1.isBlockOnDurableSend());
      assertEquals(blockOnNonDurableSend, hcf1.isBlockOnNonDurableSend());
      assertEquals(autoGroup, hcf1.isAutoGroup());
      assertEquals(preacknowledge, hcf1.isPreAcknowledge());
      assertEquals(loadBalancingPolicyClassName, hcf1.getConnectionLoadBalancingPolicyClassName());
      assertEquals(transactionBatchSize, hcf1.getTransactionBatchSize());
      assertEquals(dupsOKBatchSize, hcf1.getDupsOKBatchSize());
      assertEquals(useGlobalPools, hcf1.isUseGlobalPools());
      assertEquals(scheduledThreadPoolMaxSize, hcf1.getScheduledThreadPoolMaxSize());
      assertEquals(threadPoolMaxSize, hcf1.getThreadPoolMaxSize());
      assertEquals(retryInterval, hcf1.getRetryInterval());
      assertEquals(retryIntervalMultiplier, hcf1.getRetryIntervalMultiplier(), 0);
      assertEquals(maxRetryInterval, hcf1.getMaxRetryInterval());
      assertEquals(reconnectAttempts, hcf1.getReconnectAttempts());
      assertEquals(failoverOnInitialConnection, hcf1.isFailoverOnInitialConnection());
      assertEquals(groupId, hcf1.getGroupID());

      assertNotNull(namingContext.lookup(jndi_binding2));
      assertNotNull(namingContext.lookup("mySecondConnectionFactoryName1"));
      assertNotNull(namingContext.lookup("mySecondConnectionFactoryName2"));
   }

   @Test
   public void testJmsDestination() throws Exception
   {
      ClientSession session = basicSetUp();

      jmsServer.createQueue(true, "myQueue", null, true, "myQueueJndiBinding1", "myQueueJndiBinding2");
      jmsServer.createTopic(true, "myTopic", "myTopicJndiBinding1", "myTopicJndiBinding2");

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();


      assertNotNull(namingContext.lookup("myQueueJndiBinding1"));
      assertNotNull(namingContext.lookup("myQueueJndiBinding2"));
      assertNotNull(namingContext.lookup("myTopicJndiBinding1"));
      assertNotNull(namingContext.lookup("myTopicJndiBinding2"));

      jmsServer.createConnectionFactory("test-cf", false, JMSFactoryType.CF, Arrays.asList("in-vm1"), "test-cf");

      ConnectionFactory cf = (ConnectionFactory) namingContext.lookup("test-cf");
      Connection connection = cf.createConnection();
      Session jmsSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = jmsSession.createProducer((Destination) namingContext.lookup("myQueueJndiBinding1"));
      producer.send(jmsSession.createTextMessage());
      MessageConsumer consumer = jmsSession.createConsumer((Destination) namingContext.lookup("myQueueJndiBinding2"));
      connection.start();
      assertNotNull(consumer.receive(3000));

      consumer = jmsSession.createConsumer((Destination) namingContext.lookup("myTopicJndiBinding1"));
      producer = jmsSession.createProducer((Destination) namingContext.lookup("myTopicJndiBinding2"));
      producer.send(jmsSession.createTextMessage());
      assertNotNull(consumer.receive(3000));

      connection.close();
   }

   @Test
   public void testJmsDestinationWithAppServerCompatibility() throws Exception
   {
      ClientSession session = basicSetUp();

      jmsServer.createQueue(true, "myQueue", null, true, "myQueueJndiBinding1", "myQueueJndiBinding2");
      jmsServer.createTopic(true, "myTopic", "myTopicJndiBinding1", "myTopicJndiBinding2");

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      session = basicSetUp();

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session, true);
      xmlDataImporter.processXml();

      assertNotNull(namingContext.lookup("myQueueJndiBinding1"));
      assertNotNull(namingContext.lookup(XmlDataConstants.JNDI_COMPATIBILITY_PREFIX + "myQueueJndiBinding1"));
      assertNotNull(namingContext.lookup("myQueueJndiBinding2"));
      assertNotNull(namingContext.lookup(XmlDataConstants.JNDI_COMPATIBILITY_PREFIX + "myQueueJndiBinding2"));
      assertNotNull(namingContext.lookup("myTopicJndiBinding1"));
      assertNotNull(namingContext.lookup(XmlDataConstants.JNDI_COMPATIBILITY_PREFIX + "myTopicJndiBinding1"));
      assertNotNull(namingContext.lookup("myTopicJndiBinding2"));
      assertNotNull(namingContext.lookup(XmlDataConstants.JNDI_COMPATIBILITY_PREFIX + "myTopicJndiBinding2"));

      jmsServer.createConnectionFactory("test-cf", false, JMSFactoryType.CF, Arrays.asList("in-vm1"), "test-cf");

      ConnectionFactory cf = (ConnectionFactory) namingContext.lookup("test-cf");
      Connection connection = cf.createConnection();
      Session jmsSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = jmsSession.createProducer((Destination) namingContext.lookup("myQueueJndiBinding1"));
      producer.send(jmsSession.createTextMessage());
      MessageConsumer consumer = jmsSession.createConsumer((Destination) namingContext.lookup(XmlDataConstants.JNDI_COMPATIBILITY_PREFIX + "myQueueJndiBinding1"));
      connection.start();
      assertNotNull(consumer.receive(3000));

      consumer = jmsSession.createConsumer((Destination) namingContext.lookup("myTopicJndiBinding1"));
      producer = jmsSession.createProducer((Destination) namingContext.lookup(XmlDataConstants.JNDI_COMPATIBILITY_PREFIX + "myTopicJndiBinding1"));
      producer.send(jmsSession.createTextMessage());
      assertNotNull(consumer.receive(3000));

      connection.close();
   }

   @Test
   public void testLargeMessage() throws Exception
   {
      server = createServer(true);
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, false);

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         fileMessage.addBytes(new byte[]{getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources();

      session.createQueue("A", "A", true);

      ClientProducer prod = session.createProducer("A");

      prod.send(fileMessage);

      fileMessage.deleteFile();

      session.commit();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createFactory(false);
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      session.close();
      session = factory.createSession(false, false);
      session.start();

      ClientConsumer cons = session.createConsumer("A");

      ClientMessage msg = cons.receive(CONSUMER_TIMEOUT);

      assertNotNull(msg);

      assertEquals(2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      msg.acknowledge();
      session.commit();
   }

   @Test
   public void testPartialQueue() throws Exception
   {
      ClientSession session = basicSetUp();

      session.createQueue("myAddress", "myQueue1", true);
      session.createQueue("myAddress", "myQueue2", true);

      ClientProducer producer = session.createProducer("myAddress");

      ClientMessage msg = session.createMessage(true);
      producer.send(msg);

      ClientConsumer consumer = session.createConsumer("myQueue1");
      session.start();
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
      msg.acknowledge();
      consumer.close();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      consumer = session.createConsumer("myQueue1");
      session.start();
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertNull(msg);
      consumer.close();

      consumer = session.createConsumer("myQueue2");
      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
   }

   @Test
   public void testPagedMessageWithMissingBinding() throws Exception
   {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";
      final String MY_QUEUE2 = "myQueue2";

      HornetQServer server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
      // page mode and we could only guarantee that by setting it to synchronous
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(MY_ADDRESS, MY_QUEUE, true);
      session.createQueue(MY_ADDRESS, MY_QUEUE2, true);

      ClientProducer producer = session.createProducer(MY_ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++)
      {
         producer.send(message);
      }

      session.deleteQueue(MY_QUEUE2);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();

      ClientConsumer consumer = session.createConsumer(MY_QUEUE);

      session.start();

      for (int i = 0; i < 200; i++)
      {
         message = consumer.receive(CONSUMER_TIMEOUT);

         assertNotNull(message);
      }

      session.close();
      locator.close();
      server.stop();
   }

   @Test
   public void testPaging() throws Exception
   {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      locator = createInVMNonHALocator();
      // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
      // page mode and we could only guarantee that by setting it to synchronous
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(MY_ADDRESS, MY_QUEUE, true);

      ClientProducer producer = session.createProducer(MY_ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++)
      {
         producer.send(message);
      }

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();

      ClientConsumer consumer = session.createConsumer(MY_QUEUE);

      session.start();

      for (int i = 0; i < 200; i++)
      {
         message = consumer.receive(CONSUMER_TIMEOUT);

         assertNotNull(message);
      }
   }

   @Test
   public void testPagedLargeMessage() throws Exception
   {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      HornetQServer server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
      // page mode and we could only guarantee that by setting it to synchronous
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(MY_ADDRESS, MY_QUEUE, true);

      ClientProducer producer = session.createProducer(MY_ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++)
      {
         producer.send(message);
      }

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         fileMessage.addBytes(new byte[]{getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources();

      producer.send(fileMessage);

      fileMessage.deleteFile();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      //System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();

      ClientConsumer consumer = session.createConsumer(MY_QUEUE);

      session.start();

      for (int i = 0; i < 200; i++)
      {
         message = consumer.receive(CONSUMER_TIMEOUT);

         assertNotNull(message);
      }

      ClientMessage msg = consumer.receive(CONSUMER_TIMEOUT);

      assertNotNull(msg);

      assertEquals(2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      session.close();
      locator.close();
      server.stop();
   }

   @Test
   public void testTransactional() throws Exception
   {
      ClientSession session = basicSetUp();

      session.createQueue(QUEUE_NAME, QUEUE_NAME, true);

      ClientProducer producer = session.createProducer(QUEUE_NAME);


      ClientMessage msg = session.createMessage(true);
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session, managementSession);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
   }

   @Test
   public void testBody() throws Exception
   {
      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(QUEUE_NAME, QUEUE_NAME, true);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(Message.TEXT_TYPE, true);
      msg.getBodyBuffer().writeString("bob123");
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session, managementSession);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
      assertEquals("bob123", msg.getBodyBuffer().readString());

      session.close();
      locator.close();
      server.stop();
   }

   @Test
   public void testBody2() throws Exception
   {
      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(QUEUE_NAME, QUEUE_NAME, true);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(true);
      byte[] bodyTst = new byte[10];
      for (int i = 0; i < 10; i++)
      {
         bodyTst[i] = (byte)(i + 1);
      }
      msg.getBodyBuffer().writeBytes(bodyTst);
      assertEquals(bodyTst.length, msg.getBodySize());
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session, managementSession);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
      assertEquals(msg.getBodySize(), bodyTst.length);
      byte[] bodyRead = new byte[bodyTst.length];
      msg.getBodyBuffer().readBytes(bodyRead);
      assertEqualsByteArrays(bodyTst, bodyRead);

      session.close();
      locator.close();
      server.stop();
   }
}
