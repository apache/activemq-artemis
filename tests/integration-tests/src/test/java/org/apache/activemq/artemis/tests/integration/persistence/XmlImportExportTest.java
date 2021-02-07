/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.persistence;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.cli.commands.tools.xml.XmlDataExporter;
import org.apache.activemq.artemis.cli.commands.tools.xml.XmlDataImporter;
import org.apache.activemq.artemis.core.persistence.impl.journal.BatchingIDGenerator;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.unit.util.InVMContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A test of the XML export/import functionality
 */
@RunWith(value = Parameterized.class)
public class XmlImportExportTest extends ActiveMQTestBase {

   private static final Logger logger = Logger.getLogger(XmlImportExportTest.class);

   private boolean forceLongs;

   @Parameterized.Parameters(name = "forceLongs={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{true}, {false}});
   }

   public XmlImportExportTest(boolean forceLongs) {
      this.forceLongs = forceLongs;

   }

   public static final int CONSUMER_TIMEOUT = 5000;
   private static final String QUEUE_NAME = "A1";
   private ServerLocator locator;
   private ActiveMQServer server;
   private JMSServerManager jmsServer;
   private ClientSessionFactory factory;
   private InVMContext namingContext;

   protected void checkForLongs() {
      if (forceLongs) {
         JournalStorageManager manager = (JournalStorageManager) server.getStorageManager();
         BatchingIDGenerator idGenerator = (BatchingIDGenerator) manager.getIDGenerator();
         idGenerator.forceNextID((Integer.MAX_VALUE) + 1L);
      }
   }

   @Test
   public void testMessageProperties() throws Exception {
      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      StringBuilder international = new StringBuilder();
      for (char x = 800; x < 1200; x++) {
         international.append(x);
      }

      String special = "\"<>'&";

      for (int i = 0; i < 5; i++) {
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
         msg.putStringProperty(new SimpleString("myNullSimpleStringProperty"), (SimpleString) null);
         producer.send(msg);
      }

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      for (int i = 0; i < 5; i++) {
         ClientMessage msg = consumer.receive(CONSUMER_TIMEOUT);
         byte[] body = new byte[msg.getBodySize()];
         msg.getBodyBuffer().readBytes(body);
         assertTrue(new String(body).contains("Bob the giant pig " + i));
         assertEquals(msg.getBooleanProperty("myBooleanProperty"), Boolean.TRUE);
         assertEquals(msg.getByteProperty("myByteProperty"), new Byte("0"));
         byte[] bytes = msg.getBytesProperty("myBytesProperty");
         for (int j = 0; j < 5; j++) {
            assertEquals(j, bytes[j]);
         }
         assertEquals(i * 1.6, msg.getDoubleProperty("myDoubleProperty"), 0.000001);
         assertEquals(i * 2.5F, msg.getFloatProperty("myFloatProperty"), 0.000001);
         assertEquals(i, msg.getIntProperty("myIntProperty").intValue());
         assertEquals(Long.MAX_VALUE - i, msg.getLongProperty("myLongProperty").longValue());
         assertEquals(i, msg.getObjectProperty("myObjectProperty"));
         assertEquals(true, msg.getPropertyNames().contains(SimpleString.toSimpleString("myNullObjectProperty")));
         assertEquals(null, msg.getObjectProperty("myNullObjectProperty"));
         assertEquals(new Integer(i).shortValue(), msg.getShortProperty("myShortProperty").shortValue());
         assertEquals("myStringPropertyValue_" + i, msg.getStringProperty("myStringProperty"));
         assertEquals(true, msg.getPropertyNames().contains(SimpleString.toSimpleString("myNullStringProperty")));
         assertEquals(null, msg.getStringProperty("myNullStringProperty"));
         assertEquals(international.toString(), msg.getStringProperty("myNonAsciiStringProperty"));
         assertEquals(special, msg.getStringProperty("mySpecialCharacters"));
         assertEquals(new SimpleString("mySimpleStringPropertyValue_" + i), msg.getSimpleStringProperty(new SimpleString("mySimpleStringProperty")));
         assertEquals(true, msg.getPropertyNames().contains(SimpleString.toSimpleString("myNullSimpleStringProperty")));
         assertEquals(null, msg.getSimpleStringProperty("myNullSimpleStringProperty"));
      }
   }

   /**
    * @return ClientSession
    * @throws Exception
    */
   private ClientSession basicSetUp() throws Exception {
      server = createServer(true);
      server.getConfiguration().getConnectorConfigurations().put("in-vm1", new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      server.getConfiguration().getConnectorConfigurations().put("in-vm2", new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      jmsServer = new JMSServerManagerImpl(server);
      addActiveMQComponent(jmsServer);
      namingContext = new InVMContext();
      jmsServer.setRegistry(new JndiBindingRegistry(namingContext));
      jmsServer.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      checkForLongs();
      return addClientSession(factory.createSession(false, true, true));
   }

   @Test
   public void testMessageTypes() throws Exception {

      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

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
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
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
   public void testTextMessage() throws Exception {
      StringBuilder data = new StringBuilder();
      for (int i = 0; i < 2608; i++) {
         data.append("X");
      }

      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

      ClientProducer producer = session.createProducer(QUEUE_NAME);
      ClientMessage msg = session.createMessage(Message.TEXT_TYPE, true);
      msg.getBodyBuffer().writeString(data.toString());
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.TEXT_TYPE, msg.getType());
      assertEquals(data.toString(), msg.getBodyBuffer().readString());
   }

   @Test
   public void testBytesMessage() throws Exception {
      StringBuilder data = new StringBuilder();
      for (int i = 0; i < 2610; i++) {
         data.append("X");
      }

      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

      ClientProducer producer = session.createProducer(QUEUE_NAME);
      ClientMessage msg = session.createMessage(Message.BYTES_TYPE, true);
      msg.getBodyBuffer().writeBytes(data.toString().getBytes());
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Message.BYTES_TYPE, msg.getType());
      byte[] result = new byte[msg.getBodySize()];
      msg.getBodyBuffer().readBytes(result);
      assertEquals(data.toString().getBytes().length, result.length);
   }

   @Test
   public void testMessageAttributes() throws Exception {

      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

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
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertEquals(Long.MAX_VALUE, msg.getExpiration());
      assertEquals((byte) 0, msg.getPriority());
      assertEquals(Long.MAX_VALUE - 1, msg.getTimestamp());
      assertNotNull(msg.getUserID());
   }

   @Test
   public void testBindingAttributes() throws Exception {
      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration("queueName1").setAddress("addressName1"));
      session.createQueue(new QueueConfiguration("queueName2").setAddress("addressName1").setFilterString("bob"));

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);

      ClientSession.QueueQuery queueQuery = session.queueQuery(new SimpleString("queueName1"));

      assertEquals("addressName1", queueQuery.getAddress().toString());
      assertNull(queueQuery.getFilterString());

      queueQuery = session.queueQuery(new SimpleString("queueName2"));

      assertEquals("addressName1", queueQuery.getAddress().toString());
      assertEquals("bob", queueQuery.getFilterString().toString());
      assertEquals(true, queueQuery.isDurable());
   }

   @Test
   public void testLargeMessage() throws Exception {
      server = createServer(true);
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, false);

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
         fileMessage.addBytes(new byte[]{getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources(false, true);

      session.createQueue(new QueueConfiguration("A"));

      ClientProducer prod = session.createProducer("A");

      prod.send(fileMessage);

      fileMessage.deleteFile();

      session.commit();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
      session.close();
      session = factory.createSession(false, false);
      session.start();

      ClientConsumer cons = session.createConsumer("A");

      ClientMessage msg = cons.receive(CONSUMER_TIMEOUT);

      assertNotNull(msg);

      assertEquals(2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
         assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      msg.acknowledge();
      session.commit();
   }

   @Test
   public void testLargeMessagesNoTmpFiles() throws Exception {
      server = createServer(true);
      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, false);

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
         fileMessage.addBytes(new byte[]{getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources(false, true);

      session.createQueue(new QueueConfiguration("A"));

      ClientProducer prod = session.createProducer("A");

      prod.send(fileMessage);
      prod.send(fileMessage);

      fileMessage.deleteFile();

      session.commit();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.sort = true;
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
      session.close();
      session = factory.createSession(false, false);
      session.start();

      ClientConsumer cons = session.createConsumer("A");

      ClientMessage msg = cons.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
      assertEquals(2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
         assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
      }
      msg = cons.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
      assertEquals(2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
         assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      msg.acknowledge();
      session.commit();

      //make sure there is not tmp file left
      File workingDir = new File(System.getProperty("user.dir"));
      String[] flist = workingDir.list();
      for (String fn : flist) {
         assertFalse("leftover: " + fn, fn.endsWith(".tmp"));
      }
   }

   @Test
   public void testLargeJmsTextMessage() throws Exception {
      basicSetUp();
      ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactory("vm://0", "test");
      Connection c = cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      server.createQueue(new QueueConfiguration("A").setRoutingType(RoutingType.ANYCAST));
      MessageProducer p = s.createProducer(ActiveMQJMSClient.createQueue("A"));
      p.setDeliveryMode(DeliveryMode.PERSISTENT);
      StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < 1024 * 200; i++) {
         stringBuilder.append(RandomUtil.randomChar());
      }
      TextMessage textMessage = s.createTextMessage(stringBuilder.toString());
      textMessage.setStringProperty("_AMQ_DUPL_ID", String.valueOf(UUID.randomUUID()));
      p.send(textMessage);
      c.close();

      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
      session.close();

      c = cf.createConnection();
      s = c.createSession();
      MessageConsumer mc = s.createConsumer(ActiveMQJMSClient.createQueue("A"));
      c.start();
      javax.jms.Message msg = mc.receive(CONSUMER_TIMEOUT);

      assertNotNull(msg);

      c.close();
   }

   @Test
   public void testPartialQueue() throws Exception {
      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration("myQueue1").setAddress("myAddress"));
      session.createQueue(new QueueConfiguration("myQueue2").setAddress("myAddress"));

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
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);
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
   public void testPagedMessageWithMissingBinding() throws Exception {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";
      final String MY_QUEUE2 = "myQueue2";

      server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(10 * 1024).setMaxSizeBytes(20 * 1024);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      ServerLocator locator = createInVMNonHALocator()
         // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
         // page mode and we could only guarantee that by setting it to synchronous
         .setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(new QueueConfiguration(MY_QUEUE).setAddress(MY_ADDRESS));
      session.createQueue(new QueueConfiguration(MY_QUEUE2).setAddress(MY_ADDRESS));

      ClientProducer producer = session.createProducer(MY_ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++) {
         producer.send(message);
      }

      session.deleteQueue(MY_QUEUE2);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);

      ClientConsumer consumer = session.createConsumer(MY_QUEUE);

      session.start();

      for (int i = 0; i < 200; i++) {
         message = consumer.receive(CONSUMER_TIMEOUT);

         assertNotNull(message);
      }

      session.close();
      locator.close();
      server.stop();
   }

   @Test
   public void testPaging() throws Exception {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(10 * 1024).setMaxSizeBytes(20 * 1024);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      locator = createInVMNonHALocator()
         // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
         // page mode and we could only guarantee that by setting it to synchronous
         .setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(new QueueConfiguration(MY_QUEUE).setAddress(MY_ADDRESS));

      ClientProducer producer = session.createProducer(MY_ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++) {
         producer.send(message);
      }

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);

      ClientConsumer consumer = session.createConsumer(MY_QUEUE);

      session.start();

      for (int i = 0; i < 200; i++) {
         message = consumer.receive(CONSUMER_TIMEOUT);

         assertNotNull(message);
      }
   }

   @Test
   public void testPagedLargeMessage() throws Exception {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(10 * 1024).setMaxSizeBytes(20 * 1024);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      ServerLocator locator = createInVMNonHALocator()
         // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
         // page mode and we could only guarantee that by setting it to synchronous
         .setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(new QueueConfiguration(MY_QUEUE).setAddress(MY_ADDRESS));

      ClientProducer producer = session.createProducer(MY_ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++) {
         producer.send(message);
      }

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
         fileMessage.addBytes(new byte[]{getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources(false, true);

      producer.send(fileMessage);

      fileMessage.deleteFile();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      //System.out.print(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session);

      ClientConsumer consumer = session.createConsumer(MY_QUEUE);

      session.start();

      for (int i = 0; i < 200; i++) {
         message = consumer.receive(CONSUMER_TIMEOUT);

         assertNotNull(message);
      }

      ClientMessage msg = consumer.receive(CONSUMER_TIMEOUT);

      assertNotNull(msg);

      assertEquals(2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
         assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      session.close();
      locator.close();
      server.stop();
   }

   @Test
   public void testTransactional() throws Exception {
      ClientSession session = basicSetUp();

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(true);
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session, managementSession);
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      assertNotNull(msg);
   }

   @Test
   public void testBody() throws Exception {
      final String QUEUE_NAME = "A1";
      server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(Message.TEXT_TYPE, true);
      msg.getBodyBuffer().writeString("bob123");
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session, managementSession);
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
   public void testBody2() throws Exception {
      final String QUEUE_NAME = "A1";
      server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(new QueueConfiguration(QUEUE_NAME));

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(true);
      byte[] bodyTst = new byte[10];
      for (int i = 0; i < 10; i++) {
         bodyTst[i] = (byte) (i + 1);
      }
      msg.getBodyBuffer().writeBytes(bodyTst);
      assertEquals(bodyTst.length, msg.getBodySize());
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session, managementSession);
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

   @Test
   public void testRoutingTypes() throws Exception {
      SimpleString myAddress = SimpleString.toSimpleString("myAddress");
      ClientSession session = basicSetUp();

      EnumSet<RoutingType> routingTypes = EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST);

      session.createAddress(myAddress, routingTypes, false);

      session.createQueue(new QueueConfiguration("myQueue1").setAddress(myAddress));
      session.createQueue(new QueueConfiguration("myQueue2").setAddress(myAddress));

      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) logger.debug(new String(xmlOutputStream.toByteArray()));

      clearDataRecreateServerDirs();
      server.start();
      checkForLongs();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter();
      xmlDataImporter.validate(xmlInputStream);
      xmlInputStream.reset();
      xmlDataImporter.process(xmlInputStream, session, managementSession);

      assertTrue(server.getAddressInfo(myAddress).getRoutingTypes().contains(RoutingType.ANYCAST));
      assertTrue(server.getAddressInfo(myAddress).getRoutingTypes().contains(RoutingType.MULTICAST));
   }
}
