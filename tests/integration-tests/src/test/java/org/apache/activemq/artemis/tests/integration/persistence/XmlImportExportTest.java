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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.unit.util.InVMContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * A test of the XML export/import functionality
 */
@ExtendWith(ParameterizedTestExtension.class)
public class XmlImportExportTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private boolean forceLongs;

   @Parameters(name = "forceLongs={0}")
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

   @TestTemplate
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
         msg.putByteProperty("myByteProperty", Byte.parseByte("0"));
         msg.putBytesProperty("myBytesProperty", new byte[]{0, 1, 2, 3, 4});
         msg.putDoubleProperty("myDoubleProperty", i * 1.6);
         msg.putFloatProperty("myFloatProperty", i * 2.5F);
         msg.putIntProperty("myIntProperty", i);
         msg.putLongProperty("myLongProperty", Long.MAX_VALUE - i);
         msg.putObjectProperty("myObjectProperty", i);
         msg.putObjectProperty("myNullObjectProperty", null);
         msg.putShortProperty("myShortProperty", Integer.valueOf(i).shortValue());
         msg.putStringProperty("myStringProperty", "myStringPropertyValue_" + i);
         msg.putStringProperty("myNullStringProperty", null);
         msg.putStringProperty("myNonAsciiStringProperty", international.toString());
         msg.putStringProperty("mySpecialCharacters", special);
         msg.putStringProperty(SimpleString.of("mySimpleStringProperty"), SimpleString.of("mySimpleStringPropertyValue_" + i));
         msg.putStringProperty(SimpleString.of("myNullSimpleStringProperty"), (SimpleString) null);
         producer.send(msg);
      }

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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
         assertEquals(msg.getByteProperty("myByteProperty"), Byte.valueOf("0"));
         byte[] bytes = msg.getBytesProperty("myBytesProperty");
         for (int j = 0; j < 5; j++) {
            assertEquals(j, bytes[j]);
         }
         assertEquals(i * 1.6, msg.getDoubleProperty("myDoubleProperty"), 0.000001);
         assertEquals(i * 2.5F, msg.getFloatProperty("myFloatProperty"), 0.000001);
         assertEquals(i, msg.getIntProperty("myIntProperty").intValue());
         assertEquals(Long.MAX_VALUE - i, msg.getLongProperty("myLongProperty").longValue());
         assertEquals(i, msg.getObjectProperty("myObjectProperty"));
         assertTrue(msg.getPropertyNames().contains(SimpleString.of("myNullObjectProperty")));
         assertNull(msg.getObjectProperty("myNullObjectProperty"));
         assertEquals(Integer.valueOf(i).shortValue(), msg.getShortProperty("myShortProperty").shortValue());
         assertEquals("myStringPropertyValue_" + i, msg.getStringProperty("myStringProperty"));
         assertTrue(msg.getPropertyNames().contains(SimpleString.of("myNullStringProperty")));
         assertNull(msg.getStringProperty("myNullStringProperty"));
         assertEquals(international.toString(), msg.getStringProperty("myNonAsciiStringProperty"));
         assertEquals(special, msg.getStringProperty("mySpecialCharacters"));
         assertEquals(SimpleString.of("mySimpleStringPropertyValue_" + i), msg.getSimpleStringProperty(SimpleString.of("mySimpleStringProperty")));
         assertTrue(msg.getPropertyNames().contains(SimpleString.of("myNullSimpleStringProperty")));
         assertNull(msg.getSimpleStringProperty("myNullSimpleStringProperty"));
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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

      ClientSession.QueueQuery queueQuery = session.queueQuery(SimpleString.of("queueName1"));

      assertEquals("addressName1", queueQuery.getAddress().toString());
      assertNull(queueQuery.getFilterString());

      queueQuery = session.queueQuery(SimpleString.of("queueName2"));

      assertEquals("addressName1", queueQuery.getAddress().toString());
      assertEquals("bob", queueQuery.getFilterString().toString());
      assertTrue(queueQuery.isDurable());
   }

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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
         assertFalse(fn.endsWith(".tmp"), "leftover: " + fn);
      }
   }

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
   public void testPagedMessageWithMissingBinding() throws Exception {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";
      final String MY_QUEUE2 = "myQueue2";

      server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(10 * 1024).setMaxSizeBytes(20 * 1024).setMaxReadPageBytes(-1).setMaxReadPageMessages(-1);
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
   public void testPaging() throws Exception {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(10 * 1024).setMaxSizeBytes(20 * 1024).setMaxReadPageBytes(-1).setMaxReadPageMessages(-1);
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
   public void testPagedLargeMessage() throws Exception {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(10 * 1024).setMaxSizeBytes(20 * 1024).setMaxReadPageBytes(-1).setMaxReadPageMessages(-1);
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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
   public void testRoutingTypes() throws Exception {
      SimpleString myAddress = SimpleString.of("myAddress");
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
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

   @TestTemplate
   public void testEmptyRoutingTypes() throws Exception {
      SimpleString myAddress = SimpleString.of("myAddress");
      ClientSession session = basicSetUp();

      EnumSet<RoutingType> routingTypes = EnumSet.noneOf(RoutingType.class);

      session.createAddress(myAddress, routingTypes, false);

      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

      assertEquals(0, server.getAddressInfo(myAddress).getRoutingTypes().size());
   }

   @TestTemplate
   public void testImportWrongRoutingType() throws Exception {
      SimpleString myAddress = SimpleString.of("myAddress");
      SimpleString myQueue = SimpleString.of("myQueue");
      SimpleString dla = SimpleString.of("DLA");
      SimpleString dlaPrefix = SimpleString.of("DLA.");
      String payload = "myMessagePayload";

      server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, false);

      //Create ANYCAST queue and set "AutoCreateDeadLetterResources"
      //Send message with ANYCAST RoutingType
      server.getAddressSettingsRepository().addMatch(myAddress.toString(), new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla).setAutoCreateDeadLetterResources(true).setDeadLetterQueuePrefix(dlaPrefix).setMaxReadPageBytes(-1).setMaxReadPageMessages(-1));
      session.createQueue(new QueueConfiguration(myQueue).setAddress(myAddress).setDurable(true).setRoutingType(RoutingType.ANYCAST));

      ClientProducer producer = session.createProducer(myAddress);
      producer.send(createTextMessage(session, payload).putByteProperty(Message.HDR_ROUTING_TYPE, (byte) 1));
      session.start();
      ClientConsumer consumer = session.createConsumer(myQueue);
      ClientMessage m = consumer.receive(5000);
      m.acknowledge();

      assertNotNull(m);
      assertEquals(m.getBodyBuffer().readString(), payload);
      assertEquals(m.getRoutingType(), RoutingType.ANYCAST);

      // Rollback to place ANYCAST message on DLA (MULTICAST)
      session.rollback();
      m = consumer.receiveImmediate();
      assertNull(m);

      consumer.close();
      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter();
      xmlDataExporter.process(xmlOutputStream, server.getConfiguration().getBindingsDirectory(), server.getConfiguration().getJournalDirectory(), server.getConfiguration().getPagingDirectory(), server.getConfiguration().getLargeMessagesDirectory());
      if (logger.isDebugEnabled()) {
         logger.debug(new String(xmlOutputStream.toByteArray()));
      }

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

      //Check that message is imported with no "routingType" and is intact
      Wait.assertTrue(() -> server.getTotalMessageCount() == 1);
      session.start();
      consumer = session.createConsumer(dlaPrefix.concat(myAddress));
      m = consumer.receive(5000);

      assertNotNull(m);
      assertEquals(m.getBodyBuffer().readString(), payload);
      assertEquals(m.getRoutingType(), null);

      consumer.close();
      session.close();
      locator.close();
      server.stop();
   }
}
