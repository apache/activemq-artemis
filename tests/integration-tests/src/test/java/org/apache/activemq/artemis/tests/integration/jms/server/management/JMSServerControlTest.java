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
package org.apache.activemq.artemis.tests.integration.jms.server.management;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.json.JsonArray;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer;
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory;
import org.apache.activemq.artemis.jms.persistence.JMSStorageManager;
import org.apache.activemq.artemis.jms.persistence.config.PersistedBindings;
import org.apache.activemq.artemis.jms.persistence.config.PersistedConnectionFactory;
import org.apache.activemq.artemis.jms.persistence.config.PersistedDestination;
import org.apache.activemq.artemis.jms.persistence.config.PersistedType;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.management.ManagementTestBase;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JMSServerControlTest extends ManagementTestBase {
   // Attributes ----------------------------------------------------

   protected InVMNamingContext context;

   private ActiveMQServer server;

   private JMSServerManagerImpl serverManager;

   private FakeJMSStorageManager fakeJMSStorageManager;

   // Static --------------------------------------------------------

   private static String toCSV(final Object[] objects) {
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < objects.length; i++) {
         if (i > 0) {
            str.append(", ");
         }
         str.append(objects[i]);
      }
      return str.toString();
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Number of consumers used by the test itself
    */
   protected int getNumberOfConsumers() {
      return 0;
   }

   @Test
   public void testGetVersion() throws Exception {
      JMSServerControl control = createManagementControl();
      String version = control.getVersion();
      Assert.assertEquals(serverManager.getVersion(), version);
   }

   @Test
   public void testCreateQueueWithBindings() throws Exception {
      String[] bindings = new String[3];
      bindings[0] = RandomUtil.randomString();
      bindings[1] = RandomUtil.randomString();
      bindings[2] = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      String bindingsCSV = JMSServerControlTest.toCSV(bindings);
      ActiveMQTestBase.checkNoBinding(context, bindingsCSV);

      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, bindingsCSV);

      Object o = ActiveMQTestBase.checkBinding(context, bindings[0]);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = ActiveMQTestBase.checkBinding(context, bindings[1]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = ActiveMQTestBase.checkBinding(context, bindings[2]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[2]));
   }

   @Test
   public void testCreateQueueWithCommaBindings() throws Exception {
      String[] bindings = new String[3];
      bindings[0] = "first&comma;first";
      bindings[1] = "second&comma;second";
      bindings[2] = "third&comma;third";
      String queueName = RandomUtil.randomString();

      String bindingsCSV = JMSServerControlTest.toCSV(bindings);
      ActiveMQTestBase.checkNoBinding(context, bindingsCSV);

      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, bindingsCSV);

      Object o = ActiveMQTestBase.checkBinding(context, "first,first");
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = ActiveMQTestBase.checkBinding(context, "second,second");
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      o = ActiveMQTestBase.checkBinding(context, "third,third");
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains("first,first"));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains("second,second"));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains("third,third"));
   }

   @Test
   public void testCreateQueueWithSelector() throws Exception {
      String[] bindings = new String[3];
      bindings[0] = RandomUtil.randomString();
      bindings[1] = RandomUtil.randomString();
      bindings[2] = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      String bindingsCSV = JMSServerControlTest.toCSV(bindings);
      ActiveMQTestBase.checkNoBinding(context, bindingsCSV);

      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      String selector = "foo='bar'";
      control.createQueue(queueName, bindingsCSV, selector);

      Object o = ActiveMQTestBase.checkBinding(context, bindings[0]);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      // assertEquals(((ActiveMQDestination)queue).get);
      Assert.assertEquals(queueName, queue.getQueueName());
      Assert.assertEquals(selector, server.getPostOffice().getBinding(new SimpleString("jms.queue." + queueName)).getFilter().getFilterString().toString());
      o = ActiveMQTestBase.checkBinding(context, bindings[1]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      Assert.assertEquals(selector, server.getPostOffice().getBinding(new SimpleString("jms.queue." + queueName)).getFilter().getFilterString().toString());
      o = ActiveMQTestBase.checkBinding(context, bindings[2]);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      Assert.assertEquals(selector, server.getPostOffice().getBinding(new SimpleString("jms.queue." + queueName)).getFilter().getFilterString().toString());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(queueName).contains(bindings[2]));
   }

   @Test
   public void testCreateNonDurableQueue() throws Exception {
      String queueName = RandomUtil.randomString();
      String binding = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, binding, null, false);

      Object o = ActiveMQTestBase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(new SimpleString("jms.queue." + queueName));
      Assert.assertFalse(queueBinding.getQueue().isDurable());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      // queue is not durable => not stored
      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(queueName));
      Assert.assertNull(fakeJMSStorageManager.persistedJNDIMap.get(queueName));
   }

   @Test
   public void testDestroyQueue() throws Exception {
      String queueJNDIBinding = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      ActiveMQTestBase.checkBinding(context, queueJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      control.destroyQueue(queueName);

      ActiveMQTestBase.checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(queueName));
   }

   @Test
   public void testDestroyQueueWithConsumers() throws Exception {
      String queueJNDIBinding = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      ActiveMQTestBase.checkBinding(context, queueJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         // create a consumer will create a Core queue bound to the topic address
         ActiveMQMessageConsumer cons = (ActiveMQMessageConsumer) session.createConsumer(ActiveMQJMSClient.createQueue(queueName));

         control.destroyQueue(queueName, true);

         ActiveMQTestBase.checkNoBinding(context, queueJNDIBinding);
         checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

         Assert.assertNull(fakeJMSStorageManager.destinationMap.get(queueName));

         long time = System.currentTimeMillis();
         while (!cons.isClosed() && time + 5000 > System.currentTimeMillis()) {
            Thread.sleep(100);
         }
         Assert.assertTrue(cons.isClosed());

         try {
            cons.receive(5000);
            Assert.fail("should throw exception");
         } catch (javax.jms.IllegalStateException e) {
            Assert.assertTrue(e.getCause() instanceof ActiveMQObjectClosedException);
         }
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testDestroyQueueWithConsumersWithoutForcingTheConsumersToClose() throws Exception {
      String queueJNDIBinding = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      ActiveMQTestBase.checkBinding(context, queueJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
      connection.start();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(ActiveMQJMSClient.createQueue(queueName));
         producer.send(session.createTextMessage());
         // create a consumer will create a Core queue bound to the topic address
         ActiveMQMessageConsumer cons = (ActiveMQMessageConsumer) session.createConsumer(ActiveMQJMSClient.createQueue(queueName));

         try {
            control.destroyQueue(queueName, false);
            Assert.fail();
         } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("AMQ119025"));
         }

         ActiveMQTestBase.checkBinding(context, queueJNDIBinding);
         checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

         Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(queueName));

         Assert.assertFalse(cons.isClosed());

         Assert.assertNotNull(cons.receive(5000));
      } finally {
         connection.close();
      }
   }

   @Test
   public void testDestroyTopicWithConsumersWithoutForcingTheConsumersToClose() throws Exception {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      ActiveMQTestBase.checkBinding(context, topicJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
      connection.start();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         // create a consumer will create a Core queue bound to the topic address
         ActiveMQMessageConsumer cons = (ActiveMQMessageConsumer) session.createConsumer(ActiveMQJMSClient.createTopic(topicName));
         MessageProducer producer = session.createProducer(ActiveMQJMSClient.createTopic(topicName));
         producer.send(session.createTextMessage());

         try {
            control.destroyTopic(topicName, false);
            Assert.fail();
         } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("AMQ119025"));
         }

         ActiveMQTestBase.checkBinding(context, topicJNDIBinding);
         checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));
         Assert.assertFalse(cons.isClosed());

         Assert.assertNotNull(cons.receive(5000));
      } finally {
         connection.close();
      }
   }

   @Test
   public void testDestroyTopicWithConsumers() throws Exception {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      ActiveMQTestBase.checkBinding(context, topicJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         // create a consumer will create a Core queue bound to the topic address
         ActiveMQMessageConsumer cons = (ActiveMQMessageConsumer) session.createConsumer(ActiveMQJMSClient.createTopic(topicName));

         control.destroyTopic(topicName, true);

         ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
         checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

         long time = System.currentTimeMillis();
         while (!cons.isClosed() && time + 5000 > System.currentTimeMillis()) {
            Thread.sleep(100);
         }
         Assert.assertTrue(cons.isClosed());

         try {
            cons.receive(5000);
            Assert.fail("should throw exception");
         } catch (javax.jms.IllegalStateException e) {
            Assert.assertTrue(e.getCause() instanceof ActiveMQObjectClosedException);
         }
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testDestroyQueueWithConsumersNetty() throws Exception {
      String queueJNDIBinding = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      ActiveMQTestBase.checkBinding(context, queueJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(ActiveMQTestBase.NETTY_CONNECTOR_FACTORY));
      cf.setReconnectAttempts(-1);
      ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         // create a consumer will create a Core queue bound to the topic address
         MessageConsumer cons = session.createConsumer(ActiveMQJMSClient.createQueue(queueName));

         control.destroyQueue(queueName, true);

         ActiveMQTestBase.checkNoBinding(context, queueJNDIBinding);
         checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

         Assert.assertNull(fakeJMSStorageManager.destinationMap.get(queueName));

         long timeout = System.currentTimeMillis() + 1000;
         while (timeout > System.currentTimeMillis() && !((ActiveMQMessageConsumer) cons).isClosed()) {
            Thread.sleep(1);
         }

         Assert.assertTrue(((ActiveMQMessageConsumer) cons).isClosed());

         try {
            cons.receive(5000);
            Assert.fail("should throw exception");
         } catch (javax.jms.IllegalStateException e) {
            Assert.assertTrue(e.getCause() instanceof ActiveMQObjectClosedException);
         }
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testGetQueueNames() throws Exception {
      String queueJNDIBinding = RandomUtil.randomString();
      String queueName = RandomUtil.randomString();

      JMSServerControl control = createManagementControl();
      Assert.assertEquals(0, control.getQueueNames().length);

      control.createQueue(queueName, queueJNDIBinding);

      String[] names = control.getQueueNames();
      Assert.assertEquals(1, names.length);
      Assert.assertEquals(queueName, names[0]);

      control.destroyQueue(queueName);

      Assert.assertEquals(0, control.getQueueNames().length);
   }

   @Test
   public void testCreateTopic() throws Exception {
      String[] bindings = new String[3];
      bindings[0] = RandomUtil.randomString();
      bindings[1] = RandomUtil.randomString();
      bindings[2] = RandomUtil.randomString();
      String topicJNDIBinding = JMSServerControlTest.toCSV(bindings);
      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      String topicName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      Object o = ActiveMQTestBase.checkBinding(context, bindings[0]);
      Assert.assertTrue(o instanceof Topic);
      Topic topic = (Topic) o;
      Assert.assertEquals(topicName, topic.getTopicName());
      o = ActiveMQTestBase.checkBinding(context, bindings[1]);
      Assert.assertTrue(o instanceof Topic);
      topic = (Topic) o;
      Assert.assertEquals(topicName, topic.getTopicName());
      o = ActiveMQTestBase.checkBinding(context, bindings[2]);
      Assert.assertTrue(o instanceof Topic);
      topic = (Topic) o;
      Assert.assertEquals(topicName, topic.getTopicName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      Assert.assertNotNull(fakeJMSStorageManager.destinationMap.get(topicName));
      Assert.assertNotNull(fakeJMSStorageManager.persistedJNDIMap.get(topicName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(topicName).contains(bindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(topicName).contains(bindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(topicName).contains(bindings[2]));
   }

   @Test
   public void testDestroyTopic() throws Exception {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));
      Topic topic = (Topic) context.lookup(topicJNDIBinding);
      Assert.assertNotNull(topic);
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()));
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // create a consumer will create a Core queue bound to the topic address
      session.createConsumer(topic);

      String topicAddress = ActiveMQDestination.createTopicAddressFromName(topicName).toString();
      AddressControl addressControl = (AddressControl) server.getManagementService().getResource(ResourceNames.CORE_ADDRESS + topicAddress);
      Assert.assertNotNull(addressControl);

      Assert.assertTrue(addressControl.getQueueNames().length > 0);

      connection.close();
      control.destroyTopic(topicName);

      Assert.assertNull(server.getManagementService().getResource(ResourceNames.CORE_ADDRESS + topicAddress));
      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(topicName));
   }

   @Test
   public void testListAllConsumers() throws Exception {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));
      Topic topic = (Topic) context.lookup(topicJNDIBinding);
      Assert.assertNotNull(topic);
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // create a consumer will create a Core queue bound to the topic address
      MessageConsumer cons = session.createConsumer(topic);

      JsonArray jsonArray = JsonUtil.readJsonArray(control.listAllConsumersAsJSON());

      Assert.assertEquals(1 + getNumberOfConsumers(), jsonArray.size());

      cons.close();

      jsonArray = JsonUtil.readJsonArray(control.listAllConsumersAsJSON());

      Assert.assertEquals(getNumberOfConsumers(), jsonArray.size());

      String topicAddress = ActiveMQDestination.createTopicAddressFromName(topicName).toString();
      AddressControl addressControl = (AddressControl) server.getManagementService().getResource(ResourceNames.CORE_ADDRESS + topicAddress);
      Assert.assertNotNull(addressControl);

      Assert.assertTrue(addressControl.getQueueNames().length > 0);

      connection.close();
      control.destroyTopic(topicName);

      Assert.assertNull(server.getManagementService().getResource(ResourceNames.CORE_ADDRESS + topicAddress));
      ActiveMQTestBase.checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      Assert.assertNull(fakeJMSStorageManager.destinationMap.get(topicName));
   }

   @Test
   public void testGetTopicNames() throws Exception {
      String topicJNDIBinding = RandomUtil.randomString();
      String topicName = RandomUtil.randomString();

      JMSServerControl control = createManagementControl();
      Assert.assertEquals(0, control.getTopicNames().length);

      control.createTopic(topicName, topicJNDIBinding);

      String[] names = control.getTopicNames();
      Assert.assertEquals(1, names.length);
      Assert.assertEquals(topicName, names[0]);

      control.destroyTopic(topicName);

      Assert.assertEquals(0, control.getTopicNames().length);
   }

   @Test
   public void testCreateConnectionFactory_3b() throws Exception {
      server.getConfiguration().getConnectorConfigurations().put("tst", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      doCreateConnectionFactory(new ConnectionFactoryCreator() {
         @Override
         public void createConnectionFactory(final JMSServerControl control,
                                             final String cfName,
                                             final Object[] bindings) throws Exception {
            String jndiBindings = JMSServerControlTest.toCSV(bindings);

            control.createConnectionFactory(cfName, false, false, 0, "tst", jndiBindings);
         }
      });
   }

   @Test
   public void testCreateConnectionFactory_CompleteList() throws Exception {
      JMSServerControl control = createManagementControl();
      control.createConnectionFactory("test", //name
                                      true, // ha
                                      false, // useDiscovery
                                      1, // cfType
                                      "invm", // connectorNames
                                      "tst", // jndiBindins
                                      "tst", // clientID
                                      1, // clientFailureCheckPeriod
                                      1,  // connectionTTL
                                      1, // callTimeout
                                      1, //callFailoverTimeout
                                      1, // minLargeMessageSize
                                      true, // compressLargeMessages
                                      1, // consumerWindowSize
                                      1, // consumerMaxRate
                                      1, // confirmationWindowSize
                                      1, // ProducerWindowSize
                                      1, // producerMaxRate
                                      true, // blockOnACK
                                      true, // blockOnDurableSend
                                      true, // blockOnNonDurableSend
                                      true, // autoGroup
                                      true, // preACK
                                      ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, // loadBalancingClassName
                                      1, // transactionBatchSize
                                      1, // dupsOKBatchSize
                                      true, // useGlobalPools
                                      1, // scheduleThreadPoolSize
                                      1, // threadPoolMaxSize
                                      1, // retryInterval
                                      1, // retryIntervalMultiplier
                                      1, // maxRetryInterval
                                      1, // reconnectAttempts
                                      true, // failoverOnInitialConnection
                                      "tst"); // groupID

      ActiveMQQueueConnectionFactory cf = (ActiveMQQueueConnectionFactory) context.lookup("tst");

      Assert.assertEquals(true, cf.isHA());
      Assert.assertEquals("tst", cf.getClientID());
      Assert.assertEquals(1, cf.getClientFailureCheckPeriod());
      Assert.assertEquals(1, cf.getConnectionTTL());
      Assert.assertEquals(1, cf.getCallTimeout());
      Assert.assertEquals(1, cf.getCallFailoverTimeout());
      Assert.assertEquals(1, cf.getMinLargeMessageSize());
      Assert.assertEquals(true, cf.isCompressLargeMessage());
      Assert.assertEquals(1, cf.getConsumerWindowSize());
      Assert.assertEquals(1, cf.getConfirmationWindowSize());
      Assert.assertEquals(1, cf.getProducerWindowSize());
      Assert.assertEquals(1, cf.getProducerMaxRate());
      Assert.assertEquals(true, cf.isBlockOnAcknowledge());
      Assert.assertEquals(true, cf.isBlockOnDurableSend());
      Assert.assertEquals(true, cf.isBlockOnNonDurableSend());
      Assert.assertEquals(true, cf.isAutoGroup());
      Assert.assertEquals(true, cf.isPreAcknowledge());
      Assert.assertEquals(ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, cf.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(1, cf.getTransactionBatchSize());
      Assert.assertEquals(1, cf.getDupsOKBatchSize());
      Assert.assertEquals(true, cf.isUseGlobalPools());
      Assert.assertEquals(1, cf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(1, cf.getThreadPoolMaxSize());
      Assert.assertEquals(1, cf.getRetryInterval());
      Assert.assertEquals(1.0, cf.getRetryIntervalMultiplier(), 0.000001);
      Assert.assertEquals(1, cf.getMaxRetryInterval());
      Assert.assertEquals(1, cf.getReconnectAttempts());
      Assert.assertEquals(true, cf.isFailoverOnInitialConnection());
      Assert.assertEquals("tst", cf.getGroupID());

      server.stop();

      waitForServerToStop(server);

      startServer();

      control = createManagementControl();

      cf = (ActiveMQQueueConnectionFactory) context.lookup("tst");

      Assert.assertEquals(true, cf.isHA());
      Assert.assertEquals("tst", cf.getClientID());
      Assert.assertEquals(1, cf.getClientFailureCheckPeriod());
      Assert.assertEquals(1, cf.getConnectionTTL());
      Assert.assertEquals(1, cf.getCallTimeout());
      Assert.assertEquals(1, cf.getMinLargeMessageSize());
      Assert.assertEquals(true, cf.isCompressLargeMessage());
      Assert.assertEquals(1, cf.getConsumerWindowSize());
      Assert.assertEquals(1, cf.getConfirmationWindowSize());
      Assert.assertEquals(1, cf.getProducerWindowSize());
      Assert.assertEquals(1, cf.getProducerMaxRate());
      Assert.assertEquals(true, cf.isBlockOnAcknowledge());
      Assert.assertEquals(true, cf.isBlockOnDurableSend());
      Assert.assertEquals(true, cf.isBlockOnNonDurableSend());
      Assert.assertEquals(true, cf.isAutoGroup());
      Assert.assertEquals(true, cf.isPreAcknowledge());
      Assert.assertEquals(ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, cf.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(1, cf.getTransactionBatchSize());
      Assert.assertEquals(1, cf.getDupsOKBatchSize());
      Assert.assertEquals(true, cf.isUseGlobalPools());
      Assert.assertEquals(1, cf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(1, cf.getThreadPoolMaxSize());
      Assert.assertEquals(1, cf.getRetryInterval());
      Assert.assertEquals(1.0, cf.getRetryIntervalMultiplier(), 0.000001);
      Assert.assertEquals(1, cf.getMaxRetryInterval());
      Assert.assertEquals(1, cf.getReconnectAttempts());
      Assert.assertEquals(true, cf.isFailoverOnInitialConnection());
      Assert.assertEquals("tst", cf.getGroupID());

      control.destroyConnectionFactory("test");

      ObjectNameBuilder nameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain());
      Assert.assertFalse(mbeanServer.isRegistered(nameBuilder.getConnectionFactoryObjectName("test")));

      server.stop();

      waitForServerToStop(server);

      startServer();

      Assert.assertFalse(mbeanServer.isRegistered(nameBuilder.getConnectionFactoryObjectName("test")));

      try {
         cf = (ActiveMQQueueConnectionFactory) context.lookup("tst");
         Assert.fail("Failure expected");
      } catch (NamingException e) {
      }

   }

   @Test
   public void testDestroyConnectionFactoryWithNullBindings() throws Exception {
      // Create Connection Factory with Null Bindings
      JMSServerControl control = createManagementControl();
      control.createConnectionFactory("test-cf", // Name
                                      false,     // HA
                                      false,     // Use Discovery?
                                      1,         // ConnectionFactory Type
                                      "invm",    // Connector Names
                                      null);     // JNDI Bindings

      control.destroyConnectionFactory("test-cf");

      Assert.assertTrue(control.getConnectionFactoryNames().length == 0);
   }

   @Test
   public void testListPreparedTransactionDetails() throws Exception {
      Xid xid = newXID();

      JMSServerControl control = createManagementControl();
      String cfJNDIBinding = "/cf";
      String cfName = "cf";

      server.getConfiguration().getConnectorConfigurations().put("tst", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      control.createConnectionFactory(cfName, false, false, 3, "tst", cfJNDIBinding);

      control.createQueue("q", "/q");

      XAConnectionFactory cf = (XAConnectionFactory) context.lookup("/cf");
      Destination dest = (Destination) context.lookup("/q");
      XAConnection conn = cf.createXAConnection();
      XASession ss = conn.createXASession();
      TextMessage m1 = ss.createTextMessage("m1");
      TextMessage m2 = ss.createTextMessage("m2");
      TextMessage m3 = ss.createTextMessage("m3");
      TextMessage m4 = ss.createTextMessage("m4");
      MessageProducer mp = ss.createProducer(dest);
      XAResource xa = ss.getXAResource();
      xa.start(xid, XAResource.TMNOFLAGS);
      mp.send(m1);
      mp.send(m2);
      mp.send(m3);
      mp.send(m4);
      xa.end(xid, XAResource.TMSUCCESS);
      xa.prepare(xid);

      ss.close();

      control.listPreparedTransactionDetailsAsJSON();
   }

   @Test
   public void testListPreparedTranscationDetailsAsHTML() throws Exception {
      Xid xid = newXID();

      JMSServerControl control = createManagementControl();
      TransportConfiguration tc = new TransportConfiguration(InVMConnectorFactory.class.getName());
      String cfJNDIBinding = "/cf";
      String cfName = "cf";

      server.getConfiguration().getConnectorConfigurations().put("tst", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      control.createConnectionFactory(cfName, false, false, 3, "tst", cfJNDIBinding);

      control.createQueue("q", "/q");

      XAConnectionFactory cf = (XAConnectionFactory) context.lookup("/cf");
      Destination dest = (Destination) context.lookup("/q");
      XAConnection conn = cf.createXAConnection();
      XASession ss = conn.createXASession();
      TextMessage m1 = ss.createTextMessage("m1");
      TextMessage m2 = ss.createTextMessage("m2");
      TextMessage m3 = ss.createTextMessage("m3");
      TextMessage m4 = ss.createTextMessage("m4");
      MessageProducer mp = ss.createProducer(dest);
      XAResource xa = ss.getXAResource();
      xa.start(xid, XAResource.TMNOFLAGS);
      mp.send(m1);
      mp.send(m2);
      mp.send(m3);
      mp.send(m4);
      xa.end(xid, XAResource.TMSUCCESS);
      xa.prepare(xid);

      ss.close();

      control.listPreparedTransactionDetailsAsHTML();
   }

   @Test
   public void testRemoteClientIDConnection() throws Exception {
      JMSServerControl control = createManagementControl();

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()));
      Connection connection = cf.createConnection();

      connection.setClientID("someID");

      Connection connection2 = cf.createConnection();
      boolean failed = false;

      try {
         connection2.setClientID("someID");
      } catch (JMSException e) {
         failed = true;
      }

      Assert.assertTrue(failed);

      System.out.println(control.closeConnectionWithClientID("someID"));

      connection2.setClientID("someID");

      failed = false;
      Connection connection3 = cf.createConnection();

      try {
         connection3.setClientID("someID");
      } catch (JMSException e) {
         failed = true;
      }

      Assert.assertTrue(failed);

      // If these objects are not referenced at the end of the test, they can be destroyed by garbage collector
      // during the test, what can lead to test failure.
      connection.close();
      connection2.close();
      connection3.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      startServer();
   }

   /**
    * @throws Exception
    */
   protected void startServer() throws Exception {
      Configuration config = createDefaultNettyConfig().setJMXManagementEnabled(true).addConnectorConfiguration("netty", new TransportConfiguration(ActiveMQTestBase.NETTY_CONNECTOR_FACTORY)).addConnectorConfiguration("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, true));

      serverManager = new JMSServerManagerImpl(server);
      context = new InVMNamingContext();
      serverManager.setRegistry(new JndiBindingRegistry(context));
      serverManager.start();
      serverManager.activated();

      this.fakeJMSStorageManager = new FakeJMSStorageManager(serverManager.getJMSStorageManager());

      serverManager.replaceStorageManager(fakeJMSStorageManager);
   }

   protected JMSServerControl createManagementControl() throws Exception {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   private void doCreateConnectionFactory(final ConnectionFactoryCreator creator) throws Exception {
      Object[] cfJNDIBindings = new Object[]{RandomUtil.randomString(), RandomUtil.randomString(), RandomUtil.randomString()};

      String cfName = RandomUtil.randomString();

      for (Object cfJNDIBinding : cfJNDIBindings) {
         ActiveMQTestBase.checkNoBinding(context, cfJNDIBinding.toString());
      }
      checkNoResource(ObjectNameBuilder.DEFAULT.getConnectionFactoryObjectName(cfName));

      JMSServerControl control = createManagementControl();
      creator.createConnectionFactory(control, cfName, cfJNDIBindings);

      for (Object cfJNDIBinding : cfJNDIBindings) {
         Object o = ActiveMQTestBase.checkBinding(context, cfJNDIBinding.toString());
         Assert.assertTrue(o instanceof ConnectionFactory);
         ConnectionFactory cf = (ConnectionFactory) o;
         Connection connection = cf.createConnection();
         connection.close();
      }
      checkResource(ObjectNameBuilder.DEFAULT.getConnectionFactoryObjectName(cfName));

      Assert.assertNotNull(fakeJMSStorageManager.connectionFactoryMap.get(cfName));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(cfName).contains(cfJNDIBindings[0]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(cfName).contains(cfJNDIBindings[1]));
      Assert.assertTrue(fakeJMSStorageManager.persistedJNDIMap.get(cfName).contains(cfJNDIBindings[2]));
   }

   // Inner classes -------------------------------------------------

   interface ConnectionFactoryCreator {

      void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception;
   }

   class FakeJMSStorageManager implements JMSStorageManager {

      Map<String, PersistedDestination> destinationMap = new HashMap<>();

      Map<String, PersistedConnectionFactory> connectionFactoryMap = new HashMap<>();

      ConcurrentHashMap<String, List<String>> persistedJNDIMap = new ConcurrentHashMap<>();

      JMSStorageManager delegate;

      FakeJMSStorageManager(JMSStorageManager delegate) {
         this.delegate = delegate;
      }

      @Override
      public void storeDestination(PersistedDestination destination) throws Exception {
         destinationMap.put(destination.getName(), destination);
         delegate.storeDestination(destination);
      }

      @Override
      public void deleteDestination(PersistedType type, String name) throws Exception {
         destinationMap.remove(name);
         delegate.deleteDestination(type, name);
      }

      @Override
      public List<PersistedDestination> recoverDestinations() {
         return delegate.recoverDestinations();
      }

      @Override
      public void deleteConnectionFactory(String connectionFactory) throws Exception {
         connectionFactoryMap.remove(connectionFactory);
         delegate.deleteConnectionFactory(connectionFactory);
      }

      @Override
      public void storeConnectionFactory(PersistedConnectionFactory connectionFactory) throws Exception {
         connectionFactoryMap.put(connectionFactory.getName(), connectionFactory);
         delegate.storeConnectionFactory(connectionFactory);
      }

      @Override
      public List<PersistedConnectionFactory> recoverConnectionFactories() {
         return delegate.recoverConnectionFactories();
      }

      @Override
      public void addBindings(PersistedType type, String name, String... address) throws Exception {
         persistedJNDIMap.putIfAbsent(name, new ArrayList<String>());
         for (String ad : address) {
            persistedJNDIMap.get(name).add(ad);
         }
         delegate.addBindings(type, name, address);
      }

      @Override
      public List<PersistedBindings> recoverPersistedBindings() throws Exception {
         return delegate.recoverPersistedBindings();
      }

      @Override
      public void deleteBindings(PersistedType type, String name, String address) throws Exception {
         persistedJNDIMap.get(name).remove(address);
         delegate.deleteBindings(type, name, address);
      }

      @Override
      public void deleteBindings(PersistedType type, String name) throws Exception {
         persistedJNDIMap.get(name).clear();
         delegate.deleteBindings(type, name);
      }

      @Override
      public void start() throws Exception {
         delegate.start();
      }

      @Override
      public void stop() throws Exception {
         delegate.stop();
      }

      @Override
      public boolean isStarted() {
         return delegate.isStarted();
      }

      @Override
      public void load() throws Exception {
         delegate.load();
      }
   }

}
