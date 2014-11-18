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
package org.apache.activemq.tests.integration.server;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.core.config.ScaleDownConfiguration;
import org.apache.activemq.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.core.postoffice.Binding;
import org.apache.activemq.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ScaleDownTest extends ClusterTestBase
{
   private boolean useScaleDownGroupName;

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters(name = "useScaleDownGroupName={0}")
   public static Collection getParameters()
   {
      return Arrays.asList(new Object[][]{
         {true},
         {false}
      });
   }

   public ScaleDownTest(boolean useScaleDownGroupName)
   {
      this.useScaleDownGroupName = useScaleDownGroupName;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      setupLiveServer(0, isFileStorage(), isNetty(), true);
      setupLiveServer(1, isFileStorage(), isNetty(), true);
      LiveOnlyPolicyConfiguration haPolicyConfiguration0 = (LiveOnlyPolicyConfiguration) servers[0].getConfiguration().getHAPolicyConfiguration();
      haPolicyConfiguration0.setScaleDownConfiguration(new ScaleDownConfiguration());
      LiveOnlyPolicyConfiguration haPolicyConfiguration1 = (LiveOnlyPolicyConfiguration) servers[1].getConfiguration().getHAPolicyConfiguration();
      haPolicyConfiguration1.setScaleDownConfiguration(new ScaleDownConfiguration());
      if (useScaleDownGroupName)
      {
         haPolicyConfiguration0.getScaleDownConfiguration().setGroupName("bill");
         haPolicyConfiguration1.getScaleDownConfiguration().setGroupName("bill");
      }
      setupClusterConnection("cluster0", "testAddress", false, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster0", "testAddress", false, 1, isNetty(), 1, 0);
      haPolicyConfiguration0.getScaleDownConfiguration().getConnectors().addAll(servers[0].getConfiguration().getClusterConfigurations().iterator().next().getStaticConnectors());
      haPolicyConfiguration1.getScaleDownConfiguration().getConnectors().addAll(servers[1].getConfiguration().getClusterConfigurations().iterator().next().getStaticConnectors());
      startServers(0, 1);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
   }

   protected boolean isNetty()
   {
      return true;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      closeAllConsumers();
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();
      LiveOnlyPolicyConfiguration haPolicyConfiguration0 = (LiveOnlyPolicyConfiguration) servers[0].getConfiguration().getHAPolicyConfiguration();
      LiveOnlyPolicyConfiguration haPolicyConfiguration1 = (LiveOnlyPolicyConfiguration) servers[1].getConfiguration().getHAPolicyConfiguration();
      haPolicyConfiguration0.setScaleDownConfiguration(null);
      haPolicyConfiguration1.setScaleDownConfiguration(null);
      stopServers(0, 1);
      super.tearDown();
   }

   @Test
   public void testBasicScaleDown() throws Exception
   {
      final int TEST_SIZE = 2;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";

      // create 2 queues on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false);
      createQueue(0, addressName, queueName2, null, false);
      createQueue(1, addressName, queueName1, null, false);
      createQueue(1, addressName, queueName2, null, false);

      // send messages to node 0
      send(0, addressName, TEST_SIZE, false, null);

      // consume a message from queue 2
      addConsumer(1, 0, queueName2, null, false);
      ClientMessage clientMessage = consumers[1].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      consumers[1].getSession().commit();
//      removeConsumer(1);

      // at this point on node 0 there should be 2 messages in testQueue1 and 1 message in testQueue2
      Assert.assertEquals(TEST_SIZE, getMessageCount(((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue()));
      Assert.assertEquals(TEST_SIZE - 1, getMessageCount(((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(queueName2))).getQueue()));

      // trigger scaleDown from node 0 to node 1
      servers[0].stop();

      // get the 2 messages from queue 1
      addConsumer(0, 1, queueName1, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      // get the 1 message from queue 2
      addConsumer(0, 1, queueName2, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);
   }

   @Test
   public void testStoreAndForward() throws Exception
   {
      final int TEST_SIZE = 50;
      final String addressName1 = "testAddress1";
      final String addressName2 = "testAddress2";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";

      // create queues on each node mapped to 2 addresses
      createQueue(0, addressName1, queueName1, null, false);
      createQueue(1, addressName1, queueName1, null, false);
      createQueue(0, addressName2, queueName2, null, false);
      createQueue(1, addressName2, queueName2, null, false);

      // add consumers to node 1 to force any messages we send into the sf queue
      addConsumer(0, 1, queueName1, null);
      addConsumer(1, 1, queueName2, null);

      // find and pause the sf queue so no messages actually move from node 0 to node 1
      String sfQueueName = null;
      for (Map.Entry<SimpleString, Binding> entry : servers[0].getPostOffice().getAllBindings().entrySet())
      {
         String temp = entry.getValue().getAddress().toString();

         if (temp.startsWith("sf.") && temp.endsWith(servers[1].getNodeID().toString()))
         {
            // we found the sf queue for the other node
            // need to pause the sfQueue here
            ((LocalQueueBinding) entry.getValue()).getQueue().pause();
            sfQueueName = temp;
         }
      }

      assertNotNull(sfQueueName);

      // send messages to node 0 that will get stuck in the paused sf queue going to node 1
      send(0, addressName1, TEST_SIZE, false, null);
      send(0, addressName2, TEST_SIZE, false, null);
      removeConsumer(0);
      removeConsumer(1);

      // at this point on node 0 there should be 0 messages in testQueue and TEST_SIZE messages in the sfQueue
      Assert.assertEquals(0, getMessageCount(((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue()));
      Assert.assertEquals(0, getMessageCount(((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(queueName2))).getQueue()));
      Assert.assertEquals(TEST_SIZE * 2, getMessageCount(((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(sfQueueName))).getQueue()));

      // trigger scaleDown from node 0 to node 1
      servers[0].stop();

      // get the messages from node 1
      addConsumer(0, 1, queueName1, null);
      for (int i = 0; i < TEST_SIZE; i++)
      {
         ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
         Assert.assertNotNull(clientMessage);
         clientMessage.acknowledge();
      }

      ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      addConsumer(0, 1, queueName2, null);
      for (int i = 0; i < TEST_SIZE; i++)
      {
         clientMessage = consumers[0].getConsumer().receive(250);
         Assert.assertNotNull(clientMessage);
         clientMessage.acknowledge();
      }

      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);
   }

   @Test
   public void testScaleDownWithMissingQueue() throws Exception
   {
      final int TEST_SIZE = 2;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";

      // create 2 queues on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false);
      createQueue(0, addressName, queueName2, null, false);
      createQueue(1, addressName, queueName1, null, false);

      // send messages to node 0
      send(0, addressName, TEST_SIZE, false, null);

      // consume a message from node 0
      addConsumer(1, 0, queueName2, null, false);
      ClientMessage clientMessage = consumers[1].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      consumers[1].getSession().commit();

      // trigger scaleDown from node 0 to node 1
      servers[0].stop();

      // get the 2 messages from queue 1
      addConsumer(0, 1, queueName1, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      // get the 1 message from queue 2
      addConsumer(0, 1, queueName2, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);
   }

   @Test
   public void testMessageProperties() throws Exception
   {
      final int TEST_SIZE = 5;
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      StringBuilder international = new StringBuilder();
      for (char x = 800; x < 1200; x++)
      {
         international.append(x);
      }

      String special = "\"<>'&";

      for (int i = 0; i < TEST_SIZE; i++)
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
         msg.putShortProperty("myShortProperty", new Integer(i).shortValue());
         msg.putStringProperty("myStringProperty", "myStringPropertyValue_" + i);
         msg.putStringProperty("myNonAsciiStringProperty", international.toString());
         msg.putStringProperty("mySpecialCharacters", special);
         producer.send(msg);
      }

      servers[0].stop();

      sf = sfs[1];
      session = addClientSession(sf.createSession(false, true, true));
      ClientConsumer consumer = addClientConsumer(session.createConsumer(queueName));
      session.start();

      for (int i = 0; i < 5; i++)
      {
         ClientMessage msg = consumer.receive(250);
         byte[] body = new byte[msg.getBodySize()];
         msg.getBodyBuffer().readBytes(body);
         Assert.assertTrue(new String(body).contains("Bob the giant pig " + i));
         Assert.assertEquals(msg.getBooleanProperty("myBooleanProperty"), Boolean.TRUE);
         Assert.assertEquals(msg.getByteProperty("myByteProperty"), new Byte("0"));
         byte[] bytes = msg.getBytesProperty("myBytesProperty");
         for (int j = 0; j < 5; j++)
         {
            Assert.assertEquals(j, bytes[j]);
         }
         Assert.assertEquals(i * 1.6, msg.getDoubleProperty("myDoubleProperty"), 0.000001);
         Assert.assertEquals(i * 2.5F, msg.getFloatProperty("myFloatProperty"), 0.000001);
         Assert.assertEquals(i, msg.getIntProperty("myIntProperty").intValue());
         Assert.assertEquals(Long.MAX_VALUE - i, msg.getLongProperty("myLongProperty").longValue());
         Assert.assertEquals(i, msg.getObjectProperty("myObjectProperty"));
         Assert.assertEquals(new Integer(i).shortValue(), msg.getShortProperty("myShortProperty").shortValue());
         Assert.assertEquals("myStringPropertyValue_" + i, msg.getStringProperty("myStringProperty"));
         Assert.assertEquals(international.toString(), msg.getStringProperty("myNonAsciiStringProperty"));
         Assert.assertEquals(special, msg.getStringProperty("mySpecialCharacters"));
      }
   }

   @Test
   public void testLargeMessage() throws Exception
   {
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) servers[0].getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         fileMessage.addBytes(new byte[]{UnitTestCase.getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources();

      producer.send(fileMessage);

      fileMessage.deleteFile();

      session.commit();

      servers[0].stop();

      sf = sfs[1];
      session = addClientSession(sf.createSession(false, true, true));
      ClientConsumer consumer = addClientConsumer(session.createConsumer(queueName));
      session.start();

      ClientMessage msg = consumer.receive(250);

      Assert.assertNotNull(msg);

      Assert.assertEquals(2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      msg.acknowledge();
      session.commit();
   }

   @Test
   public void testPaging() throws Exception
   {
      final int CHUNK_SIZE = 50;
      int messageCount = 0;
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      servers[0].getAddressSettingsRepository().addMatch("#", defaultSetting);

      while (!servers[0].getPagingManager().getPageStore(new SimpleString(addressName)).isPaging())
      {
         for (int i = 0; i < CHUNK_SIZE; i++)
         {
            Message message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[1024]);
            producer.send(message);
            messageCount++;
         }
         session.commit();
      }

      servers[0].stop();

      addConsumer(0, 1, queueName, null);
      for (int i = 0; i < messageCount; i++)
      {
         Assert.assertNotNull(consumers[0].getConsumer().receive(250));
      }

      Assert.assertNull(consumers[0].getConsumer().receive(250));
      removeConsumer(0);
   }

   @Test
   public void testOrderWithPaging() throws Exception
   {
      final int CHUNK_SIZE = 50;
      int messageCount = 0;
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      servers[0].getAddressSettingsRepository().addMatch("#", defaultSetting);

      while (!servers[0].getPagingManager().getPageStore(new SimpleString(addressName)).isPaging())
      {
         for (int i = 0; i < CHUNK_SIZE; i++)
         {
            Message message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[1024]);
            message.putIntProperty("order", i);
            producer.send(message);
            messageCount++;
         }
         session.commit();
      }

      servers[0].stop();

      addConsumer(0, 1, queueName, null);
      for (int i = 0; i < messageCount; i++)
      {
         Assert.assertEquals(i, consumers[0].getConsumer().receive(250).getIntProperty("order").intValue());
      }

      Assert.assertNull(consumers[0].getConsumer().receive(250));
      removeConsumer(0);
   }

   @Test
   public void testFilters() throws Exception
   {
      final int TEST_SIZE = 50;
      final String addressName = "testAddress";
      final String evenQueue = "evenQueue";
      final String oddQueue = "oddQueue";

      createQueue(0, addressName, evenQueue, "0", false);
      createQueue(0, addressName, oddQueue, "1", false);
      createQueue(1, addressName, evenQueue, "0", false);
      createQueue(1, addressName, oddQueue, "1", false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      for (int i = 0; i < TEST_SIZE; i++)
      {
         Message message = session.createMessage(false);
         if (i % 2 == 0)
            message.putStringProperty(ClusterTestBase.FILTER_PROP, new SimpleString("0"));
         else
            message.putStringProperty(ClusterTestBase.FILTER_PROP, new SimpleString("1"));
         producer.send(message);
      }
      session.commit();

      servers[0].stop();

      addConsumer(0, 1, evenQueue, null);
      addConsumer(1, 1, oddQueue, null);
      for (int i = 0; i < TEST_SIZE; i++)
      {
         String compare;
         ClientMessage message;
         if (i % 2 == 0)
         {
            message = consumers[0].getConsumer().receive(250);
            compare = "0";
         }
         else
         {
            message = consumers[1].getConsumer().receive(250);
            compare = "1";
         }
         Assert.assertEquals(compare, message.getStringProperty(ClusterTestBase.FILTER_PROP));
      }

      Assert.assertNull(consumers[0].getConsumer().receive(250));
      Assert.assertNull(consumers[1].getConsumer().receive(250));
      removeConsumer(0);
      removeConsumer(1);
   }
}
