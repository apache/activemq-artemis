/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import static org.apache.activemq.transport.amqp.AmqpSupport.LIFETIME_POLICY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_QUEUE_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_TOPIC_CAPABILITY;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for temporary destination handling over AMQP
 */
public class AmqpTempDestinationTest extends AmqpClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(AmqpTempDestinationTest.class);

   @Test(timeout = 60000)
   public void testCreateDynamicSenderToTopic() throws Exception {
      doTestCreateDynamicSender(true);
   }

   @Test(timeout = 60000)
   public void testCreateDynamicSenderToQueue() throws Exception {
      doTestCreateDynamicSender(false);
   }

   @SuppressWarnings("unchecked")
   protected void doTestCreateDynamicSender(boolean topic) throws Exception {
      Target target = createDynamicTarget(topic);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(target);
      assertNotNull(sender);

      Target remoteTarget = (Target) sender.getEndpoint().getRemoteTarget();
      assertTrue(remoteTarget.getDynamic());
      assertTrue(remoteTarget.getDurable().equals(TerminusDurability.NONE));
      assertTrue(remoteTarget.getExpiryPolicy().equals(TerminusExpiryPolicy.LINK_DETACH));

      // Check the dynamic node lifetime-policy
      Map<Symbol, Object> dynamicNodeProperties = remoteTarget.getDynamicNodeProperties();
      assertTrue(dynamicNodeProperties.containsKey(LIFETIME_POLICY));
      assertEquals(DeleteOnClose.getInstance(), dynamicNodeProperties.get(LIFETIME_POLICY));

      Queue queueView = getProxyToQueue(remoteTarget.getAddress());
      assertNotNull(queueView);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testDynamicSenderLifetimeBoundToLinkTopic() throws Exception {
      doTestDynamicSenderLifetimeBoundToLinkQueue(true);
   }

   @Test(timeout = 60000)
   public void testDynamicSenderLifetimeBoundToLinkQueue() throws Exception {
      doTestDynamicSenderLifetimeBoundToLinkQueue(false);
   }

   protected void doTestDynamicSenderLifetimeBoundToLinkQueue(boolean topic) throws Exception {
      Target target = createDynamicTarget(topic);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(target);
      assertNotNull(sender);

      Target remoteTarget = (Target) sender.getEndpoint().getRemoteTarget();
      Queue queueView = getProxyToQueue(remoteTarget.getAddress());
      assertNotNull(queueView);

      sender.close();

      Thread.sleep(200);

      queueView = getProxyToQueue(remoteTarget.getAddress());
      assertNull(queueView);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testCreateDynamicReceiverToTopic() throws Exception {
      doTestCreateDynamicSender(true);
   }

   @Test(timeout = 60000)
   public void testCreateDynamicReceiverToQueue() throws Exception {
      doTestCreateDynamicSender(false);
   }

   @SuppressWarnings("unchecked")
   protected void doTestCreateDynamicReceiver(boolean topic) throws Exception {
      Source source = createDynamicSource(topic);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(source);
      assertNotNull(receiver);

      Source remoteSource = (Source) receiver.getEndpoint().getRemoteSource();
      assertTrue(remoteSource.getDynamic());
      assertTrue(remoteSource.getDurable().equals(TerminusDurability.NONE));
      assertTrue(remoteSource.getExpiryPolicy().equals(TerminusExpiryPolicy.LINK_DETACH));

      // Check the dynamic node lifetime-policy
      Map<Symbol, Object> dynamicNodeProperties = remoteSource.getDynamicNodeProperties();
      assertTrue(dynamicNodeProperties.containsKey(LIFETIME_POLICY));
      assertEquals(DeleteOnClose.getInstance(), dynamicNodeProperties.get(LIFETIME_POLICY));

      Queue queueView = getProxyToQueue(remoteSource.getAddress());
      assertNotNull(queueView);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testDynamicReceiverLifetimeBoundToLinkTopic() throws Exception {
      doTestDynamicReceiverLifetimeBoundToLinkQueue(true);
   }

   @Test(timeout = 60000)
   public void testDynamicReceiverLifetimeBoundToLinkQueue() throws Exception {
      doTestDynamicReceiverLifetimeBoundToLinkQueue(false);
   }

   protected void doTestDynamicReceiverLifetimeBoundToLinkQueue(boolean topic) throws Exception {
      Source source = createDynamicSource(topic);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(source);
      assertNotNull(receiver);

      Source remoteSource = (Source) receiver.getEndpoint().getRemoteSource();
      Queue queueView = getProxyToQueue(remoteSource.getAddress());
      assertNotNull(queueView);

      receiver.close();

      queueView = getProxyToQueue(remoteSource.getAddress());
      assertNull(queueView);

      connection.close();
   }

   @Test(timeout = 60000)
   public void TestCreateDynamicQueueSenderAndPublish() throws Exception {
      doTestCreateDynamicSenderAndPublish(false);
   }

   @Test(timeout = 60000)
   public void TestCreateDynamicTopicSenderAndPublish() throws Exception {
      doTestCreateDynamicSenderAndPublish(true);
   }

   protected void doTestCreateDynamicSenderAndPublish(boolean topic) throws Exception {
      Target target = createDynamicTarget(topic);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(target);
      assertNotNull(sender);

      Target remoteTarget = (Target) sender.getEndpoint().getRemoteTarget();
      Queue queueView = getProxyToQueue(remoteTarget.getAddress());
      assertNotNull(queueView);

      // Get the new address
      String address = sender.getSender().getRemoteTarget().getAddress();
      LOG.info("New dynamic sender address -> {}", address);

      // Create a message and send to a receive that is listening on the newly
      // created dynamic link address.
      AmqpMessage message = new AmqpMessage();
      message.setMessageId("msg-1");
      message.setText("Test-Message");

      AmqpReceiver receiver = session.createReceiver(address);
      receiver.flow(1);

      sender.send(message);

      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull("Should have read a message", received);
      received.accept();

      receiver.close();
      sender.close();

      connection.close();
   }

   @Test(timeout = 60000)
   public void testCreateDynamicReceiverToTopicAndSend() throws Exception {
      doTestCreateDynamicSender(true);
   }

   @Test(timeout = 60000)
   public void testCreateDynamicReceiverToQueueAndSend() throws Exception {
      doTestCreateDynamicSender(false);
   }

   protected void doTestCreateDynamicReceiverAndSend(boolean topic) throws Exception {
      Source source = createDynamicSource(topic);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(source);
      assertNotNull(receiver);

      Source remoteSource = (Source) receiver.getEndpoint().getRemoteSource();
      Queue queueView = getProxyToQueue(remoteSource.getAddress());
      assertNotNull(queueView);

      // Get the new address
      String address = receiver.getReceiver().getRemoteSource().getAddress();
      LOG.info("New dynamic receiver address -> {}", address);

      // Create a message and send to a receive that is listening on the newly
      // created dynamic link address.
      AmqpMessage message = new AmqpMessage();
      message.setMessageId("msg-1");
      message.setText("Test-Message");

      AmqpSender sender = session.createSender(address);
      sender.send(message);

      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull("Should have read a message", received);
      received.accept();

      sender.close();
      receiver.close();

      connection.close();
   }

   protected Source createDynamicSource(boolean topic) {

      Source source = new Source();
      source.setDynamic(true);
      source.setDurable(TerminusDurability.NONE);
      source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

      // Set the dynamic node lifetime-policy
      Map<Symbol, Object> dynamicNodeProperties = new HashMap<>();
      dynamicNodeProperties.put(LIFETIME_POLICY, DeleteOnClose.getInstance());
      source.setDynamicNodeProperties(dynamicNodeProperties);

      // Set the capability to indicate the node type being created
      if (!topic) {
         source.setCapabilities(TEMP_QUEUE_CAPABILITY);
      } else {
         source.setCapabilities(TEMP_TOPIC_CAPABILITY);
      }

      return source;
   }

   protected Target createDynamicTarget(boolean topic) {

      Target target = new Target();
      target.setDynamic(true);
      target.setDurable(TerminusDurability.NONE);
      target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

      // Set the dynamic node lifetime-policy
      Map<Symbol, Object> dynamicNodeProperties = new HashMap<>();
      dynamicNodeProperties.put(LIFETIME_POLICY, DeleteOnClose.getInstance());
      target.setDynamicNodeProperties(dynamicNodeProperties);

      // Set the capability to indicate the node type being created
      if (!topic) {
         target.setCapabilities(TEMP_QUEUE_CAPABILITY);
      } else {
         target.setCapabilities(TEMP_TOPIC_CAPABILITY);
      }

      return target;
   }
}
