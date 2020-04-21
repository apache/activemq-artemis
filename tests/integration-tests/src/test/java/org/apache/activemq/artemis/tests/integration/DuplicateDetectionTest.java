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
package org.apache.activemq.artemis.tests.integration;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;

import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DuplicateDetectionTest extends ActiveMQTestBase {

   private final Logger log = Logger.getLogger(this.getClass());

   private ActiveMQServer server;

   private final SimpleString propKey = new SimpleString("propkey");

   private final int cacheSize = 10;

   @Test
   public void testSimpleDuplicateDetecion() throws Exception {
      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 0);
      producer.send(message);
      ClientMessage message2 = consumer.receive(1000);
      Assert.assertEquals(0, message2.getObjectProperty(propKey));

      message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      message2 = consumer.receive(1000);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message = createMessage(session, 3);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      // Now try with a different id

      message = createMessage(session, 4);
      SimpleString dupID2 = new SimpleString("hijklmnop");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      message2 = consumer.receive(1000);
      Assert.assertEquals(4, message2.getObjectProperty(propKey));

      message = createMessage(session, 5);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message = createMessage(session, 6);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);
   }

   @Test
   public void testDuplicateIDCacheMemoryRetentionForNonTemporaryQueues() throws Exception {
      testDuplicateIDCacheMemoryRetention(false);
   }

   @Test
   public void testDuplicateIDCacheMemoryRetentionForTemporaryQueues() throws Exception {
      testDuplicateIDCacheMemoryRetention(true);
   }

   @Test
   public void testDuplicateIDCacheJournalRetentionForNonTemporaryQueues() throws Exception {
      testDuplicateIDCacheMemoryRetention(false);

      server.stop();

      waitForServerToStop(server);

      server.start();

      Assert.assertEquals(0, ((PostOfficeImpl) server.getPostOffice()).getDuplicateIDCaches().size());
   }

   @Test
   public void testDuplicateIDCacheJournalRetentionForTemporaryQueues() throws Exception {
      testDuplicateIDCacheMemoryRetention(true);

      server.stop();

      waitForServerToStop(server);

      server.start();

      Assert.assertEquals(0, ((PostOfficeImpl) server.getPostOffice()).getDuplicateIDCaches().size());
   }

   public void testDuplicateIDCacheMemoryRetention(boolean temporary) throws Exception {
      final int TEST_SIZE = 100;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      Assert.assertEquals(0, ((PostOfficeImpl) server.getPostOffice()).getDuplicateIDCaches().size());

      final SimpleString addressName = new SimpleString("DuplicateDetectionTestAddress");

      for (int i = 0; i < TEST_SIZE; i++) {
         final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue_" + i);

         session.createQueue(new QueueConfiguration(queueName).setAddress(addressName).setDurable(!temporary).setTemporary(temporary));

         ClientProducer producer = session.createProducer(addressName);

         ClientConsumer consumer = session.createConsumer(queueName);

         ClientMessage message = createMessage(session, 1);
         SimpleString dupID = new SimpleString("abcdefg");
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         ClientMessage message2 = consumer.receive(1000);
         Assert.assertEquals(1, message2.getObjectProperty(propKey));

         message = createMessage(session, 2);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         message2 = consumer.receiveImmediate();
         Assert.assertNull(message2);

         message = createMessage(session, 3);
         message.putBytesProperty(Message.HDR_BRIDGE_DUPLICATE_ID, dupID.getData());
         producer.send(message);
         message2 = consumer.receive(1000);
         Assert.assertEquals(3, message2.getObjectProperty(propKey));

         message = createMessage(session, 4);
         message.putBytesProperty(Message.HDR_BRIDGE_DUPLICATE_ID, dupID.getData());
         producer.send(message);
         message2 = consumer.receiveImmediate();
         Assert.assertNull(message2);

         producer.close();
         consumer.close();

         // there will be 2 ID caches, one for messages using "_AMQ_DUPL_ID" and one for "_AMQ_BRIDGE_DUP"
         Assert.assertEquals(2, ((PostOfficeImpl) server.getPostOffice()).getDuplicateIDCaches().size());
         session.deleteQueue(queueName);
         Assert.assertEquals(0, ((PostOfficeImpl) server.getPostOffice()).getDuplicateIDCaches().size());
      }

      Assert.assertEquals(0, ((PostOfficeImpl) server.getPostOffice()).getDuplicateIDCaches().size());
   }

   // It is important to test the shrink with this rule
   // because we could have this after crashes
   // we would eventually have a higher number of caches while we couldn't have time to clear previous ones
   @Test
   public void testShrinkCache() throws Exception {
      server.stop();
      server.getConfiguration().setIDCacheSize(150);
      server.start();

      final int TEST_SIZE = 200;

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      locator.setBlockOnNonDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");
      session.createQueue(new QueueConfiguration(queueName));

      ClientProducer producer = session.createProducer(queueName);

      for (int i = 0; i < TEST_SIZE; i++) {
         ClientMessage message = session.createMessage(true);
         message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, SimpleString.toSimpleString("DUPL-" + i));
         producer.send(message);
      }
      session.commit();

      sf.close();
      session.close();
      locator.close();

      server.stop();

      server.getConfiguration().setIDCacheSize(100);

      server.start();

      locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      locator.setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);
      session = sf.createSession(false, false, false);
      session.start();

      producer = session.createProducer(queueName);

      // will send the last 50 again
      for (int i = TEST_SIZE - 50; i < TEST_SIZE; i++) {
         ClientMessage message = session.createMessage(true);
         message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, SimpleString.toSimpleString("DUPL-" + i));
         producer.send(message);
      }

      try {
         session.commit();
         Assert.fail("Exception expected");
      } catch (ActiveMQException expected) {

      }

   }

   @Test
   public void testSimpleDuplicateDetectionWithString() throws Exception {
      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 0);
      producer.send(message);
      ClientMessage message2 = consumer.receive(1000);
      Assert.assertEquals(0, message2.getObjectProperty(propKey));

      message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID);
      producer.send(message);
      message2 = consumer.receive(1000);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID);
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message = createMessage(session, 3);
      message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID);
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      // Now try with a different id

      message = createMessage(session, 4);
      SimpleString dupID2 = new SimpleString("hijklmnop");
      message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2);
      producer.send(message);
      message2 = consumer.receive(1000);
      Assert.assertEquals(4, message2.getObjectProperty(propKey));

      message = createMessage(session, 5);
      message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2);
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message = createMessage(session, 6);
      message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID);
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);
   }

   @Test
   public void testCacheSize() throws Exception {
      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName1 = new SimpleString("DuplicateDetectionTestQueue1");

      final SimpleString queueName2 = new SimpleString("DuplicateDetectionTestQueue2");

      final SimpleString queueName3 = new SimpleString("DuplicateDetectionTestQueue3");

      session.createQueue(new QueueConfiguration(queueName1).setDurable(false));

      session.createQueue(new QueueConfiguration(queueName2).setDurable(false));

      session.createQueue(new QueueConfiguration(queueName3).setDurable(false));

      ClientProducer producer1 = session.createProducer(queueName1);
      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientProducer producer2 = session.createProducer(queueName2);
      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientProducer producer3 = session.createProducer(queueName3);
      ClientConsumer consumer3 = session.createConsumer(queueName3);

      for (int i = 0; i < cacheSize; i++) {
         SimpleString dupID = new SimpleString("dupID" + i);

         ClientMessage message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      for (int i = 0; i < cacheSize; i++) {
         ClientMessage message = consumer1.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message = consumer2.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message = consumer3.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
      }

      log.debug("Now sending more");
      for (int i = 0; i < cacheSize; i++) {
         SimpleString dupID = new SimpleString("dupID" + i);

         ClientMessage message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      ClientMessage message = consumer1.receiveImmediate();
      Assert.assertNull(message);
      message = consumer2.receiveImmediate();
      Assert.assertNull(message);
      message = consumer3.receiveImmediate();
      Assert.assertNull(message);

      for (int i = 0; i < cacheSize; i++) {
         SimpleString dupID = new SimpleString("dupID2-" + i);

         message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      for (int i = 0; i < cacheSize; i++) {
         message = consumer1.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message = consumer2.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message = consumer3.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
      }

      for (int i = 0; i < cacheSize; i++) {
         SimpleString dupID = new SimpleString("dupID2-" + i);

         message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      message = consumer1.receiveImmediate();
      Assert.assertNull(message);
      message = consumer2.receiveImmediate();
      Assert.assertNull(message);
      message = consumer3.receiveImmediate();
      Assert.assertNull(message);

      // Should be able to send the first lot again now - since the second lot pushed the
      // first lot out of the cache
      for (int i = 0; i < cacheSize; i++) {
         SimpleString dupID = new SimpleString("dupID" + i);

         message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      for (int i = 0; i < cacheSize; i++) {
         message = consumer1.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message = consumer2.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message = consumer3.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
      }
   }

   @Test
   public void testTransactedDuplicateDetection1() throws Exception {
      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.close();

      session = sf.createSession(false, false, false);

      session.start();

      producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      // Should be able to resend it and not get rejected since transaction didn't commit

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.commit();

      message = consumer.receive(250);
      Assert.assertEquals(1, message.getObjectProperty(propKey));

      message = consumer.receiveImmediate();
      Assert.assertNull(message);
   }

   @Test
   public void testTransactedDuplicateDetection2() throws Exception {
      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.rollback();

      // Should be able to resend it and not get rejected since transaction didn't commit

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.commit();

      message = consumer.receive(250);
      Assert.assertEquals(1, message.getObjectProperty(propKey));

      message = consumer.receiveImmediate();
      Assert.assertNull(message);
   }

   @Test
   public void testTransactedDuplicateDetection3() throws Exception {
      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID1 = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID1.getData());
      producer.send(message);

      message = createMessage(session, 1);
      SimpleString dupID2 = new SimpleString("hijklmno");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.commit();

      // These next two should get rejected

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID1.getData());
      producer.send(message);

      message = createMessage(session, 3);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      try {
         session.commit();
      } catch (Exception e) {
         session.rollback();
      }

      message = consumer.receive(250);
      Assert.assertEquals(0, message.getObjectProperty(propKey));

      message = consumer.receive(250);
      Assert.assertEquals(1, message.getObjectProperty(propKey));

      message = consumer.receiveImmediate();
      Assert.assertNull(message);
   }

   @Test
   public void testRollbackThenSend() throws Exception {
      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID1 = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID1.getData());
      message.putStringProperty("key", dupID1.toString());
      producer.send(message);

      session.rollback();

      message = createMessage(session, 0);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID1.getData());
      message.putStringProperty("key", dupID1.toString());
      producer.send(message);

      session.commit();

      message = consumer.receive(5000);
      assertNotNull(message);
      assertTrue(message.getStringProperty("key").equals(dupID1.toString()));
   }

   /*
    * Entire transaction should be rejected on duplicate detection
    * Even if not all entries have dupl id header
    */
   @Test
   public void testEntireTransactionRejected() throws Exception {
      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      final SimpleString queue2 = new SimpleString("queue2");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      session.createQueue(new QueueConfiguration(queue2).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      ClientMessage message2 = createMessage(session, 0);
      ClientProducer producer2 = session.createProducer(queue2);
      producer2.send(message2);

      session.commit();

      session.close();

      session = sf.createSession(false, false, false);

      session.start();

      ClientConsumer consumer2 = session.createConsumer(queue2);

      producer = session.createProducer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      producer.send(message);

      message = createMessage(session, 3);
      producer.send(message);

      message = createMessage(session, 4);
      producer.send(message);

      message = consumer2.receive(5000);
      assertNotNull(message);
      message.acknowledge();

      try {
         session.commit();
      } catch (ActiveMQDuplicateIdException die) {
         session.rollback();
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      ClientConsumer consumer = session.createConsumer(queueName);

      message = consumer.receive(250);
      Assert.assertEquals(0, message.getObjectProperty(propKey));

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      message = consumer2.receive(5000);
      assertNotNull(message);

      message.acknowledge();

      session.commit();
   }

   @Test
   public void testXADuplicateDetection1() throws Exception {
      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.close();

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session = sf.createSession(true, false, false);

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      // Should be able to resend it and not get rejected since transaction didn't commit

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      message = consumer.receive(250);
      Assert.assertEquals(1, message.getObjectProperty(propKey));

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      log.debug("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      log.debug("preparing session");
      session.prepare(xid3);

      log.debug("committing session");
      session.commit(xid3, false);
   }

   @Test
   public void testXADuplicateDetection2() throws Exception {
      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.rollback(xid);

      session.close();

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session = sf.createSession(true, false, false);

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      // Should be able to resend it and not get rejected since transaction didn't commit

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      message = consumer.receive(250);
      Assert.assertEquals(1, message.getObjectProperty(propKey));

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      log.debug("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      log.debug("preparing session");
      session.prepare(xid3);

      log.debug("committing session");
      session.commit(xid3, false);
   }

   @Test
   public void testXADuplicateDetection3() throws Exception {
      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.close();

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session = sf.createSession(true, false, false);

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      // Should NOT be able to resend it

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      consumer.receive(250);

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      log.debug("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      log.debug("preparing session");
      session.prepare(xid3);

      log.debug("committing session");
      session.commit(xid3, false);
   }

   @Test
   public void testXADuplicateDetectionPrepareAndRollback() throws Exception {
      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.rollback(xid);

      session.close();

      Xid xid2 = new XidImpl("xa2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session = sf.createSession(true, false, false);

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      producer = session.createProducer(queueName);

      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);

      session.close();

      session = sf.createSession(false, false, false);

      session.start();

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage msgRec = consumer.receive(5000);
      assertNotNull(msgRec);
      msgRec.acknowledge();

      session.commit();
   }

   @Test
   public void testXADuplicateDetectionPrepareAndRollbackStopServer() throws Exception {
      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      session.start(xid, XAResource.TMJOIN);

      session.end(xid, XAResource.TMSUCCESS);

      session.rollback(xid);

      session.close();

      Xid xid2 = new XidImpl("xa2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session = sf.createSession(true, false, false);

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      producer = session.createProducer(queueName);

      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);

      session.close();

      session = sf.createSession(false, false, false);

      session.start();

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage msgRec = consumer.receive(5000);
      assertNotNull(msgRec);
      msgRec.acknowledge();

      session.commit();
   }

   @Test
   public void testXADuplicateDetection4() throws Exception {
      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.commit(xid, false);

      session.close();

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session = sf.createSession(true, false, false);

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      // Should NOT be able to resend it

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);

      try {
         session.prepare(xid2);
         fail("Should throw an exception here!");
      } catch (XAException expected) {
         assertTrue(expected.getCause().toString().contains("DUPLICATE_ID_REJECTED"));
      }

      session.rollback(xid2);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      consumer.receive(250);

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      log.debug("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      log.debug("preparing session");
      session.prepare(xid3);

      log.debug("committing session");
      session.commit(xid3, false);
   }

   private ClientMessage createMessage(final ClientSession session, final int i) {
      ClientMessage message = session.createMessage(false);

      message.putIntProperty(propKey, i);

      return message;
   }

   @Test
   public void testDuplicateCachePersisted() throws Exception {
      server.stop();

      config = createDefaultInVMConfig().setIDCacheSize(cacheSize);

      server = createServer(config);

      server.start();

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      ClientMessage message2 = consumer.receive(1000);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      SimpleString dupID2 = new SimpleString("hijklmnopqr");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      message2 = consumer.receive(1000);
      Assert.assertEquals(2, message2.getObjectProperty(propKey));

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);
   }

   @Test
   public void testDuplicateCachePersisted2() throws Exception {
      server.stop();

      final int theCacheSize = 5;

      config = createDefaultInVMConfig().setIDCacheSize(theCacheSize);

      server = createServer(config);

      server.start();

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      for (int i = 0; i < theCacheSize; i++) {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         ClientMessage message2 = consumer.receive(1000);
         Assert.assertEquals(i, message2.getObjectProperty(propKey));
      }

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      for (int i = 0; i < theCacheSize; i++) {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         ClientMessage message2 = consumer.receiveImmediate();
         Assert.assertNull(message2);
      }
   }

   @Test
   public void testNoPersist() throws Exception {
      server.stop();

      config = createDefaultInVMConfig().setIDCacheSize(cacheSize).setPersistIDCache(false);

      server = createServer(config);

      server.start();

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      ClientMessage message2 = consumer.receive(1000);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      SimpleString dupID2 = new SimpleString("hijklmnopqr");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      message2 = consumer.receive(1000);
      Assert.assertEquals(2, message2.getObjectProperty(propKey));

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      message2 = consumer.receive(200);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      message2 = consumer.receive(200);
      Assert.assertEquals(2, message2.getObjectProperty(propKey));
   }

   @Test
   public void testNoPersistTransactional() throws Exception {
      server.stop();

      config = createDefaultInVMConfig().setIDCacheSize(cacheSize).setPersistIDCache(false);

      server = createServer(config);

      server.start();

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      session.commit();
      ClientMessage message2 = consumer.receive(1000);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      SimpleString dupID2 = new SimpleString("hijklmnopqr");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      session.commit();
      message2 = consumer.receive(1000);
      Assert.assertEquals(2, message2.getObjectProperty(propKey));

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      session.commit();
      message2 = consumer.receive(200);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      session.commit();
      message2 = consumer.receive(200);
      Assert.assertEquals(2, message2.getObjectProperty(propKey));
   }

   @Test
   public void testPersistTransactional() throws Exception {
      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);
      session.commit();
      ClientMessage message2 = consumer.receive(1000);
      message2.acknowledge();
      session.commit();
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message = createMessage(session, 2);
      SimpleString dupID2 = new SimpleString("hijklmnopqr");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);
      session.commit();
      message2 = consumer.receive(1000);
      message2.acknowledge();
      session.commit();
      Assert.assertEquals(2, message2.getObjectProperty(propKey));

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      try {
         session.commit();
      } catch (ActiveMQDuplicateIdException die) {
         session.rollback();
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      try {
         session.commit();
      } catch (ActiveMQDuplicateIdException die) {
         session.rollback();
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);
   }

   @Test
   public void testNoPersistXA1() throws Exception {
      server.stop();

      config = createDefaultInVMConfig().setIDCacheSize(cacheSize).setPersistIDCache(false);

      server = createServer(config);

      server.start();

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      SimpleString dupID2 = new SimpleString("hijklmnopqr");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.commit(xid, false);

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);
      session.prepare(xid2);
      session.commit(xid2, false);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      ClientMessage message2 = consumer.receive(200);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message2 = consumer.receive(200);
      Assert.assertEquals(2, message2.getObjectProperty(propKey));
   }

   @Test
   public void testNoPersistXA2() throws Exception {
      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      SimpleString dupID2 = new SimpleString("hijklmnopqr");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = addClientSession(sf.createSession(true, false, false));

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);
      session.prepare(xid2);
      session.commit(xid2, false);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      ClientMessage message2 = consumer.receive(200);
      Assert.assertEquals(1, message2.getObjectProperty(propKey));

      message2 = consumer.receive(200);
      Assert.assertEquals(2, message2.getObjectProperty(propKey));
   }

   @Test
   public void testPersistXA1() throws Exception {
      ClientSession session = addClientSession(sf.createSession(true, false, false));

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      ClientMessage message = createMessage(session, 1);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      SimpleString dupID2 = new SimpleString("hijklmnopqr");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.commit(xid, false);

      session.close();

      sf.close();

      server.stop();

      waitForServerToStop(server);

      server.start();

      sf = createSessionFactory(locator);

      session = addClientSession(sf.createSession(true, false, false));

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      session.createQueue(new QueueConfiguration(queueName).setDurable(false));

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);
      try {
         session.prepare(xid2);
         fail("Should throw an exception here!");
      } catch (XAException expected) {
      }

      session.rollback(xid2);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      ClientMessage message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);
   }

   private Configuration config;
   ServerLocator locator;
   ClientSessionFactory sf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      config = createDefaultInVMConfig().setIDCacheSize(cacheSize);

      server = createServer(true, config);

      server.start();

      locator = createInVMNonHALocator();

      sf = createSessionFactory(locator);
   }
}
