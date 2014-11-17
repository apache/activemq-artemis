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
package org.apache.activemq6.tests.integration;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq6.api.core.HornetQDuplicateIdException;
import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.Message;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientMessage;
import org.apache.activemq6.api.core.client.ClientProducer;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.message.impl.MessageImpl;
import org.apache.activemq6.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.transaction.impl.XidImpl;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.apache.activemq6.utils.UUIDGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A DuplicateDetectionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *         <p/>
 *         Created 9 Dec 2008 12:31:48
 */
public class DuplicateDetectionTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private HornetQServer messagingService;

   private final SimpleString propKey = new SimpleString("propkey");

   private final int cacheSize = 10;

   @Test
   public void testSimpleDuplicateDetecion() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testDuplicateIDCacheMemoryRetentionForNonTemporaryQueues() throws Exception
   {
      testDuplicateIDCacheMemoryRetention(false);
   }

   @Test
   public void testDuplicateIDCacheMemoryRetentionForTemporaryQueues() throws Exception
   {
      testDuplicateIDCacheMemoryRetention(true);
   }

   @Test
   public void testDuplicateIDCacheJournalRetentionForNonTemporaryQueues() throws Exception
   {
      testDuplicateIDCacheMemoryRetention(false);

      messagingService.stop();

      messagingService.start();

      Assert.assertEquals(0, ((PostOfficeImpl) messagingService.getPostOffice()).getDuplicateIDCaches().size());
   }

   @Test
   public void testDuplicateIDCacheJournalRetentionForTemporaryQueues() throws Exception
   {
      testDuplicateIDCacheMemoryRetention(true);

      messagingService.stop();

      messagingService.start();

      Assert.assertEquals(0, ((PostOfficeImpl) messagingService.getPostOffice()).getDuplicateIDCaches().size());
   }

   public void testDuplicateIDCacheMemoryRetention(boolean temporary) throws Exception
   {
      final int TEST_SIZE = 100;

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      locator.setBlockOnNonDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      Assert.assertEquals(0, ((PostOfficeImpl)messagingService.getPostOffice()).getDuplicateIDCaches().size());

      final SimpleString addressName = new SimpleString("DuplicateDetectionTestAddress");

      for (int i = 0; i < TEST_SIZE; i++)
      {
         final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue_" + i);

         if (temporary)
         {
            session.createTemporaryQueue(addressName, queueName, null);
         }
         else
         {
            session.createQueue(addressName, queueName, null, true);
         }

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
         message.putBytesProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID, dupID.getData());
         producer.send(message);
         message2 = consumer.receive(1000);
         Assert.assertEquals(3, message2.getObjectProperty(propKey));

         message = createMessage(session, 4);
         message.putBytesProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID, dupID.getData());
         producer.send(message);
         message2 = consumer.receiveImmediate();
         Assert.assertNull(message2);

         producer.close();
         consumer.close();

         // there will be 2 ID caches, one for messages using "_HQ_DUPL_ID" and one for "_HQ_BRIDGE_DUP"
         Assert.assertEquals(2, ((PostOfficeImpl)messagingService.getPostOffice()).getDuplicateIDCaches().size());
         session.deleteQueue(queueName);
         Assert.assertEquals(0, ((PostOfficeImpl)messagingService.getPostOffice()).getDuplicateIDCaches().size());
      }

      session.close();

      sf.close();

      locator.close();

      Assert.assertEquals(0, ((PostOfficeImpl)messagingService.getPostOffice()).getDuplicateIDCaches().size());
   }

   @Test
   public void testSimpleDuplicateDetectionWithString() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testCacheSize() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName1 = new SimpleString("DuplicateDetectionTestQueue1");

      final SimpleString queueName2 = new SimpleString("DuplicateDetectionTestQueue2");

      final SimpleString queueName3 = new SimpleString("DuplicateDetectionTestQueue3");

      session.createQueue(queueName1, queueName1, null, false);

      session.createQueue(queueName2, queueName2, null, false);

      session.createQueue(queueName3, queueName3, null, false);

      ClientProducer producer1 = session.createProducer(queueName1);
      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientProducer producer2 = session.createProducer(queueName2);
      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientProducer producer3 = session.createProducer(queueName3);
      ClientConsumer consumer3 = session.createConsumer(queueName3);

      for (int i = 0; i < cacheSize; i++)
      {
         SimpleString dupID = new SimpleString("dupID" + i);

         ClientMessage message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      for (int i = 0; i < cacheSize; i++)
      {
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

      DuplicateDetectionTest.log.info("Now sending more");
      for (int i = 0; i < cacheSize; i++)
      {
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

      for (int i = 0; i < cacheSize; i++)
      {
         SimpleString dupID = new SimpleString("dupID2-" + i);

         message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      for (int i = 0; i < cacheSize; i++)
      {
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

      for (int i = 0; i < cacheSize; i++)
      {
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
      for (int i = 0; i < cacheSize; i++)
      {
         SimpleString dupID = new SimpleString("dupID" + i);

         message = createMessage(session, i);

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());

         producer1.send(message);
         producer2.send(message);
         producer3.send(message);
      }

      for (int i = 0; i < cacheSize; i++)
      {
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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testTransactedDuplicateDetection1() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testTransactedDuplicateDetection2() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testTransactedDuplicateDetection3() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      try
      {
         session.commit();
      }
      catch (Exception e)
      {
         session.rollback();
      }

      message = consumer.receive(250);
      Assert.assertEquals(0, message.getObjectProperty(propKey));

      message = consumer.receive(250);
      Assert.assertEquals(1, message.getObjectProperty(propKey));

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testRollbackThenSend() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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
      session.close();

      sf.close();

      locator.close();
   }

   /*
    * Entire transaction should be rejected on duplicate detection
    * Even if not all entries have dupl id header
    */
   @Test
   public void testEntireTransactionRejected() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      final SimpleString queue2 = new SimpleString("queue2");

      session.createQueue(queueName, queueName, null, false);

      session.createQueue(queue2, queue2, null, false);

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


      try
      {
         session.commit();
      }
      catch (HornetQDuplicateIdException die)
      {
         session.rollback();
      }
      catch (HornetQException e)
      {
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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testXADuplicateDetection1() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      DuplicateDetectionTest.log.info("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      DuplicateDetectionTest.log.info("preparing session");
      session.prepare(xid3);

      DuplicateDetectionTest.log.info("committing session");
      session.commit(xid3, false);

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testXADuplicateDetection2() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      DuplicateDetectionTest.log.info("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      DuplicateDetectionTest.log.info("preparing session");
      session.prepare(xid3);

      DuplicateDetectionTest.log.info("committing session");
      session.commit(xid3, false);

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testXADuplicateDetection3() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      message = consumer.receive(250);

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      DuplicateDetectionTest.log.info("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      DuplicateDetectionTest.log.info("preparing session");
      session.prepare(xid3);

      DuplicateDetectionTest.log.info("committing session");
      session.commit(xid3, false);

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testXADuplicateDetectionPrepareAndRollback() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testXADuplicateDetectionPrepareAndRollbackStopServer() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, true);

      ClientProducer producer = session.createProducer(queueName);

      ClientMessage message = createMessage(session, 0);
      SimpleString dupID = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.close();

      messagingService.stop();

      messagingService.start();

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

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testXADuplicateDetection4() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      try
      {
         session.prepare(xid2);
         fail("Should throw an exception here!");
      }
      catch (XAException expected)
      {
      }

      session.rollback(xid2);

      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      message = consumer.receive(250);

      message = consumer.receiveImmediate();
      Assert.assertNull(message);

      DuplicateDetectionTest.log.info("ending session");
      session.end(xid3, XAResource.TMSUCCESS);

      DuplicateDetectionTest.log.info("preparing session");
      session.prepare(xid3);

      DuplicateDetectionTest.log.info("committing session");
      session.commit(xid3, false);

      session.close();

      sf.close();

      locator.close();
   }

   private ClientMessage createMessage(final ClientSession session, final int i)
   {
      ClientMessage message = session.createMessage(false);

      message.putIntProperty(propKey, i);

      return message;
   }

   @Test
   public void testDuplicateCachePersisted() throws Exception
   {
      messagingService.stop();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testDuplicateCachePersisted2() throws Exception
   {
      messagingService.stop();

      final int theCacheSize = 5;

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(theCacheSize);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      for (int i = 0; i < theCacheSize; i++)
      {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         ClientMessage message2 = consumer.receive(1000);
         Assert.assertEquals(i, message2.getObjectProperty(propKey));
      }

      session.close();

      sf.close();

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(queueName, queueName, null, false);

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      for (int i = 0; i < theCacheSize; i++)
      {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         ClientMessage message2 = consumer.receiveImmediate();
         Assert.assertNull(message2);
      }

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testDuplicateCachePersistedRestartWithSmallerCache() throws Exception
   {
      messagingService.stop();

      final int initialCacheSize = 10;
      final int subsequentCacheSize = 5;

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(initialCacheSize);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      for (int i = 0; i < initialCacheSize; i++)
      {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         ClientMessage message2 = consumer.receive(1000);
         Assert.assertEquals(i, message2.getObjectProperty(propKey));
      }

      session.close();

      sf.close();

      messagingService2.stop();

      conf.setIDCacheSize(subsequentCacheSize);

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(queueName, queueName, null, false);

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      for (int i = 0; i < initialCacheSize; i++)
      {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         if (i >= subsequentCacheSize)
         {
            // Message should get through
            ClientMessage message2 = consumer.receive(1000);
            Assert.assertEquals(i, message2.getObjectProperty(propKey));
         }
         else
         {
            ClientMessage message2 = consumer.receiveImmediate();
            Assert.assertNull(message2);
         }
      }

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testDuplicateCachePersistedRestartWithSmallerCacheEnsureDeleted() throws Exception
   {
      messagingService.stop();

      final int initialCacheSize = 10;
      final int subsequentCacheSize = 5;

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(initialCacheSize);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

      ClientProducer producer = session.createProducer(queueName);

      ClientConsumer consumer = session.createConsumer(queueName);

      for (int i = 0; i < initialCacheSize; i++)
      {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         ClientMessage message2 = consumer.receive(1000);
         Assert.assertEquals(i, message2.getObjectProperty(propKey));
      }

      session.close();

      sf.close();

      messagingService2.stop();

      conf.setIDCacheSize(subsequentCacheSize);

      messagingService2 = createServer(conf);

      messagingService2.start();

      // Now stop and set back to original cache size and restart

      messagingService2.stop();

      conf.setIDCacheSize(initialCacheSize);

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(queueName, queueName, null, false);

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      for (int i = 0; i < initialCacheSize; i++)
      {
         ClientMessage message = createMessage(session, i);
         SimpleString dupID = new SimpleString("abcdefg" + i);
         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
         producer.send(message);
         if (i >= subsequentCacheSize)
         {
            // Message should get through
            ClientMessage message2 = consumer.receive(1000);
            Assert.assertEquals(i, message2.getObjectProperty(propKey));
         }
         else
         {
            ClientMessage message2 = consumer.receiveImmediate();
            Assert.assertNull(message2);
         }
      }

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testNoPersist() throws Exception
   {
      messagingService.stop();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize)
         .setPersistIDCache(false);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.start();

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testNoPersistTransactional() throws Exception
   {
      messagingService.stop();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize)
         .setPersistIDCache(false);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      session.start();

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testPersistTransactional() throws Exception
   {
      messagingService.stop();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      session.start();

      session.createQueue(queueName, queueName, null, false);

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      try
      {
         session.commit();
      }
      catch (HornetQDuplicateIdException die)
      {
         session.rollback();
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      try
      {
         session.commit();
      }
      catch (HornetQDuplicateIdException die)
      {
         session.rollback();
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testNoPersistXA1() throws Exception
   {
      messagingService.stop();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize)
         .setPersistIDCache(false);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testNoPersistXA2() throws Exception
   {
      messagingService.stop();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      session.createQueue(queueName, queueName, null, false);

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

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Test
   public void testPersistXA1() throws Exception
   {
      messagingService.stop();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize);

      HornetQServer messagingService2 = createServer(conf);

      messagingService2.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      session.start();

      final SimpleString queueName = new SimpleString("DuplicateDetectionTestQueue");

      session.createQueue(queueName, queueName, null, false);

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

      messagingService2.stop();

      messagingService2 = createServer(conf);

      messagingService2.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      Xid xid2 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      session.start();

      session.createQueue(queueName, queueName, null, false);

      producer = session.createProducer(queueName);

      consumer = session.createConsumer(queueName);

      message = createMessage(session, 1);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID.getData());
      producer.send(message);

      message = createMessage(session, 2);
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID2.getData());
      producer.send(message);

      session.end(xid2, XAResource.TMSUCCESS);
      try
      {
         session.prepare(xid2);
         fail("Should throw an exception here!");
      }
      catch (XAException expected)
      {
      }

      session.rollback(xid2);


      Xid xid3 = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      session.start(xid3, XAResource.TMNOFLAGS);

      ClientMessage message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      message2 = consumer.receiveImmediate();
      Assert.assertNull(message2);

      session.close();

      sf.close();

      locator.close();

      messagingService2.stop();
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createDefaultConfig()
         .setIDCacheSize(cacheSize);

      messagingService = createServer(true, conf);

      messagingService.start();
   }
}
