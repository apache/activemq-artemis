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
package org.apache.activemq.artemis.tests.integration.client;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RefCountMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.encode.BodyType;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQConsumerContext;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.spi.core.remoting.ConsumerContext;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.UUID;
import org.junit.Assert;
import org.junit.Test;

public class AcknowledgeTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   @Test
   public void testReceiveAckLastMessageOnly() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator().setAckBatchSize(0).setBlockOnAcknowledge(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++) {
         cp.send(sendSession.createMessage(false));
      }
      session.start();
      ClientMessage cm = null;
      for (int i = 0; i < numMessages; i++) {
         cm = cc.receive(5000);
         Assert.assertNotNull(cm);
      }
      cm.acknowledge();
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();

      Assert.assertEquals(0, q.getDeliveringCount());
      session.close();
      sendSession.close();
   }

   @Test
   public void testAsyncConsumerNoAck() throws Exception {
      ActiveMQServer server = createServer(false);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 3;
      for (int i = 0; i < numMessages; i++) {
         cp.send(sendSession.createMessage(false));
      }

      Thread.sleep(500);
      log.info("woke up");

      final CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new MessageHandler() {
         int c = 0;

         @Override
         public void onMessage(final ClientMessage message) {
            log.info("Got message " + c++);
            latch.countDown();
         }
      });
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      sendSession.close();
      session.close();
   }

   @Test
   public void testAsyncConsumerAck() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator().setBlockOnAcknowledge(true).setAckBatchSize(0);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      final ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++) {
         cp.send(sendSession.createMessage(false));
      }
      final CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new MessageHandler() {
         @Override
         public void onMessage(final ClientMessage message) {
            try {
               message.acknowledge();
            } catch (ActiveMQException e) {
               try {
                  session.close();
               } catch (ActiveMQException e1) {
                  e1.printStackTrace();
               }
            }
            latch.countDown();
         }
      });
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(0, q.getDeliveringCount());
      sendSession.close();
      session.close();
   }

   /**
    * This is validating a case where a consumer will try to ack a message right after failover, but the consumer at the target server didn't
    * receive the message yet.
    * on that case the system should rollback any acks done and redeliver any messages
    */
   @Test
   public void testInvalidACK() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();

      ServerLocator locator = createInVMNonHALocator().setAckBatchSize(0).setBlockOnAcknowledge(true);

      ClientSessionFactory cf = createSessionFactory(locator);

      int numMessages = 100;

      ClientSession sessionConsumer = cf.createSession(true, true, 0);

      sessionConsumer.start();

      sessionConsumer.createQueue(addressA, queueA, true);

      ClientConsumer consumer = sessionConsumer.createConsumer(queueA);

      // sending message
      {
         ClientSession sendSession = cf.createSession(false, true, true);

         ClientProducer cp = sendSession.createProducer(addressA);

         for (int i = 0; i < numMessages; i++) {
            ClientMessage msg = sendSession.createMessage(true);
            msg.putIntProperty("seq", i);
            cp.send(msg);
         }

         sendSession.close();
      }

      {

         ClientMessage msg = consumer.receive(5000);

         // need to way some time before all the possible references are sent to the consumer
         // as we need to guarantee the order on cancellation on this test
         Thread.sleep(1000);

         try {
            // pretending to be an unbehaved client doing an invalid ack right after failover
            ((ClientSessionInternal) sessionConsumer).acknowledge(new FakeConsumerWithID(0), new FakeMessageWithID(12343));
            fail("supposed to throw an exception here");
         } catch (Exception e) {
         }

         try {
            // pretending to be an unbehaved client doing an invalid ack right after failover
            ((ClientSessionInternal) sessionConsumer).acknowledge(new FakeConsumerWithID(3), new FakeMessageWithID(12343));
            fail("supposed to throw an exception here");
         } catch (Exception e) {
            e.printStackTrace();
         }

         consumer.close();

         consumer = sessionConsumer.createConsumer(queueA);

         for (int i = 0; i < numMessages; i++) {
            msg = consumer.receive(5000);
            assertNotNull(msg);
            assertEquals(i, msg.getIntProperty("seq").intValue());
            msg.acknowledge();
         }
      }
   }

   @Test
   public void testAsyncConsumerAckLastMessageOnly() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator().setBlockOnAcknowledge(true).setAckBatchSize(0);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      final ClientSession session = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++) {
         cp.send(sendSession.createMessage(false));
      }
      final CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new MessageHandler() {
         @Override
         public void onMessage(final ClientMessage message) {
            if (latch.getCount() == 1) {
               try {
                  message.acknowledge();
               } catch (ActiveMQException e) {
                  try {
                     session.close();
                  } catch (ActiveMQException e1) {
                     e1.printStackTrace();
                  }
               }
            }
            latch.countDown();
         }
      });
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(0, q.getDeliveringCount());
      sendSession.close();
      session.close();
   }

   class FakeConsumerWithID implements ClientConsumer {

      final long id;

      FakeConsumerWithID(long id) {
         this.id = id;
      }

      @Override
      public ConsumerContext getConsumerContext() {
         return new ActiveMQConsumerContext(this.id);
      }

      @Override
      public ClientMessage receive() throws ActiveMQException {
         return null;
      }

      @Override
      public ClientMessage receive(long timeout) throws ActiveMQException {
         return null;
      }

      @Override
      public ClientMessage receiveImmediate() throws ActiveMQException {
         return null;
      }

      @Override
      public MessageHandler getMessageHandler() throws ActiveMQException {
         return null;
      }

      @Override
      public FakeConsumerWithID setMessageHandler(MessageHandler handler) throws ActiveMQException {
         return this;
      }

      @Override
      public void close() throws ActiveMQException {

      }

      @Override
      public boolean isClosed() {
         return false;
      }

      @Override
      public Exception getLastException() {
         return null;
      }
   }

   class FakeMessageWithID extends RefCountMessage {

      @Override
      public int getPersistSize() {
         return 0;
      }

      @Override
      public void persist(ActiveMQBuffer targetRecord) {
      }

      @Override
      public Persister<Message> getPersister() {
         return null;
      }

      @Override
      public Message setProtocol(Object protocol) {
         return this;
      }

      @Override
      public void reloadPersistence(ActiveMQBuffer record) {

      }

      @Override
      public Message toCore() {
         return this;
      }

      @Override
      public void receiveBuffer(ByteBuf buffer) {

      }

      @Override
      public void sendBuffer(ByteBuf buffer, int count) {

      }

      @Override
      public LargeBodyEncoder getBodyEncoder() throws ActiveMQException {
         return null;
      }

      @Override
      public Message setUserID(Object userID) {
         return null;
      }

      @Override
      public void copyHeadersAndProperties(Message msg) {

      }

      @Override
      public void messageChanged() {

      }

      @Override
      public ActiveMQBuffer getReadOnlyBodyBuffer() {
         return null;
      }

      final long id;

      @Override
      public Message setType(byte type) {
         return null;
      }

      @Override
      public Message copy() {
         return null;
      }

      @Override
      public Message copy(long newID) {
         return null;
      }

      @Override
      public Message setMessageID(long id) {
         return null;
      }

      @Override
      public int getRefCount() {
         return 0;
      }

      @Override
      public int incrementRefCount() throws Exception {
         return 0;
      }

      @Override
      public int decrementRefCount() throws Exception {
         return 0;
      }

      @Override
      public int incrementDurableRefCount() {
         return 0;
      }

      @Override
      public int decrementDurableRefCount() {
         return 0;
      }

      @Override
      public int getMemoryEstimate() {
         return 0;
      }

      FakeMessageWithID(final long id) {
         this.id = id;
      }

      @Override
      public long getMessageID() {
         return id;
      }

      @Override
      public UUID getUserID() {
         return null;
      }

      @Override
      public String getAddress() {
         return null;
      }

      @Override
      public SimpleString getAddressSimpleString() {
         return null;
      }

      @Override
      public Message setBuffer(ByteBuf buffer) {
         return null;
      }

      @Override
      public ByteBuf getBuffer() {
         return null;
      }

      @Override
      public Object getProtocol() {
         return null;
      }

      @Override
      public Object getBody() {
         return null;
      }

      @Override
      public BodyType getBodyType() {
         return null;
      }

      @Override
      public Message setBody(BodyType type, Object body) {
         return null;
      }

      @Override
      public Message setAddress(String address) {
         return null;
      }

      @Override
      public Message setAddress(SimpleString address) {
         return null;
      }

      @Override
      public byte getType() {
         return 0;
      }

      @Override
      public boolean isDurable() {
         return false;
      }

      @Override
      public FakeMessageWithID setDurable(boolean durable) {
         return this;
      }

      @Override
      public long getExpiration() {
         return 0;
      }

      @Override
      public boolean isExpired() {
         return false;
      }

      @Override
      public FakeMessageWithID setExpiration(long expiration) {
         return this;
      }

      @Override
      public long getTimestamp() {
         return 0;
      }

      @Override
      public FakeMessageWithID setTimestamp(long timestamp) {
         return this;
      }

      @Override
      public byte getPriority() {
         return 0;
      }

      @Override
      public FakeMessageWithID setPriority(byte priority) {
         return this;
      }

      @Override
      public int getEncodeSize() {
         return 0;
      }

      @Override
      public boolean isLargeMessage() {
         return false;
      }

      @Override
      public ActiveMQBuffer getBodyBuffer() {
         return null;
      }

      @Override
      public Message putBooleanProperty(SimpleString key, boolean value) {
         return null;
      }

      @Override
      public Message putBooleanProperty(String key, boolean value) {
         return null;
      }

      @Override
      public Message putByteProperty(SimpleString key, byte value) {
         return null;
      }

      @Override
      public Message putByteProperty(String key, byte value) {
         return null;
      }

      @Override
      public Message putBytesProperty(SimpleString key, byte[] value) {
         return null;
      }

      @Override
      public Message putBytesProperty(String key, byte[] value) {
         return null;
      }

      @Override
      public Message putShortProperty(SimpleString key, short value) {
         return null;
      }

      @Override
      public Message putShortProperty(String key, short value) {
         return null;
      }

      @Override
      public Message putCharProperty(SimpleString key, char value) {
         return null;
      }

      @Override
      public Message putCharProperty(String key, char value) {
         return null;
      }

      @Override
      public Message putIntProperty(SimpleString key, int value) {
         return null;
      }

      @Override
      public Message putIntProperty(String key, int value) {
         return null;
      }

      @Override
      public Message putLongProperty(SimpleString key, long value) {
         return null;
      }

      @Override
      public Message putLongProperty(String key, long value) {
         return null;
      }

      @Override
      public Message putFloatProperty(SimpleString key, float value) {
         return null;
      }

      @Override
      public Message putFloatProperty(String key, float value) {
         return null;
      }

      @Override
      public Message putDoubleProperty(SimpleString key, double value) {
         return null;
      }

      @Override
      public Message putDoubleProperty(String key, double value) {
         return null;
      }

      @Override
      public Message putStringProperty(SimpleString key, SimpleString value) {
         return null;
      }

      @Override
      public Message putStringProperty(String key, String value) {
         return null;
      }

      @Override
      public Message putObjectProperty(SimpleString key, Object value) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Message putObjectProperty(String key, Object value) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Object removeProperty(SimpleString key) {
         return null;
      }

      @Override
      public Object removeProperty(String key) {
         return null;
      }

      @Override
      public boolean containsProperty(SimpleString key) {
         return false;
      }

      @Override
      public boolean containsProperty(String key) {
         return false;
      }

      @Override
      public Boolean getBooleanProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Byte getByteProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Byte getByteProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Double getDoubleProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Integer getIntProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Integer getIntProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Long getLongProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Long getLongProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Object getObjectProperty(SimpleString key) {
         return null;
      }

      @Override
      public Object getObjectProperty(String key) {
         return null;
      }

      @Override
      public Short getShortProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Short getShortProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Float getFloatProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public Float getFloatProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public String getStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public String getStringProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public SimpleString getSimpleStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
         return null;
      }

      @Override
      public byte[] getBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException {
         return new byte[0];
      }

      @Override
      public byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException {
         return new byte[0];
      }

      @Override
      public Set<SimpleString> getPropertyNames() {
         return null;
      }

      @Override
      public Map<String, Object> toMap() {
         return null;
      }

      @Override
      public Map<String, Object> toPropertyMap() {
         return null;
      }
   }
}
