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
package org.apache.activemq.artemis.core.server.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RefCountMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.encode.BodyType;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.LinkedListIterator;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ReferenceCounter;
import org.apache.activemq.artemis.utils.UUID;
import org.junit.Assert;
import org.junit.Test;

public class ScheduledDeliveryHandlerTest extends Assert {

   @Test
   public void testScheduleRandom() throws Exception {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);

      long nextMessage = 0;
      long NUMBER_OF_SEQUENCES = 100000;

      for (int i = 0; i < NUMBER_OF_SEQUENCES; i++) {
         int numberOfMessages = RandomUtil.randomInt() % 10;
         if (numberOfMessages == 0)
            numberOfMessages = 1;

         long nextScheduledTime = RandomUtil.randomPositiveLong();

         for (int j = 0; j < numberOfMessages; j++) {
            boolean tail = RandomUtil.randomBoolean();

            addMessage(handler, nextMessage++, nextScheduledTime, tail);
         }
      }

      debugList(true, handler, nextMessage);

   }

   @Test
   public void testScheduleSameTimeHeadAndTail() throws Exception {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);

      long time = System.currentTimeMillis() + 10000;
      for (int i = 10001; i < 20000; i++) {
         addMessage(handler, i, time, true);
      }
      addMessage(handler, 10000, time, false);

      time = System.currentTimeMillis() + 5000;
      for (int i = 1; i < 10000; i++) {
         addMessage(handler, i, time, true);
      }
      addMessage(handler, 0, time, false);

      debugList(true, handler, 20000);

      validateSequence(handler);

   }

   @Test
   public void testScheduleFixedSample() throws Exception {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);

      addMessage(handler, 0, 48L, true);
      addMessage(handler, 1, 75L, true);
      addMessage(handler, 2, 56L, true);
      addMessage(handler, 3, 7L, false);
      addMessage(handler, 4, 69L, true);

      debugList(true, handler, 5);

   }

   @Test
   public void testScheduleWithAddHeads() throws Exception {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);

      addMessage(handler, 0, 1, true);
      addMessage(handler, 1, 2, true);
      addMessage(handler, 2, 3, true);
      addMessage(handler, 3, 3, true);
      addMessage(handler, 4, 4, true);

      addMessage(handler, 10, 5, false);
      addMessage(handler, 9, 5, false);
      addMessage(handler, 8, 5, false);
      addMessage(handler, 7, 5, false);
      addMessage(handler, 6, 5, false);
      addMessage(handler, 5, 5, false);

      validateSequence(handler);

   }

   @Test
   public void testScheduleFixedSampleTailAndHead() throws Exception {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);

      // mix a sequence of tails / heads, but at the end this was supposed to be all sequential
      addMessage(handler, 1, 48L, true);
      addMessage(handler, 2, 48L, true);
      addMessage(handler, 3, 48L, true);
      addMessage(handler, 4, 48L, true);
      addMessage(handler, 5, 48L, true);
      addMessage(handler, 0, 48L, false);

      addMessage(handler, 13, 59L, true);
      addMessage(handler, 14, 59L, true);
      addMessage(handler, 15, 59L, true);
      addMessage(handler, 16, 59L, true);
      addMessage(handler, 17, 59L, true);
      addMessage(handler, 12, 59L, false);

      addMessage(handler, 7, 49L, true);
      addMessage(handler, 8, 49L, true);
      addMessage(handler, 9, 49L, true);
      addMessage(handler, 10, 49L, true);
      addMessage(handler, 11, 49L, true);
      addMessage(handler, 6, 49L, false);

      validateSequence(handler);
   }

   @Test
   public void testScheduleNow() throws Exception {

      ExecutorService executor = Executors.newFixedThreadPool(50, ActiveMQThreadFactory.defaultThreadFactory());
      ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, ActiveMQThreadFactory.defaultThreadFactory());
      try {
         for (int i = 0; i < 100; i++) {
            // it's better to run the test a few times instead of run millions of messages here
            internalSchedule(executor, scheduler);
         }
      } finally {
         scheduler.shutdownNow();
         executor.shutdownNow();
      }
   }

   private void internalSchedule(ExecutorService executor, ScheduledThreadPoolExecutor scheduler) throws Exception {
      final int NUMBER_OF_MESSAGES = 200;
      int NUMBER_OF_THREADS = 20;
      final ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(scheduler);
      final FakeQueueForScheduleUnitTest fakeQueue = new FakeQueueForScheduleUnitTest(NUMBER_OF_MESSAGES * NUMBER_OF_THREADS);

      final long now = System.currentTimeMillis();

      final CountDownLatch latchDone = new CountDownLatch(NUMBER_OF_THREADS);

      final AtomicInteger error = new AtomicInteger(0);

      class ProducerThread implements Runnable {

         @Override
         public void run() {
            try {
               for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
                  checkAndSchedule(handler, i, now, false, fakeQueue);
               }
            } catch (Exception e) {
               e.printStackTrace();
               error.incrementAndGet();
            } finally {
               latchDone.countDown();
            }

         }
      }

      for (int i = 0; i < NUMBER_OF_THREADS; i++) {
         executor.execute(new ProducerThread());
      }

      assertTrue(latchDone.await(1, TimeUnit.MINUTES));

      assertEquals(0, error.get());

      if (!fakeQueue.waitCompletion(2, TimeUnit.SECONDS)) {
         fail("Couldn't complete queue.add, expected " + NUMBER_OF_MESSAGES + ", still missing " + fakeQueue.expectedElements.toString());
      }
   }

   private void validateSequence(ScheduledDeliveryHandlerImpl handler) throws Exception {
      long lastSequence = -1;
      for (MessageReference ref : handler.getScheduledReferences()) {
         assertEquals(lastSequence + 1, ref.getMessage().getMessageID());
         lastSequence = ref.getMessage().getMessageID();
      }
   }

   private void addMessage(ScheduledDeliveryHandlerImpl handler,
                           long nextMessageID,
                           long nextScheduledTime,
                           boolean tail) {
      MessageReferenceImpl refImpl = new MessageReferenceImpl(new FakeMessage(nextMessageID), null);
      refImpl.setScheduledDeliveryTime(nextScheduledTime);
      handler.addInPlace(nextScheduledTime, refImpl, tail);
   }

   private void checkAndSchedule(ScheduledDeliveryHandlerImpl handler,
                                 long nextMessageID,
                                 long nextScheduledTime,
                                 boolean tail,
                                 Queue queue) {
      MessageReferenceImpl refImpl = new MessageReferenceImpl(new FakeMessage(nextMessageID), queue);
      refImpl.setScheduledDeliveryTime(nextScheduledTime);
      handler.checkAndSchedule(refImpl, tail);
   }

   private void debugList(boolean fail,
                          ScheduledDeliveryHandlerImpl handler,
                          long numberOfExpectedMessages) throws Exception {
      List<MessageReference> refs = handler.getScheduledReferences();

      HashSet<Long> messages = new HashSet<>();

      long lastTime = -1;

      for (MessageReference ref : refs) {
         assertFalse(messages.contains(ref.getMessage().getMessageID()));
         messages.add(ref.getMessage().getMessageID());

         if (fail) {
            assertTrue(ref.getScheduledDeliveryTime() >= lastTime);
         } else {
            if (ref.getScheduledDeliveryTime() < lastTime) {
               System.out.println("^^^fail at " + ref.getScheduledDeliveryTime());
            }
         }
         lastTime = ref.getScheduledDeliveryTime();
      }

      for (long i = 0; i < numberOfExpectedMessages; i++) {
         assertTrue(messages.contains(Long.valueOf(i)));
      }
   }

   class FakeMessage extends RefCountMessage {

      @Override
      public void persist(ActiveMQBuffer targetRecord) {

      }

      @Override
      public Long getScheduledDeliveryTime() {
         return null;
      }

      @Override
      public void reloadPersistence(ActiveMQBuffer record) {

      }

      @Override
      public Persister<Message> getPersister() {
         return null;
      }

      @Override
      public int getPersistSize() {
         return 0;
      }
      final long id;

      @Override
      public Message toCore() {
         return this;
      }

      @Override
      public ActiveMQBuffer getReadOnlyBodyBuffer() {
         return null;
      }

      FakeMessage(final long id) {
         this.id = id;
      }

      @Override
      public FakeMessage setMessageID(long id) {
         return this;
      }

      @Override
      public long getMessageID() {
         return id;
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
      public Message copy(long newID) {
         return null;
      }

      @Override
      public Message copy() {
         return null;
      }

      @Override
      public int getMemoryEstimate() {
         return 0;
      }

      @Override
      public int getRefCount() {
         return 0;
      }

      @Override
      public byte[] getDuplicateIDBytes() {
         return new byte[0];
      }

      @Override
      public Object getDuplicateProperty() {
         return null;
      }

      @Override
      public void messageChanged() {

      }
      @Override
      public LargeBodyEncoder getBodyEncoder() throws ActiveMQException {
         return null;
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
      public Message setProtocol(Object protocol) {
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
      public FakeMessage setDurable(boolean durable) {
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
      public FakeMessage setExpiration(long expiration) {
         return this;
      }

      @Override
      public long getTimestamp() {
         return 0;
      }

      @Override
      public FakeMessage setTimestamp(long timestamp) {
         return this;
      }

      @Override
      public byte getPriority() {
         return 0;
      }

      @Override
      public FakeMessage setPriority(byte priority) {
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

      @Override
      public Message setUserID(Object userID) {
         return null;
      }

      @Override
      public void copyHeadersAndProperties(Message msg) {

      }

      @Override
      public Message setType(byte type) {
         return null;
      }

      @Override
      public void receiveBuffer(ByteBuf buffer) {

      }

      @Override
      public void sendBuffer(ByteBuf buffer, int count) {

      }
   }

   public class FakeQueueForScheduleUnitTest implements Queue {

      @Override
      public void setPurgeOnNoConsumers(boolean value) {

      }

      @Override
      public void setMaxConsumer(int maxConsumers) {

      }

      @Override
      public void unproposed(SimpleString groupID) {

      }

      public FakeQueueForScheduleUnitTest(final int expectedElements) {
         this.expectedElements = new CountDownLatch(expectedElements);
      }

      @Override
      public boolean isPersistedPause() {
         return false;
      }

      public boolean waitCompletion(long timeout, TimeUnit timeUnit) throws Exception {
         return expectedElements.await(timeout, timeUnit);
      }

      final CountDownLatch expectedElements;
      LinkedList<MessageReference> messages = new LinkedList<>();

      @Override
      public SimpleString getName() {
         return null;
      }

      @Override
      public long getID() {
         return 0;
      }

      @Override
      public void pause(boolean persist) {
      }

      @Override
      public void reloadPause(long recordID) {
      }

      @Override
      public Filter getFilter() {
         return null;
      }

      @Override
      public PageSubscription getPageSubscription() {
         return null;
      }

      @Override
      public RoutingType getRoutingType() {
         return null;
      }

      @Override
      public void setRoutingType(RoutingType routingType) {

      }

      @Override
      public boolean isDurable() {
         return false;
      }

      @Override
      public boolean isTemporary() {
         return false;
      }

      @Override
      public boolean isAutoCreated() {
         return false;
      }

      @Override
      public boolean isPurgeOnNoConsumers() {
         return false;
      }

      @Override
      public int getMaxConsumers() {
         return -1;
      }

      @Override
      public void addConsumer(Consumer consumer) throws Exception {

      }

      @Override
      public void removeConsumer(Consumer consumer) {

      }

      @Override
      public int retryMessages(Filter filter) throws Exception {
         return 0;
      }

      @Override
      public int getConsumerCount() {
         return 0;
      }

      @Override
      public void setConsumersRefCount(ReferenceCounter referenceCounter) {

      }

      @Override
      public ReferenceCounter getConsumersRefCount() {
         return null;
      }

      @Override
      public void reload(MessageReference ref) {

      }

      @Override
      public void addTail(MessageReference ref) {

      }

      @Override
      public void addTail(MessageReference ref, boolean direct) {

      }

      @Override
      public void addHead(MessageReference ref, boolean scheduling) {

      }

      @Override
      public void addHead(List<MessageReference> refs, boolean scheduling) {
         for (MessageReference ref : refs) {
            addFirst(ref);
         }
      }

      private void addFirst(MessageReference ref) {
         expectedElements.countDown();
         this.messages.addFirst(ref);
      }

      @Override
      public void acknowledge(MessageReference ref) throws Exception {

      }

      @Override
      public void acknowledge(MessageReference ref, AckReason reason) throws Exception {

      }

      @Override
      public void acknowledge(Transaction tx, MessageReference ref) throws Exception {

      }

      @Override
      public void acknowledge(Transaction tx, MessageReference ref, AckReason reason) throws Exception {

      }

      @Override
      public void reacknowledge(Transaction tx, MessageReference ref) throws Exception {

      }

      @Override
      public void cancel(Transaction tx, MessageReference ref) {

      }

      @Override
      public void cancel(Transaction tx, MessageReference ref, boolean ignoreRedeliveryCheck) {

      }

      @Override
      public void cancel(MessageReference reference, long timeBase) throws Exception {

      }

      @Override
      public void deliverAsync() {

      }

      @Override
      public void forceDelivery() {

      }

      @Override
      public void deleteQueue() throws Exception {

      }

      @Override
      public void deleteQueue(boolean removeConsumers) throws Exception {

      }

      @Override
      public void destroyPaging() throws Exception {

      }

      @Override
      public long getMessageCount() {
         return 0;
      }

      @Override
      public int getDeliveringCount() {
         return 0;
      }

      @Override
      public void referenceHandled() {

      }

      @Override
      public int getScheduledCount() {
         return 0;
      }

      @Override
      public List<MessageReference> getScheduledMessages() {
         return null;
      }

      @Override
      public Map<String, List<MessageReference>> getDeliveringMessages() {
         return null;
      }

      @Override
      public long getMessagesAdded() {
         return 0;
      }

      @Override
      public long getMessagesAcknowledged() {
         return 0;
      }

      @Override
      public long getMessagesExpired() {
         return 0;
      }

      @Override
      public long getMessagesKilled() {
         return 0;
      }

      @Override
      public MessageReference removeReferenceWithID(long id) throws Exception {
         return null;
      }

      @Override
      public MessageReference getReference(long id) {
         return null;
      }

      @Override
      public int deleteAllReferences() throws Exception {
         return 0;
      }

      @Override
      public int deleteAllReferences(int flushLimit) throws Exception {
         return 0;
      }

      @Override
      public boolean deleteReference(long messageID) throws Exception {
         return false;
      }

      @Override
      public int deleteMatchingReferences(Filter filter) throws Exception {
         return 0;
      }

      @Override
      public int deleteMatchingReferences(int flushLImit, Filter filter) throws Exception {
         return 0;
      }

      @Override
      public boolean expireReference(long messageID) throws Exception {
         return false;
      }

      @Override
      public int expireReferences(Filter filter) throws Exception {
         return 0;
      }

      @Override
      public void expireReferences() throws Exception {

      }

      @Override
      public void expire(MessageReference ref) throws Exception {

      }

      @Override
      public void sendToDeadLetterAddress(Transaction tx, MessageReference ref) throws Exception {
      }

      @Override
      public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception {
         return false;
      }

      @Override
      public int sendMessagesToDeadLetterAddress(Filter filter) throws Exception {
         return 0;
      }

      @Override
      public boolean changeReferencePriority(long messageID, byte newPriority) throws Exception {
         return false;
      }

      @Override
      public int changeReferencesPriority(Filter filter, byte newPriority) throws Exception {
         return 0;
      }

      @Override
      public boolean moveReference(long messageID, SimpleString toAddress) throws Exception {
         return false;
      }

      @Override
      public boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception {
         return false;
      }

      @Override
      public int moveReferences(Filter filter, SimpleString toAddress) throws Exception {
         return 0;
      }

      @Override
      public int moveReferences(int flushLimit,
                                Filter filter,
                                SimpleString toAddress,
                                boolean rejectDuplicates) throws Exception {
         return 0;
      }

      @Override
      public void addRedistributor(long delay) {

      }

      @Override
      public void cancelRedistributor() throws Exception {

      }

      @Override
      public boolean hasMatchingConsumer(Message message) {
         return false;
      }

      @Override
      public Collection<Consumer> getConsumers() {
         return null;
      }

      @Override
      public boolean checkRedelivery(MessageReference ref,
                                     long timeBase,
                                     boolean ignoreRedeliveryDelay) throws Exception {
         return false;
      }

      @Override
      public LinkedListIterator<MessageReference> iterator() {
         return null;
      }

      @Override
      public LinkedListIterator<MessageReference> browserIterator() {
         return null;
      }

      @Override
      public SimpleString getExpiryAddress() {
         return null;
      }

      @Override
      public void pause() {

      }

      @Override
      public void resume() {

      }

      @Override
      public boolean isPaused() {
         return false;
      }

      @Override
      public Executor getExecutor() {
         return null;
      }

      @Override
      public void resetAllIterators() {

      }

      @Override
      public boolean flushExecutor() {
         return false;
      }

      @Override
      public void close() throws Exception {

      }

      @Override
      public boolean isDirectDeliver() {
         return false;
      }

      @Override
      public SimpleString getAddress() {
         return null;
      }

      @Override
      public boolean isInternalQueue() {
         return false;
      }

      @Override
      public void setInternalQueue(boolean internalQueue) {

      }

      @Override
      public void resetMessagesAdded() {

      }

      @Override
      public void resetMessagesAcknowledged() {

      }

      @Override
      public void resetMessagesExpired() {

      }

      @Override
      public void resetMessagesKilled() {

      }

      @Override
      public void incrementMesssagesAdded() {

      }

      @Override
      public void deliverScheduledMessages() {

      }

      @Override
      public void route(Message message, RoutingContext context) throws Exception {

      }

      @Override
      public void routeWithAck(Message message, RoutingContext context) {

      }

      @Override
      public void postAcknowledge(MessageReference ref) {

      }

      @Override
      public float getRate() {
         return 0.0f;
      }

      @Override
      public SimpleString getUser() {
         return null;
      }

      @Override
      public void decDelivering(int size) {

      }




   }
}
