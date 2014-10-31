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
package org.hornetq.core.server.impl;


import java.io.InputStream;
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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQPropertyConversionException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.LinkedListIterator;
import org.hornetq.utils.ReferenceCounter;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class ScheduledDeliveryHandlerTest extends Assert
{

   @Test
   public void testScheduleRandom() throws Exception
   {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);


      long nextMessage = 0;
      long NUMBER_OF_SEQUENCES = 100000;

      for (int i = 0; i < NUMBER_OF_SEQUENCES; i++)
      {
         int numberOfMessages = RandomUtil.randomInt() % 10;
         if (numberOfMessages == 0) numberOfMessages = 1;

         long nextScheduledTime = RandomUtil.randomPositiveLong();

         for (int j = 0; j < numberOfMessages; j++)
         {
            boolean tail = RandomUtil.randomBoolean();

            addMessage(handler, nextMessage++, nextScheduledTime, tail);
         }
      }

      debugList(true, handler, nextMessage);

   }

   @Test
   public void testScheduleSameTimeHeadAndTail() throws Exception
   {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);

      long time = System.currentTimeMillis() + 10000;
      for (int i = 10001; i < 20000; i++)
      {
         addMessage(handler, i, time, true);
      }
      addMessage(handler, 10000, time, false);


      time = System.currentTimeMillis() + 5000;
      for (int i = 1; i < 10000; i++)
      {
         addMessage(handler, i, time, true);
      }
      addMessage(handler, 0, time, false);

      debugList(true, handler, 20000);

      validateSequence(handler);

   }

   @Test
   public void testScheduleFixedSample() throws Exception
   {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);


      addMessage(handler, 0, 48L, true);
      addMessage(handler, 1, 75L, true);
      addMessage(handler, 2, 56L, true);
      addMessage(handler, 3, 7L, false);
      addMessage(handler, 4, 69L, true);

      debugList(true, handler, 5);

   }

   @Test
   public void testScheduleWithAddHeads() throws Exception
   {
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
   public void testScheduleFixedSampleTailAndHead() throws Exception
   {
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
   public void testScheduleNow() throws Exception
   {

      ExecutorService executor = Executors.newFixedThreadPool(50);
      ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
      try
      {
         for (int i = 0; i < 100; i++)
         {
            // it's better to run the test a few times instead of run millions of messages here
            internalSchedule(executor, scheduler);
         }
      }
      finally
      {
         scheduler.shutdownNow();
         executor.shutdownNow();
      }
   }

   private void internalSchedule(ExecutorService executor, ScheduledThreadPoolExecutor scheduler) throws Exception
   {
      final int NUMBER_OF_MESSAGES = 200;
      int NUMBER_OF_THREADS = 20;
      final ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(scheduler);
      final FakeQueueForScheduleUnitTest fakeQueue = new FakeQueueForScheduleUnitTest(NUMBER_OF_MESSAGES * NUMBER_OF_THREADS);

      final long now = System.currentTimeMillis();

      final CountDownLatch latchDone = new CountDownLatch(NUMBER_OF_THREADS);

      final AtomicInteger error = new AtomicInteger(0);

      class ProducerThread implements Runnable
      {
         public void run()
         {
            try
            {
               for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
               {
                  checkAndSchedule(handler, i, now, false, fakeQueue);
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
               error.incrementAndGet();
            }
            finally
            {
               latchDone.countDown();
            }

         }
      }

      for (int i = 0; i < NUMBER_OF_THREADS; i++)
      {
         executor.execute(new ProducerThread());
      }

      assertTrue(latchDone.await(1, TimeUnit.MINUTES));

      assertEquals(0, error.get());

      if (!fakeQueue.waitCompletion(2, TimeUnit.SECONDS))
      {
         fail("Couldn't complete queue.add, expected " + NUMBER_OF_MESSAGES + ", still missing " + fakeQueue.expectedElements.toString());
      }
   }

   private void validateSequence(ScheduledDeliveryHandlerImpl handler)
   {
      long lastSequence = -1;
      for (MessageReference ref : handler.getScheduledReferences())
      {
         assertEquals(lastSequence + 1, ref.getMessage().getMessageID());
         lastSequence = ref.getMessage().getMessageID();
      }
   }

   private void addMessage(ScheduledDeliveryHandlerImpl handler, long nextMessageID, long nextScheduledTime, boolean tail)
   {
      MessageReferenceImpl refImpl = new MessageReferenceImpl(new FakeMessage(nextMessageID), null);
      refImpl.setScheduledDeliveryTime(nextScheduledTime);
      handler.addInPlace(nextScheduledTime, refImpl, tail);
   }

   private void checkAndSchedule(ScheduledDeliveryHandlerImpl handler, long nextMessageID, long nextScheduledTime, boolean tail, Queue queue)
   {
      MessageReferenceImpl refImpl = new MessageReferenceImpl(new FakeMessage(nextMessageID), queue);
      refImpl.setScheduledDeliveryTime(nextScheduledTime);
      handler.checkAndSchedule(refImpl, tail);
   }


   private void debugList(boolean fail, ScheduledDeliveryHandlerImpl handler, long numberOfExpectedMessages)
   {
      List<MessageReference> refs = handler.getScheduledReferences();

      HashSet<Long> messages = new HashSet<Long>();

      long lastTime = -1;

      for (MessageReference ref : refs)
      {
         assertFalse(messages.contains(ref.getMessage().getMessageID()));
         messages.add(ref.getMessage().getMessageID());

         if (fail)
         {
            assertTrue(ref.getScheduledDeliveryTime() >= lastTime);
         }
         else
         {
            if (ref.getScheduledDeliveryTime() < lastTime)
            {
               System.out.println("^^^fail at " + ref.getScheduledDeliveryTime());
            }
         }
         lastTime = ref.getScheduledDeliveryTime();
      }

      for (long i = 0; i < numberOfExpectedMessages; i++)
      {
         assertTrue(messages.contains(Long.valueOf(i)));
      }
   }

   class FakeMessage implements ServerMessage
   {

      final long id;

      public FakeMessage(final long id)
      {
         this.id = id;
      }

      @Override
      public void setMessageID(long id)
      {
      }

      @Override
      public long getMessageID()
      {
         return id;
      }

      @Override
      public MessageReference createReference(Queue queue)
      {
         return null;
      }

      @Override
      public void forceAddress(SimpleString address)
      {

      }

      @Override
      public int incrementRefCount() throws Exception
      {
         return 0;
      }

      @Override
      public int decrementRefCount() throws Exception
      {
         return 0;
      }

      @Override
      public int incrementDurableRefCount()
      {
         return 0;
      }

      @Override
      public int decrementDurableRefCount()
      {
         return 0;
      }

      @Override
      public ServerMessage copy(long newID)
      {
         return null;
      }

      @Override
      public void finishCopy() throws Exception
      {

      }

      @Override
      public ServerMessage copy()
      {
         return null;
      }

      @Override
      public int getMemoryEstimate()
      {
         return 0;
      }

      @Override
      public int getRefCount()
      {
         return 0;
      }

      @Override
      public ServerMessage makeCopyForExpiryOrDLA(long newID, MessageReference originalReference, boolean expiry, boolean copyOriginalHeaders) throws Exception
      {
         return null;
      }

      @Override
      public void setOriginalHeaders(ServerMessage other, MessageReference originalReference, boolean expiry)
      {

      }

      @Override
      public void setPagingStore(PagingStore store)
      {

      }

      @Override
      public PagingStore getPagingStore()
      {
         return null;
      }

      @Override
      public boolean hasInternalProperties()
      {
         return false;
      }

      @Override
      public boolean storeIsPaging()
      {
         return false;
      }

      @Override
      public void encodeMessageIDToBuffer()
      {

      }

      @Override
      public byte[] getDuplicateIDBytes()
      {
         return new byte[0];
      }

      @Override
      public Object getDuplicateProperty()
      {
         return null;
      }

      @Override
      public void encode(HornetQBuffer buffer)
      {

      }

      @Override
      public void decode(HornetQBuffer buffer)
      {

      }

      @Override
      public void decodeFromBuffer(HornetQBuffer buffer)
      {

      }

      @Override
      public int getEndOfMessagePosition()
      {
         return 0;
      }

      @Override
      public int getEndOfBodyPosition()
      {
         return 0;
      }

      @Override
      public void checkCopy()
      {

      }

      @Override
      public void bodyChanged()
      {

      }

      @Override
      public void resetCopied()
      {

      }

      @Override
      public boolean isServerMessage()
      {
         return false;
      }

      @Override
      public HornetQBuffer getEncodedBuffer()
      {
         return null;
      }

      @Override
      public int getHeadersAndPropertiesEncodeSize()
      {
         return 0;
      }

      @Override
      public HornetQBuffer getWholeBuffer()
      {
         return null;
      }

      @Override
      public void encodeHeadersAndProperties(HornetQBuffer buffer)
      {

      }

      @Override
      public void decodeHeadersAndProperties(HornetQBuffer buffer)
      {

      }

      @Override
      public BodyEncoder getBodyEncoder() throws HornetQException
      {
         return null;
      }

      @Override
      public InputStream getBodyInputStream()
      {
         return null;
      }

      @Override
      public void setAddressTransient(SimpleString address)
      {

      }

      @Override
      public TypedProperties getTypedProperties()
      {
         return null;
      }

      @Override
      public UUID getUserID()
      {
         return null;
      }

      @Override
      public void setUserID(UUID userID)
      {

      }

      @Override
      public SimpleString getAddress()
      {
         return null;
      }

      @Override
      public Message setAddress(SimpleString address)
      {
         return null;
      }

      @Override
      public byte getType()
      {
         return 0;
      }

      @Override
      public boolean isDurable()
      {
         return false;
      }

      @Override
      public void setDurable(boolean durable)
      {

      }

      @Override
      public long getExpiration()
      {
         return 0;
      }

      @Override
      public boolean isExpired()
      {
         return false;
      }

      @Override
      public void setExpiration(long expiration)
      {

      }

      @Override
      public long getTimestamp()
      {
         return 0;
      }

      @Override
      public void setTimestamp(long timestamp)
      {

      }

      @Override
      public byte getPriority()
      {
         return 0;
      }

      @Override
      public void setPriority(byte priority)
      {

      }

      @Override
      public int getEncodeSize()
      {
         return 0;
      }

      @Override
      public boolean isLargeMessage()
      {
         return false;
      }

      @Override
      public HornetQBuffer getBodyBuffer()
      {
         return null;
      }

      @Override
      public HornetQBuffer getBodyBufferCopy()
      {
         return null;
      }

      @Override
      public Message putBooleanProperty(SimpleString key, boolean value)
      {
         return null;
      }

      @Override
      public Message putBooleanProperty(String key, boolean value)
      {
         return null;
      }

      @Override
      public Message putByteProperty(SimpleString key, byte value)
      {
         return null;
      }

      @Override
      public Message putByteProperty(String key, byte value)
      {
         return null;
      }

      @Override
      public Message putBytesProperty(SimpleString key, byte[] value)
      {
         return null;
      }

      @Override
      public Message putBytesProperty(String key, byte[] value)
      {
         return null;
      }

      @Override
      public Message putShortProperty(SimpleString key, short value)
      {
         return null;
      }

      @Override
      public Message putShortProperty(String key, short value)
      {
         return null;
      }

      @Override
      public Message putCharProperty(SimpleString key, char value)
      {
         return null;
      }

      @Override
      public Message putCharProperty(String key, char value)
      {
         return null;
      }

      @Override
      public Message putIntProperty(SimpleString key, int value)
      {
         return null;
      }

      @Override
      public Message putIntProperty(String key, int value)
      {
         return null;
      }

      @Override
      public Message putLongProperty(SimpleString key, long value)
      {
         return null;
      }

      @Override
      public Message putLongProperty(String key, long value)
      {
         return null;
      }

      @Override
      public Message putFloatProperty(SimpleString key, float value)
      {
         return null;
      }

      @Override
      public Message putFloatProperty(String key, float value)
      {
         return null;
      }

      @Override
      public Message putDoubleProperty(SimpleString key, double value)
      {
         return null;
      }

      @Override
      public Message putDoubleProperty(String key, double value)
      {
         return null;
      }

      @Override
      public Message putStringProperty(SimpleString key, SimpleString value)
      {
         return null;
      }

      @Override
      public Message putStringProperty(String key, String value)
      {
         return null;
      }

      @Override
      public Message putObjectProperty(SimpleString key, Object value) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Message putObjectProperty(String key, Object value) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Object removeProperty(SimpleString key)
      {
         return null;
      }

      @Override
      public Object removeProperty(String key)
      {
         return null;
      }

      @Override
      public boolean containsProperty(SimpleString key)
      {
         return false;
      }

      @Override
      public boolean containsProperty(String key)
      {
         return false;
      }

      @Override
      public Boolean getBooleanProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Boolean getBooleanProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Byte getByteProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Byte getByteProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Double getDoubleProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Double getDoubleProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Integer getIntProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Integer getIntProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Long getLongProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Long getLongProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Object getObjectProperty(SimpleString key)
      {
         return null;
      }

      @Override
      public Object getObjectProperty(String key)
      {
         return null;
      }

      @Override
      public Short getShortProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Short getShortProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Float getFloatProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public Float getFloatProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public String getStringProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public String getStringProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public SimpleString getSimpleStringProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public SimpleString getSimpleStringProperty(String key) throws HornetQPropertyConversionException
      {
         return null;
      }

      @Override
      public byte[] getBytesProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return new byte[0];
      }

      @Override
      public byte[] getBytesProperty(String key) throws HornetQPropertyConversionException
      {
         return new byte[0];
      }

      @Override
      public Set<SimpleString> getPropertyNames()
      {
         return null;
      }

      @Override
      public Map<String, Object> toMap()
      {
         return null;
      }
   }


   public class FakeQueueForScheduleUnitTest implements Queue
   {

      @Override
      public void unproposed(SimpleString groupID)
      {

      }

      public FakeQueueForScheduleUnitTest(final int expectedElements)
      {
         this.expectedElements = new CountDownLatch(expectedElements);
      }


      public boolean waitCompletion(long timeout, TimeUnit timeUnit) throws Exception
      {
         return expectedElements.await(timeout, timeUnit);
      }

      final CountDownLatch expectedElements;
      LinkedList<MessageReference> messages = new LinkedList<>();

      @Override
      public SimpleString getName()
      {
         return null;
      }

      @Override
      public long getID()
      {
         return 0;
      }

      @Override
      public Filter getFilter()
      {
         return null;
      }

      @Override
      public PageSubscription getPageSubscription()
      {
         return null;
      }

      @Override
      public boolean isDurable()
      {
         return false;
      }

      @Override
      public boolean isTemporary()
      {
         return false;
      }

      @Override
      public void addConsumer(Consumer consumer) throws Exception
      {

      }

      @Override
      public void removeConsumer(Consumer consumer)
      {

      }

      @Override
      public int getConsumerCount()
      {
         return 0;
      }

      @Override
      public void setConsumersRefCount(HornetQServer server)
      {

      }

      @Override
      public ReferenceCounter getConsumersRefCount()
      {
         return null;
      }

      @Override
      public void reload(MessageReference ref)
      {

      }

      @Override
      public void addTail(MessageReference ref)
      {

      }

      @Override
      public void addTail(MessageReference ref, boolean direct)
      {

      }

      @Override
      public void addHead(MessageReference ref)
      {

      }

      @Override
      public void addHead(List<MessageReference> refs)
      {
         for (MessageReference ref : refs)
         {
            addFirst(ref);
         }
      }

      private void addFirst(MessageReference ref)
      {
         expectedElements.countDown();
         this.messages.addFirst(ref);
      }

      @Override
      public void acknowledge(MessageReference ref) throws Exception
      {

      }

      @Override
      public void acknowledge(Transaction tx, MessageReference ref) throws Exception
      {

      }

      @Override
      public void reacknowledge(Transaction tx, MessageReference ref) throws Exception
      {

      }

      @Override
      public void cancel(Transaction tx, MessageReference ref)
      {

      }

      @Override
      public void cancel(Transaction tx, MessageReference ref, boolean ignoreRedeliveryCheck)
      {

      }

      @Override
      public void cancel(MessageReference reference, long timeBase) throws Exception
      {

      }

      @Override
      public void deliverAsync()
      {

      }

      @Override
      public void forceDelivery()
      {

      }

      @Override
      public void deleteQueue() throws Exception
      {

      }

      @Override
      public void deleteQueue(boolean removeConsumers) throws Exception
      {

      }

      @Override
      public void destroyPaging() throws Exception
      {

      }

      @Override
      public long getMessageCount()
      {
         return 0;
      }

      @Override
      public long getMessageCount(long timeout)
      {
         return 0;
      }

      @Override
      public long getInstantMessageCount()
      {
         return 0;
      }

      @Override
      public int getDeliveringCount()
      {
         return 0;
      }

      @Override
      public void referenceHandled()
      {

      }

      @Override
      public int getScheduledCount()
      {
         return 0;
      }

      @Override
      public List<MessageReference> getScheduledMessages()
      {
         return null;
      }

      @Override
      public Map<String, List<MessageReference>> getDeliveringMessages()
      {
         return null;
      }

      @Override
      public long getMessagesAdded()
      {
         return 0;
      }

      @Override
      public long getMessagesAdded(long timeout)
      {
         return 0;
      }

      @Override
      public long getInstantMessagesAdded()
      {
         return 0;
      }

      @Override
      public MessageReference removeReferenceWithID(long id) throws Exception
      {
         return null;
      }

      @Override
      public MessageReference getReference(long id)
      {
         return null;
      }

      @Override
      public int deleteAllReferences() throws Exception
      {
         return 0;
      }

      @Override
      public int deleteAllReferences(int flushLimit) throws Exception
      {
         return 0;
      }

      @Override
      public boolean deleteReference(long messageID) throws Exception
      {
         return false;
      }

      @Override
      public int deleteMatchingReferences(Filter filter) throws Exception
      {
         return 0;
      }

      @Override
      public int deleteMatchingReferences(int flushLImit, Filter filter) throws Exception
      {
         return 0;
      }

      @Override
      public boolean expireReference(long messageID) throws Exception
      {
         return false;
      }

      @Override
      public int expireReferences(Filter filter) throws Exception
      {
         return 0;
      }

      @Override
      public void expireReferences() throws Exception
      {

      }

      @Override
      public void expire(MessageReference ref) throws Exception
      {

      }

      @Override
      public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception
      {
         return false;
      }

      @Override
      public int sendMessagesToDeadLetterAddress(Filter filter) throws Exception
      {
         return 0;
      }

      @Override
      public boolean changeReferencePriority(long messageID, byte newPriority) throws Exception
      {
         return false;
      }

      @Override
      public int changeReferencesPriority(Filter filter, byte newPriority) throws Exception
      {
         return 0;
      }

      @Override
      public boolean moveReference(long messageID, SimpleString toAddress) throws Exception
      {
         return false;
      }

      @Override
      public boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception
      {
         return false;
      }

      @Override
      public int moveReferences(Filter filter, SimpleString toAddress) throws Exception
      {
         return 0;
      }

      @Override
      public int moveReferences(int flushLimit, Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception
      {
         return 0;
      }

      @Override
      public void addRedistributor(long delay)
      {

      }

      @Override
      public void cancelRedistributor() throws Exception
      {

      }

      @Override
      public boolean hasMatchingConsumer(ServerMessage message)
      {
         return false;
      }

      @Override
      public Collection<Consumer> getConsumers()
      {
         return null;
      }

      @Override
      public boolean checkRedelivery(MessageReference ref, long timeBase, boolean ignoreRedeliveryDelay) throws Exception
      {
         return false;
      }

      @Override
      public LinkedListIterator<MessageReference> iterator()
      {
         return null;
      }

      @Override
      public LinkedListIterator<MessageReference> totalIterator()
      {
         return null;
      }

      @Override
      public SimpleString getExpiryAddress()
      {
         return null;
      }

      @Override
      public void pause()
      {

      }

      @Override
      public void resume()
      {

      }

      @Override
      public boolean isPaused()
      {
         return false;
      }

      @Override
      public Executor getExecutor()
      {
         return null;
      }

      @Override
      public void resetAllIterators()
      {

      }

      @Override
      public boolean flushExecutor()
      {
         return false;
      }

      @Override
      public void close() throws Exception
      {

      }

      @Override
      public boolean isDirectDeliver()
      {
         return false;
      }

      @Override
      public SimpleString getAddress()
      {
         return null;
      }

      @Override
      public boolean isInternalQueue()
      {
         return false;
      }

      @Override
      public void setInternalQueue(boolean internalQueue)
      {

      }

      @Override
      public void resetMessagesAdded()
      {

      }

      @Override
      public void incrementMesssagesAdded()
      {

      }

      @Override
      public List<MessageReference> cancelScheduledMessages()
      {
         return null;
      }

      @Override
      public void route(ServerMessage message, RoutingContext context) throws Exception
      {

      }

      @Override
      public void routeWithAck(ServerMessage message, RoutingContext context)
      {

      }
   }
}
