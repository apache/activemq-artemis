/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.Assert;
import org.junit.Test;

public class CoreMTMessageTest {

   public static final SimpleString ADDRESS = new SimpleString("this.local.address");
   public static final SimpleString ADDRESS2 = new SimpleString("some.other.address");
   public static final byte MESSAGE_TYPE = Message.TEXT_TYPE;
   public static final boolean DURABLE = true;
   public static final long EXPIRATION = 123L;
   public static final long TIMESTAMP = 321L;
   public static final byte PRIORITY = (byte) 3;

   @Test
   public void testDecodeEncodeMultiThread() throws Exception {

      for (int i = 0; i < 100; i++) {
         internalTest();
      }
   }

   public void internalTest() throws Exception {

      CoreMessageObjectPools objectPools = new CoreMessageObjectPools();
      SimpleString propValue = UUIDGenerator.getInstance().generateSimpleStringUUID();

      UUID userID = UUIDGenerator.getInstance().generateUUID();
      String body = UUIDGenerator.getInstance().generateStringUUID();
      ClientMessageImpl message = new ClientMessageImpl(MESSAGE_TYPE, DURABLE, EXPIRATION, TIMESTAMP, PRIORITY, 10 * 1024, objectPools);
      TextMessageUtil.writeBodyText(message.getBodyBuffer(), SimpleString.toSimpleString(body));

      message.setAddress(ADDRESS);
      message.setUserID(userID);
      message.getProperties().putSimpleStringProperty(SimpleString.toSimpleString("str-prop"), propValue);

      ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(10 * 1024);
      message.sendBuffer(buffer.byteBuf(), 0);


      CoreMessage coreMessage = new CoreMessage(objectPools);
      coreMessage.receiveBuffer(buffer.byteBuf());
      coreMessage.setAddress(ADDRESS2.toString());
      coreMessage.setMessageID(33);


      Thread[] threads = new Thread[50];
      final CountDownLatch aligned = new CountDownLatch(threads.length);
      final CountDownLatch startFlag = new CountDownLatch(1);
      final AtomicInteger errors = new AtomicInteger(0);

      Runnable runnable = new Runnable() {
         @Override
         public void run() {
            try {
               ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(10 * 1024);
               aligned.countDown();
               Assert.assertTrue(startFlag.await(5, TimeUnit.SECONDS));
               coreMessage.messageChanged();
               coreMessage.sendBuffer(buffer.byteBuf(), 0);
               CoreMessage recMessage = new CoreMessage();
               recMessage.receiveBuffer(buffer.byteBuf());
               Assert.assertEquals(ADDRESS2, recMessage.getAddressSimpleString());
               Assert.assertEquals(33, recMessage.getMessageID());
               Assert.assertEquals(propValue, recMessage.getSimpleStringProperty(SimpleString.toSimpleString("str-prop")));
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         }
      };


      for (int i = 0; i < threads.length; i++) {
         threads[i] = new Thread(runnable);
         threads[i].start();
      }

      aligned.await(10, TimeUnit.SECONDS);
      coreMessage.messageChanged();
      startFlag.countDown();

      for (Thread t : threads) {
         t.join(10000);
         Assert.assertFalse(t.isAlive());
      }

      Assert.assertEquals(0, errors.get());

   }

}
