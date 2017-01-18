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
package org.apache.activemq.artemis.tests.unit.core.message.impl;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class MessageImplTest extends ActiveMQTestBase {

   @Test
   public void getSetAttributes() {
      for (int j = 0; j < 10; j++) {
         byte[] bytes = new byte[1000];
         for (int i = 0; i < bytes.length; i++) {
            bytes[i] = RandomUtil.randomByte();
         }

         final byte type = RandomUtil.randomByte();
         final boolean durable = RandomUtil.randomBoolean();
         final long expiration = RandomUtil.randomLong();
         final long timestamp = RandomUtil.randomLong();
         final byte priority = RandomUtil.randomByte();
         Message message1 = new ClientMessageImpl(type, durable, expiration, timestamp, priority, 100);

         Message message = message1;

         Assert.assertEquals(type, message.getType());
         Assert.assertEquals(durable, message.isDurable());
         Assert.assertEquals(expiration, message.getExpiration());
         Assert.assertEquals(timestamp, message.getTimestamp());
         Assert.assertEquals(priority, message.getPriority());

         final SimpleString destination = new SimpleString(RandomUtil.randomString());
         final boolean durable2 = RandomUtil.randomBoolean();
         final long expiration2 = RandomUtil.randomLong();
         final long timestamp2 = RandomUtil.randomLong();
         final byte priority2 = RandomUtil.randomByte();

         message.setAddress(destination);
         Assert.assertEquals(destination, message.getAddress());

         message.setDurable(durable2);
         Assert.assertEquals(durable2, message.isDurable());

         message.setExpiration(expiration2);
         Assert.assertEquals(expiration2, message.getExpiration());

         message.setTimestamp(timestamp2);
         Assert.assertEquals(timestamp2, message.getTimestamp());

         message.setPriority(priority2);
         Assert.assertEquals(priority2, message.getPriority());

      }
   }

   @Test
   public void testExpired() {
      Message message = new ClientMessageImpl();

      Assert.assertEquals(0, message.getExpiration());
      Assert.assertFalse(message.isExpired());

      message.setExpiration(System.currentTimeMillis() + 1000);
      Assert.assertFalse(message.isExpired());

      message.setExpiration(System.currentTimeMillis() - 1);
      Assert.assertTrue(message.isExpired());

      message.setExpiration(System.currentTimeMillis() - 1000);
      Assert.assertTrue(message.isExpired());

      message.setExpiration(0);
      Assert.assertFalse(message.isExpired());
   }

   @Test
   public void testProperties() {
      for (int j = 0; j < 10; j++) {
         Message msg = new ClientMessageImpl();

         SimpleString prop1 = new SimpleString("prop1");
         boolean val1 = RandomUtil.randomBoolean();
         msg.putBooleanProperty(prop1, val1);

         SimpleString prop2 = new SimpleString("prop2");
         byte val2 = RandomUtil.randomByte();
         msg.putByteProperty(prop2, val2);

         SimpleString prop3 = new SimpleString("prop3");
         byte[] val3 = RandomUtil.randomBytes();
         msg.putBytesProperty(prop3, val3);

         SimpleString prop4 = new SimpleString("prop4");
         double val4 = RandomUtil.randomDouble();
         msg.putDoubleProperty(prop4, val4);

         SimpleString prop5 = new SimpleString("prop5");
         float val5 = RandomUtil.randomFloat();
         msg.putFloatProperty(prop5, val5);

         SimpleString prop6 = new SimpleString("prop6");
         int val6 = RandomUtil.randomInt();
         msg.putIntProperty(prop6, val6);

         SimpleString prop7 = new SimpleString("prop7");
         long val7 = RandomUtil.randomLong();
         msg.putLongProperty(prop7, val7);

         SimpleString prop8 = new SimpleString("prop8");
         short val8 = RandomUtil.randomShort();
         msg.putShortProperty(prop8, val8);

         SimpleString prop9 = new SimpleString("prop9");
         SimpleString val9 = new SimpleString(RandomUtil.randomString());
         msg.putStringProperty(prop9, val9);

         Assert.assertEquals(9, msg.getPropertyNames().size());
         Assert.assertTrue(msg.getPropertyNames().contains(prop1));
         Assert.assertTrue(msg.getPropertyNames().contains(prop2));
         Assert.assertTrue(msg.getPropertyNames().contains(prop3));
         Assert.assertTrue(msg.getPropertyNames().contains(prop4));
         Assert.assertTrue(msg.getPropertyNames().contains(prop5));
         Assert.assertTrue(msg.getPropertyNames().contains(prop6));
         Assert.assertTrue(msg.getPropertyNames().contains(prop7));
         Assert.assertTrue(msg.getPropertyNames().contains(prop8));
         Assert.assertTrue(msg.getPropertyNames().contains(prop9));

         Assert.assertTrue(msg.containsProperty(prop1));
         Assert.assertTrue(msg.containsProperty(prop2));
         Assert.assertTrue(msg.containsProperty(prop3));
         Assert.assertTrue(msg.containsProperty(prop4));
         Assert.assertTrue(msg.containsProperty(prop5));
         Assert.assertTrue(msg.containsProperty(prop6));
         Assert.assertTrue(msg.containsProperty(prop7));
         Assert.assertTrue(msg.containsProperty(prop8));
         Assert.assertTrue(msg.containsProperty(prop9));

         Assert.assertEquals(val1, msg.getObjectProperty(prop1));
         Assert.assertEquals(val2, msg.getObjectProperty(prop2));
         Assert.assertEquals(val3, msg.getObjectProperty(prop3));
         Assert.assertEquals(val4, msg.getObjectProperty(prop4));
         Assert.assertEquals(val5, msg.getObjectProperty(prop5));
         Assert.assertEquals(val6, msg.getObjectProperty(prop6));
         Assert.assertEquals(val7, msg.getObjectProperty(prop7));
         Assert.assertEquals(val8, msg.getObjectProperty(prop8));
         Assert.assertEquals(val9, msg.getObjectProperty(prop9));

         SimpleString val10 = new SimpleString(RandomUtil.randomString());
         // test overwrite
         msg.putStringProperty(prop9, val10);
         Assert.assertEquals(val10, msg.getObjectProperty(prop9));

         int val11 = RandomUtil.randomInt();
         msg.putIntProperty(prop9, val11);
         Assert.assertEquals(val11, msg.getObjectProperty(prop9));

         msg.removeProperty(prop1);
         Assert.assertEquals(8, msg.getPropertyNames().size());
         Assert.assertTrue(msg.getPropertyNames().contains(prop2));
         Assert.assertTrue(msg.getPropertyNames().contains(prop3));
         Assert.assertTrue(msg.getPropertyNames().contains(prop4));
         Assert.assertTrue(msg.getPropertyNames().contains(prop5));
         Assert.assertTrue(msg.getPropertyNames().contains(prop6));
         Assert.assertTrue(msg.getPropertyNames().contains(prop7));
         Assert.assertTrue(msg.getPropertyNames().contains(prop8));
         Assert.assertTrue(msg.getPropertyNames().contains(prop9));

         msg.removeProperty(prop2);
         Assert.assertEquals(7, msg.getPropertyNames().size());
         Assert.assertTrue(msg.getPropertyNames().contains(prop3));
         Assert.assertTrue(msg.getPropertyNames().contains(prop4));
         Assert.assertTrue(msg.getPropertyNames().contains(prop5));
         Assert.assertTrue(msg.getPropertyNames().contains(prop6));
         Assert.assertTrue(msg.getPropertyNames().contains(prop7));
         Assert.assertTrue(msg.getPropertyNames().contains(prop8));
         Assert.assertTrue(msg.getPropertyNames().contains(prop9));

         msg.removeProperty(prop9);
         Assert.assertEquals(6, msg.getPropertyNames().size());
         Assert.assertTrue(msg.getPropertyNames().contains(prop3));
         Assert.assertTrue(msg.getPropertyNames().contains(prop4));
         Assert.assertTrue(msg.getPropertyNames().contains(prop5));
         Assert.assertTrue(msg.getPropertyNames().contains(prop6));
         Assert.assertTrue(msg.getPropertyNames().contains(prop7));
         Assert.assertTrue(msg.getPropertyNames().contains(prop8));

         msg.removeProperty(prop3);
         msg.removeProperty(prop4);
         msg.removeProperty(prop5);
         msg.removeProperty(prop6);
         msg.removeProperty(prop7);
         msg.removeProperty(prop8);
         Assert.assertEquals(0, msg.getPropertyNames().size());
      }
   }

   @Test
   public void testMessageCopyIssue() throws Exception {
      for (long i = 0; i < 300; i++) {
         if (i % 10 == 0)
            System.out.println("#test " + i);
         internalMessageCopy();
      }
   }

   private void internalMessageCopy() throws Exception {
      final long RUNS = 2;
      final ServerMessageImpl msg = new ServerMessageImpl(123, 18);

      msg.setMessageID(RandomUtil.randomLong());
      msg.encodeMessageIDToBuffer();
      msg.setAddress(new SimpleString("Batatantkashf aksjfh aksfjh askfdjh askjfh "));

      final AtomicInteger errors = new AtomicInteger(0);

      int T1_number = 10;
      int T2_number = 10;

      final CountDownLatch latchAlign = new CountDownLatch(T1_number + T2_number);
      final CountDownLatch latchReady = new CountDownLatch(1);
      class T1 extends Thread {

         @Override
         public void run() {
            latchAlign.countDown();
            try {
               latchReady.await();
            } catch (Exception ignored) {
            }

            for (int i = 0; i < RUNS; i++) {
               try {
                  ServerMessageImpl newMsg = (ServerMessageImpl) msg.copy();
               } catch (Throwable e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }
         }
      }

      final String bigString;
      {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0; i < 500; i++) {
            buffer.append(" ");
         }
         bigString = buffer.toString();
      }

      class T2 extends Thread {

         @Override
         public void run() {
            latchAlign.countDown();
            try {
               latchReady.await();
            } catch (Exception ignored) {
            }

            for (int i = 0; i < RUNS; i++) {
               ActiveMQBuffer buf = null;
               try {
                  SessionSendMessage ssm = new SessionSendMessage(msg);
                  buf = ssm.encode(null);
                  simulateRead(buf);
               } catch (Throwable e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               } finally {
                  if ( buf != null ) {
                     buf.release();
                  }
               }
            }
         }
      }

      ArrayList<Thread> threads = new ArrayList<>();

      for (int i = 0; i < T1_number; i++) {
         T1 t = new T1();
         threads.add(t);
         t.start();
      }

      for (int i = 0; i < T2_number; i++) {
         T2 t2 = new T2();
         threads.add(t2);
         t2.start();
      }

      latchAlign.await();

      latchReady.countDown();

      for (Thread t : threads) {
         t.join();
      }

      Assert.assertEquals(0, errors.get());
   }

   private void simulateRead(ActiveMQBuffer buf) {
      buf.setIndex(buf.capacity() / 2, buf.capacity() / 2);

      // ok this is not actually happening during the read process, but changing this shouldn't affect the buffer on copy
      // this is to exaggerate the isolation on this test
      buf.writeBytes(new byte[1024]);
   }

   // Protected -------------------------------------------------------------------------------
   // Private ----------------------------------------------------------------------------------

}
