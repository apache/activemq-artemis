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
package org.apache.activemq.artemis.tests.integration.jms;

import javax.jms.DeliveryMode;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.Queue;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Random;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ClientSessionImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSContext;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JmsProducerTest extends JMSTestBase {

   private JMSProducer producer;
   private Random random;
   private JMSContext context;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      context = createContext();
      producer = context.createProducer();
      random = new Random();
   }

   @Override
   protected void testCaseCfExtraConfig(ConnectionFactoryConfiguration configuration) {
      configuration.setConfirmationWindowSize(0);
      configuration.setPreAcknowledge(false);
      configuration.setBlockOnDurableSend(false);
   }

   @Test
   public void testSetters() {
      long v = random.nextLong();
      producer.setDeliveryDelay(v);
      Assert.assertEquals(v, producer.getDeliveryDelay());

      long l = random.nextLong();
      producer.setTimeToLive(l);
      Assert.assertEquals(l, producer.getTimeToLive());

      String id = "ID: jms2-tests-correlation-id" + random.nextLong();
      producer.setJMSCorrelationID(id);
      Assert.assertEquals(id, producer.getJMSCorrelationID());

      //set a property of an invalid type (ArrayList)
      try {
         producer.setProperty("name1", new ArrayList<String>(2));
         fail("didn't get expected MessageFormatRuntimeException");
      } catch (MessageFormatRuntimeException e) {
         //expected.
      }
   }

   @Test
   public void testDisMsgID() {
      producer.setDisableMessageID(true);
      Assert.assertEquals(true, producer.getDisableMessageID());
      producer.setDisableMessageID(false);
      Assert.assertEquals(false, producer.getDisableMessageID());
   }

   @Test
   public void multipleSendsUsingSetters() throws Exception {
      server.createQueue(SimpleString.toSimpleString("q1"), SimpleString.toSimpleString("q1"), null, true, false);

      Queue q1 = context.createQueue("q1");

      context.createProducer().setProperty("prop1", 1).setProperty("prop2", 2).send(q1, "Text1");

      context.createProducer().setProperty("prop1", 3).setProperty("prop2", 4).send(q1, "Text2");

      for (int i = 0; i < 100; i++) {
         context.createProducer().send(q1, "Text" + i);
      }

      ActiveMQSession sessionUsed = (ActiveMQSession) (((ActiveMQJMSContext) context).getUsedSession());

      ClientSessionImpl coreSession = (ClientSessionImpl) sessionUsed.getCoreSession();

      // JMSConsumer is supposed to cache the producer, each call to createProducer is supposed to always return the same producer
      assertEquals(1, coreSession.cloneProducers().size());

      JMSConsumer consumer = context.createConsumer(q1);

      TextMessage text = (TextMessage) consumer.receive(5000);
      assertNotNull(text);
      assertEquals("Text1", text.getText());
      assertEquals(1, text.getIntProperty("prop1"));
      assertEquals(2, text.getIntProperty("prop2"));

      text = (TextMessage) consumer.receive(5000);
      assertNotNull(text);
      assertEquals("Text2", text.getText());
      assertEquals(3, text.getIntProperty("prop1"));
      assertEquals(4, text.getIntProperty("prop2"));

      for (int i = 0; i < 100; i++) {
         assertEquals("Text" + i, consumer.receiveBody(String.class, 1000));
      }

      consumer.close();
      context.close();
   }

   @Test
   public void testDeliveryMode() {
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);
      Assert.assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      Assert.assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
   }

   @Test
   public void testGetNonExistentProperties() throws Exception {
      Throwable expected = null;

      {
         //byte
         byte value0 = 0;
         try {
            value0 = Byte.valueOf(null);
         } catch (Throwable t) {
            expected = t;
         }

         try {
            byte value1 = producer.getByteProperty("testGetNonExistentProperties");

            if (expected == null) {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            } else {
               fail("non existent byte property expects exception, but got value: " + value1);
            }
         } catch (Throwable t) {
            if (expected == null)
               throw t;
            if (!t.getClass().equals(expected.getClass())) {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                                      " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //boolean
         expected = null;
         boolean value0 = false;
         try {
            value0 = Boolean.valueOf(null);
         } catch (Throwable t) {
            expected = t;
         }

         try {
            boolean value1 = producer.getBooleanProperty("testGetNonExistentProperties");

            if (expected == null) {
               assertEquals("value0: " + value0 + " value1: " + value1, value1, value0);
            } else {
               fail("non existent boolean property expects exception, but got value: " + value1);
            }
         } catch (Throwable t) {
            if (expected == null)
               throw t;
            if (!t.getClass().equals(expected.getClass())) {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                                      " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //double
         expected = null;
         double value0 = 0;
         try {
            value0 = Double.valueOf(null);
         } catch (Throwable t) {
            expected = t;
         }

         try {
            double value1 = producer.getDoubleProperty("testGetNonExistentProperties");

            if (expected == null) {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            } else {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         } catch (Throwable t) {
            if (expected == null)
               throw t;
            if (!t.getClass().equals(expected.getClass())) {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                                      " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //float
         expected = null;
         float value0 = 0;
         try {
            value0 = Float.valueOf(null);
         } catch (Throwable t) {
            expected = t;
         }

         try {
            float value1 = producer.getFloatProperty("testGetNonExistentProperties");

            if (expected == null) {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            } else {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         } catch (Throwable t) {
            if (expected == null)
               throw t;
            if (!t.getClass().equals(expected.getClass())) {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                                      " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //int
         expected = null;
         int value0 = 0;
         try {
            value0 = Integer.valueOf(null);
         } catch (Throwable t) {
            expected = t;
         }

         try {
            int value1 = producer.getIntProperty("testGetNonExistentProperties");

            if (expected == null) {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            } else {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         } catch (Throwable t) {
            if (expected == null)
               throw t;
            if (!t.getClass().equals(expected.getClass())) {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                                      " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //long
         expected = null;
         long value0 = 0;
         try {
            value0 = Integer.valueOf(null);
         } catch (Throwable t) {
            expected = t;
         }

         try {
            long value1 = producer.getLongProperty("testGetNonExistentProperties");

            if (expected == null) {
               assertEquals("value0: " + value0 + " value1: " + value1, value1, value0);
            } else {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         } catch (Throwable t) {
            if (expected == null)
               throw t;
            if (!t.getClass().equals(expected.getClass())) {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                                      " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //short
         expected = null;
         short value0 = 0;
         try {
            value0 = Short.valueOf(null);
         } catch (Throwable t) {
            expected = t;
         }

         try {
            short value1 = producer.getShortProperty("testGetNonExistentProperties");

            if (expected == null) {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            } else {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         } catch (Throwable t) {
            if (expected == null)
               throw t;
            if (!t.getClass().equals(expected.getClass())) {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                                      " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //Object
         Object value0 = producer.getObjectProperty("testGetNonExistentProperties");
         assertNull(value0);
         //String
         Object value1 = producer.getStringProperty("testGetNonExistentProperties");
         assertNull(value1);
      }

   }
}
