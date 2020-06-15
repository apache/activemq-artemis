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
package org.apache.activemq.artemis.jms.tests.message;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class BodyIsAssignableFromTest extends MessageBodyTestCase {

   @Test
   public void testText() throws JMSException {
      bodyAssignableFrom(JmsMessageType.TEXT, String.class, CharSequence.class, Comparable.class, Serializable.class);
      bodyNotAssignableFrom(JmsMessageType.TEXT, List.class, StringBuilder.class, Map.class, File.class);
   }

   @Test
   public void testMap() throws JMSException {
      bodyAssignableFrom(JmsMessageType.MAP, Map.class, Object.class);
      bodyNotAssignableFrom(JmsMessageType.MAP, String.class, CharSequence.class, Comparable.class, Serializable.class);
   }

   @Test
   public void testStream() throws JMSException {
      bodyNotAssignableFrom(JmsMessageType.STREAM, Object.class, Serializable.class);
   }

   @Test
   public void testByte() throws JMSException {
      bodyAssignableFrom(JmsMessageType.BYTE, Object.class, byte[].class);
      bodyNotAssignableFrom(JmsMessageType.BYTE, String.class, CharSequence.class, Comparable.class);
   }

   @Test
   public void testObject() throws JMSException {
      bodyAssignableFrom(JmsMessageType.OBJECT, Object.class, Serializable.class, Comparable.class, Double.class);
      // we are sending a Double in the body, so the de-serialized Object will be an instanceof these:
      bodyAssignableFrom(JmsMessageType.OBJECT, Comparable.class, Double.class);
      bodyNotAssignableFrom(JmsMessageType.OBJECT, String.class, CharSequence.class, List.class);
   }

   private void bodyAssignableFrom(JmsMessageType type, Class... clazz) throws JMSException {
      bodyAssignableFrom(type, true, clazz);
   }

   /**
    * @param type
    * @param clazz
    * @param bool
    * @throws JMSException
    */
   private void bodyAssignableFrom(final JmsMessageType type, final boolean bool, Class... clazz) throws JMSException {
      Assert.assertNotNull("clazz!=null", clazz);
      Assert.assertTrue("clazz[] not empty", clazz.length > 0);
      Object body = createBodySendAndReceive(type);
      Message msg = queueConsumer.receive(500);
      Assert.assertNotNull("must have a msg", msg);
      Assert.assertEquals(type.toString(), msg.getStringProperty("type"));
      for (Class<?> c : clazz) {
         Assert.assertEquals(msg + " " + type + " & " + c + ": " + bool, bool, msg.isBodyAssignableTo(c));
         if (bool) {
            Object receivedBody = msg.getBody(c);
            Assert.assertTrue("correct type " + c, c.isInstance(receivedBody));
            if (body.getClass().isAssignableFrom(byte[].class)) {
               Assert.assertArrayEquals(byte[].class.cast(body), (byte[]) receivedBody);
            } else {
               Assert.assertEquals("clazz=" + c + ", bodies must match.. " + body.equals(receivedBody), body, receivedBody);
            }
         } else {
            try {
               Object foo = msg.getBody(c);
               Assert.assertNull("body should be null", foo);
            } catch (MessageFormatException e) {
               // expected
            }
         }
      }
   }

   /**
    * @param type
    * @throws JMSException
    */
   private Object createBodySendAndReceive(JmsMessageType type) throws JMSException {
      Object res = null;
      Message msg = null;
      switch (type) {
         case BYTE:
            BytesMessage mByte = queueProducerSession.createBytesMessage();
            final int size = 20;
            byte[] resByte = new byte[size];
            for (int i = 0; i < size; i++) {
               resByte[i] = (byte) i;
               mByte.writeByte((byte) i);
            }
            msg = mByte;
            res = resByte;
            break;
         case TEXT:
            res = "JMS2";
            msg = queueProducerSession.createTextMessage("JMS2");
            break;
         case STREAM:
            msg = queueProducerSession.createStreamMessage();
            break;
         case OBJECT:
            res = new Double(37.6);
            msg = queueProducerSession.createObjectMessage(new Double(37.6));
            break;
         case MAP:
            MapMessage msg1 = queueProducerSession.createMapMessage();
            msg1.setInt("int", 13);
            msg1.setLong("long", 37L);
            msg1.setString("string", "crocodile");
            msg = msg1;
            Map<String, Object> map = new HashMap<>();
            map.put("int", Integer.valueOf(13));
            map.put("long", Long.valueOf(37L));
            map.put("string", "crocodile");
            res = map;
            break;
         default:
            Assert.fail("no default...");
      }
      Assert.assertNotNull(msg);
      msg.setStringProperty("type", type.toString());
      queueProducer.send(msg);
      return res;
   }

   private void bodyNotAssignableFrom(JmsMessageType type, Class... clazz) throws JMSException {
      bodyAssignableFrom(type, false, clazz);
   }
}
