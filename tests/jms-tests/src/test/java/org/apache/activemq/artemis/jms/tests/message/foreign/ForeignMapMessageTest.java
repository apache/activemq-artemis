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
package org.apache.activemq.artemis.jms.tests.message.foreign;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

import org.apache.activemq.artemis.jms.tests.message.SimpleJMSMapMessage;
import org.apache.activemq.artemis.jms.tests.util.ProxyAssertSupport;

/**
 * Tests the delivery/receipt of a foreign map message
 */
public class ForeignMapMessageTest extends ForeignMessageTest {

   private final String obj = new String("stringobject");

   @Override
   protected Message createForeignMessage() throws Exception {
      SimpleJMSMapMessage m = new SimpleJMSMapMessage();

      log.debug("creating JMS Message type " + m.getClass().getName());

      m.setBoolean("boolean1", true);
      m.setChar("char1", 'c');
      m.setDouble("double1", 1.0D);
      m.setFloat("float1", 2.0F);
      m.setInt("int1", 3);
      m.setLong("long1", 4L);
      m.setObject("object1", obj);
      m.setShort("short1", (short) 5);
      m.setString("string1", "stringvalue");

      return m;
   }

   @Override
   protected void assertEquivalent(final Message m, final int mode, final boolean redelivery) throws JMSException {
      super.assertEquivalent(m, mode, redelivery);

      MapMessage map = (MapMessage) m;

      ProxyAssertSupport.assertTrue(map.getBoolean("boolean1"));
      ProxyAssertSupport.assertEquals(map.getChar("char1"), 'c');
      ProxyAssertSupport.assertEquals(map.getDouble("double1"), 1.0D, 0.0D);
      ProxyAssertSupport.assertEquals(map.getFloat("float1"), 2.0F, 0.0F);
      ProxyAssertSupport.assertEquals(map.getInt("int1"), 3);
      ProxyAssertSupport.assertEquals(map.getLong("long1"), 4L);
      ProxyAssertSupport.assertEquals(map.getObject("object1"), obj);
      ProxyAssertSupport.assertEquals(map.getShort("short1"), (short) 5);
      ProxyAssertSupport.assertEquals(map.getString("string1"), "stringvalue");
   }
}
