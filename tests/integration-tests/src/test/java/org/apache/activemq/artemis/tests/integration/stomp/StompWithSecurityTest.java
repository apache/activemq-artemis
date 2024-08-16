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
package org.apache.activemq.artemis.tests.integration.stomp;

import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StompWithSecurityTest extends StompTestBase {

   public StompWithSecurityTest() {
      super("tcp+v10.stomp");
   }

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Test
   public void testJMSXUserID() throws Exception {
      server.getConfiguration().setPopulateValidatedUser(true);

      MessageConsumer consumer = session.createConsumer(queue);

      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");
      conn.sendFrame(frame);

      conn.disconnect();

      TextMessage message = (TextMessage) consumer.receive(1000);
      assertNotNull(message);
      assertEquals("Hello World", message.getText());
      // Assert default priority 4 is used when priority header is not set
      assertEquals(4, message.getJMSPriority(), "getJMSPriority");
      assertEquals("brianm", message.getStringProperty("JMSXUserID"), "JMSXUserID");

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      assertTrue(Math.abs(tnow - tmsg) < 1000);
   }
}
