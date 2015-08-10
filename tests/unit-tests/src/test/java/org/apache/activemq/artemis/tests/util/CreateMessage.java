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
package org.apache.activemq.artemis.tests.util;

import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQBytesMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;

public final class CreateMessage {

   private CreateMessage() {
      // Utility class
   }

   public static ClientMessage createTextMessage(final String s, final ClientSession clientSession) {
      ClientMessage message = clientSession.createMessage(ActiveMQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte) 4);
      message.getBodyBuffer().writeString(s);
      return message;
   }

   public static ClientMessage createBytesMessage(final ClientSession session, final byte[] b, final boolean durable) {
      ClientMessage message = session.createMessage(ActiveMQBytesMessage.TYPE, durable, 0, System.currentTimeMillis(), (byte) 1);
      message.getBodyBuffer().writeBytes(b);
      return message;
   }

   public static ClientMessage createTextMessage(final ClientSession session, final String s, final boolean durable) {
      ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE, durable, 0, System.currentTimeMillis(), (byte) 1);
      message.getBodyBuffer().writeString(s);
      return message;
   }

}
