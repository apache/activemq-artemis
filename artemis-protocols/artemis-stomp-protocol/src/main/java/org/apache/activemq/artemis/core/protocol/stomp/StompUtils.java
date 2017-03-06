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
package org.apache.activemq.artemis.core.protocol.stomp;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.reader.MessageUtil;

public class StompUtils {

   // Constants -----------------------------------------------------
   private static final String DEFAULT_MESSAGE_PRIORITY = "4";

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void copyStandardHeadersFromFrameToMessage(StompFrame frame, Message msg) throws Exception {
      Map<String, String> headers = new HashMap<>(frame.getHeadersMap());

      String priority = headers.remove(Stomp.Headers.Send.PRIORITY);
      if (priority != null) {
         msg.setPriority(Byte.parseByte(priority));
      } else {
         msg.setPriority(Byte.parseByte(DEFAULT_MESSAGE_PRIORITY));
      }
      String persistent = headers.remove(Stomp.Headers.Send.PERSISTENT);
      if (persistent != null) {
         msg.setDurable(Boolean.parseBoolean(persistent));
      }

      msg.putObjectProperty(MessageUtil.CORRELATIONID_HEADER_NAME, headers.remove(Stomp.Headers.Send.CORRELATION_ID));
      msg.putObjectProperty(MessageUtil.TYPE_HEADER_NAME, headers.remove(Stomp.Headers.Send.TYPE));

      String groupID = headers.remove(MessageUtil.JMSXGROUPID);
      if (groupID != null) {
         msg.putStringProperty(Message.HDR_GROUP_ID, SimpleString.toSimpleString(groupID));
      }
      String contentType = headers.remove(Stomp.Headers.CONTENT_TYPE);
      if (contentType != null) {
         msg.putStringProperty(Message.HDR_CONTENT_TYPE, SimpleString.toSimpleString(contentType));
      }
      Object replyTo = headers.remove(Stomp.Headers.Send.REPLY_TO);
      if (replyTo != null) {
         msg.putStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME, SimpleString.toSimpleString((String) replyTo));
      }
      String expiration = headers.remove(Stomp.Headers.Send.EXPIRATION_TIME);
      if (expiration != null) {
         msg.setExpiration(Long.parseLong(expiration));
      }

      // now the general headers
      for (Entry<String, String> entry : headers.entrySet()) {
         String name = entry.getKey();
         Object value = entry.getValue();
         msg.putObjectProperty(name, value);
      }
   }

   public static void copyStandardHeadersFromMessageToFrame(Message message,
                                                            StompFrame command,
                                                            int deliveryCount) throws Exception {
      command.addHeader(Stomp.Headers.Message.MESSAGE_ID, String.valueOf(message.getMessageID()));
      command.addHeader(Stomp.Headers.Message.DESTINATION, message.getAddress().toString());

      if (message.getObjectProperty(MessageUtil.CORRELATIONID_HEADER_NAME) != null) {
         command.addHeader(Stomp.Headers.Message.CORRELATION_ID, message.getObjectProperty(MessageUtil.CORRELATIONID_HEADER_NAME).toString());
      }
      command.addHeader(Stomp.Headers.Message.EXPIRATION_TIME, "" + message.getExpiration());
      command.addHeader(Stomp.Headers.Message.REDELIVERED, String.valueOf(deliveryCount > 1));
      command.addHeader(Stomp.Headers.Message.PRIORITY, "" + message.getPriority());
      command.addHeader(Stomp.Headers.Message.PERSISTENT, "" + message.isDurable());
      if (message.getStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME) != null) {
         command.addHeader(Stomp.Headers.Message.REPLY_TO, message.getStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME));
      }
      command.addHeader(Stomp.Headers.Message.TIMESTAMP, "" + message.getTimestamp());

      if (message.getObjectProperty(MessageUtil.TYPE_HEADER_NAME) != null) {
         command.addHeader(Stomp.Headers.Message.TYPE, message.getObjectProperty(MessageUtil.TYPE_HEADER_NAME).toString());
      }
      if (message.getStringProperty(Message.HDR_CONTENT_TYPE.toString()) != null) {
         command.addHeader(Stomp.Headers.CONTENT_TYPE, message.getStringProperty(Message.HDR_CONTENT_TYPE.toString()));
      }
      if (message.getStringProperty(Message.HDR_VALIDATED_USER.toString()) != null) {
         command.addHeader(Stomp.Headers.Message.VALIDATED_USER, message.getStringProperty(Message.HDR_VALIDATED_USER.toString()));
      }

      // now let's add all the rest of the message headers
      Set<SimpleString> names = message.getPropertyNames();
      for (SimpleString name : names) {
         if (name.equals(ClientMessageImpl.REPLYTO_HEADER_NAME) ||
            name.equals(Message.HDR_CONTENT_TYPE) ||
            name.equals(Message.HDR_VALIDATED_USER) ||
            name.equals(MessageUtil.TYPE_HEADER_NAME) ||
            name.equals(MessageUtil.CORRELATIONID_HEADER_NAME) ||
            name.toString().equals(Stomp.Headers.Message.DESTINATION)) {
            continue;
         }

         command.addHeader(name.toString(), message.getObjectProperty(name).toString());
      }
   }
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
