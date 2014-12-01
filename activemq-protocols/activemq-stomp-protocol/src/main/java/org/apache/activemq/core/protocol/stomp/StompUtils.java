/**
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
package org.apache.activemq.core.protocol.stomp;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.client.impl.ClientMessageImpl;
import org.apache.activemq.core.message.impl.MessageInternal;
import org.apache.activemq.core.server.impl.ServerMessageImpl;

/**
 * A StompUtils
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class StompUtils
{
   // Constants -----------------------------------------------------
   private static final String DEFAULT_MESSAGE_PRIORITY = "4";

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void copyStandardHeadersFromFrameToMessage(StompFrame frame, ServerMessageImpl msg) throws Exception
   {
      Map<String, String> headers = new HashMap<String, String>(frame.getHeadersMap());

      String priority = headers.remove(Stomp.Headers.Send.PRIORITY);
      if (priority != null)
      {
         msg.setPriority(Byte.parseByte(priority));
      }
      else
      {
         msg.setPriority(Byte.parseByte(DEFAULT_MESSAGE_PRIORITY));
      }
      String persistent = headers.remove(Stomp.Headers.Send.PERSISTENT);
      if (persistent != null)
      {
         msg.setDurable(Boolean.parseBoolean(persistent));
      }

      // FIXME should use a proper constant
      msg.putObjectProperty("JMSCorrelationID", headers.remove(Stomp.Headers.Send.CORRELATION_ID));
      msg.putObjectProperty("JMSType", headers.remove(Stomp.Headers.Send.TYPE));

      String groupID = headers.remove("JMSXGroupID");
      if (groupID != null)
      {
         msg.putStringProperty(Message.HDR_GROUP_ID, SimpleString.toSimpleString(groupID));
      }
      Object replyTo = headers.remove(Stomp.Headers.Send.REPLY_TO);
      if (replyTo != null)
      {
         msg.putStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME, SimpleString.toSimpleString((String) replyTo));
      }
      String expiration = headers.remove(Stomp.Headers.Send.EXPIRATION_TIME);
      if (expiration != null)
      {
         msg.setExpiration(Long.parseLong(expiration));
      }

      // now the general headers
      for (Entry<String, String> entry : headers.entrySet())
      {
         String name = entry.getKey();
         Object value = entry.getValue();
         msg.putObjectProperty(name, value);
      }
   }

   public static void copyStandardHeadersFromMessageToFrame(MessageInternal message, StompFrame command, int deliveryCount) throws Exception
   {
      command.addHeader(Stomp.Headers.Message.MESSAGE_ID, String.valueOf(message.getMessageID()));
      command.addHeader(Stomp.Headers.Message.DESTINATION, message.getAddress().toString());

      if (message.getObjectProperty("JMSCorrelationID") != null)
      {
         command.addHeader(Stomp.Headers.Message.CORRELATION_ID, message.getObjectProperty("JMSCorrelationID").toString());
      }
      command.addHeader(Stomp.Headers.Message.EXPIRATION_TIME, "" + message.getExpiration());
      command.addHeader(Stomp.Headers.Message.REDELIVERED, String.valueOf(deliveryCount > 1));
      command.addHeader(Stomp.Headers.Message.PRORITY, "" + message.getPriority());
      if (message.getStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME) != null)
      {
         command.addHeader(Stomp.Headers.Message.REPLY_TO,
                           message.getStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME));
      }
      command.addHeader(Stomp.Headers.Message.TIMESTAMP, "" + message.getTimestamp());

      if (message.getObjectProperty("JMSType") != null)
      {
         command.addHeader(Stomp.Headers.Message.TYPE, message.getObjectProperty("JMSType").toString());
      }

      // now let's add all the message headers
      Set<SimpleString> names = message.getPropertyNames();
      for (SimpleString name : names)
      {
         String value = name.toString();
         if (name.equals(ClientMessageImpl.REPLYTO_HEADER_NAME) ||
            value.equals("JMSType") ||
            value.equals("JMSCorrelationID") ||
            value.equals(Stomp.Headers.Message.DESTINATION))
         {
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
