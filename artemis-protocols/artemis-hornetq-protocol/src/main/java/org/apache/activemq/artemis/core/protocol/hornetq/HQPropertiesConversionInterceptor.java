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

package org.apache.activemq.artemis.core.protocol.hornetq;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.MessagePacket;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class HQPropertiesConversionInterceptor implements Interceptor
{
   private static Map<SimpleString, SimpleString> dictionary;

   static
   {
      Map<SimpleString, SimpleString> d = new HashMap<SimpleString, SimpleString>();

      // Add entries for outgoing messages
      d.put(new SimpleString("_HQ_ACTUAL_EXPIRY"), new SimpleString("_AMQ_ACTUAL_EXPIRY"));
      d.put(new SimpleString("_HQ_ORIG_ADDRESS"), new SimpleString("_AMQ_ORIG_ADDRESS"));
      d.put(new SimpleString("_HQ_ORIG_QUEUE"), new SimpleString("_AMQ_ORIG_QUEUE"));
      d.put(new SimpleString("_HQ_ORIG_MESSAGE_ID"), new SimpleString("_AMQ_ORIG_MESSAGE_ID"));
      d.put(new SimpleString("_HQ_GROUP_ID"), new SimpleString("_AMQ_GROUP_ID"));
      d.put(new SimpleString("_HQ_LARGE_COMPRESSED"), new SimpleString("_AMQ_LARGE_COMPRESSED"));
      d.put(new SimpleString("_HQ_LARGE_SIZE"), new SimpleString("_AMQ_LARGE_SIZE"));
      d.put(new SimpleString("_HQ_SCHED_DELIVERY"), new SimpleString("_AMQ_SCHED_DELIVERY"));
      d.put(new SimpleString("_HQ_DUPL_ID"), new SimpleString("_AMQ_DUPL_ID"));
      d.put(new SimpleString("_HQ_LVQ_NAME"), new SimpleString("_AMQ_LVQ_NAME"));

      // Add entries for incoming messages
      d.put(new SimpleString("_AMQ_ACTUAL_EXPIRY"), new SimpleString("_HQ_ACTUAL_EXPIRY"));
      d.put(new SimpleString("_AMQ_ORIG_ADDRESS"), new SimpleString("_HQ_ORIG_ADDRESS"));
      d.put(new SimpleString("_AMQ_ORIG_QUEUE"), new SimpleString("_HQ_ORIG_QUEUE"));
      d.put(new SimpleString("_AMQ_ORIG_MESSAGE_ID"), new SimpleString("_HQ_ORIG_MESSAGE_ID"));
      d.put(new SimpleString("_AMQ_GROUP_ID"), new SimpleString("_HQ_GROUP_ID"));
      d.put(new SimpleString("_AMQ_LARGE_COMPRESSED"), new SimpleString("_HQ_LARGE_COMPRESSED"));
      d.put(new SimpleString("_AMQ_LARGE_SIZE"), new SimpleString("_HQ_LARGE_SIZE"));
      d.put(new SimpleString("_AMQ_SCHED_DELIVERY"), new SimpleString("_HQ_SCHED_DELIVERY"));
      d.put(new SimpleString("_AMQ_DUPL_ID"), new SimpleString("_HQ_DUPL_ID"));
      d.put(new SimpleString("_AMQ_LVQ_NAME"), new SimpleString("_HQ_LVQ_NAME"));

      dictionary = Collections.unmodifiableMap(d);
   }

   @Override
   public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException
   {
      if (isMessagePacket(packet))
      {
         handleReceiveMessage((MessagePacket) packet);
      }
      return true;
   }

   private void handleReceiveMessage(MessagePacket messagePacket)
   {
      Message message = messagePacket.getMessage();
      // We are modifying the key set so we iterate over a shallow copy.
      for (SimpleString property : new HashSet<>(message.getPropertyNames()))
      {
         if (dictionary.containsKey(property))
         {
            message.putObjectProperty(dictionary.get(property), message.removeProperty(property));
         }
      }
   }

   private boolean isMessagePacket(Packet packet)
   {
      int type = packet.getType();
      return type == PacketImpl.SESS_SEND ||
             type == PacketImpl.SESS_SEND_CONTINUATION ||
             type == PacketImpl.SESS_SEND_LARGE ||
             type == PacketImpl.SESS_RECEIVE_LARGE_MSG ||
             type == PacketImpl.SESS_RECEIVE_MSG;
   }
}
