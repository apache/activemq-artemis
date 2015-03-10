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
package org.apache.activemq.core.protocol.proton.converter;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.qpid.proton.jms.JMSVendor;
import org.apache.activemq.core.buffers.impl.ResetLimitWrappedActiveMQBuffer;
import org.apache.activemq.core.protocol.proton.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.core.protocol.proton.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.core.protocol.proton.converter.jms.ServerJMSMessage;
import org.apache.activemq.core.protocol.proton.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.core.protocol.proton.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.server.impl.ServerMessageImpl;
import org.apache.activemq.utils.IDGenerator;

public class ActiveMQJMSVendor extends JMSVendor
{

   private final IDGenerator serverGenerator;

   ActiveMQJMSVendor(IDGenerator idGenerator)
   {
      this.serverGenerator = idGenerator;
   }

   @Override
   public BytesMessage createBytesMessage()
   {
      return new ServerJMSBytesMessage(newMessage(org.apache.activemq.api.core.Message.BYTES_TYPE), 0);
   }

   @Override
   public StreamMessage createStreamMessage()
   {
      return new ServerJMSStreamMessage(newMessage(org.apache.activemq.api.core.Message.STREAM_TYPE), 0);
   }

   @Override
   public Message createMessage()
   {
      return new ServerJMSMessage(newMessage(org.apache.activemq.api.core.Message.DEFAULT_TYPE), 0 );
   }

   @Override
   public TextMessage createTextMessage()
   {
      return new ServerJMSTextMessage(newMessage(org.apache.activemq.api.core.Message.TEXT_TYPE), 0);
   }

   @Override
   public ObjectMessage createObjectMessage()
   {
      return null;
   }

   @Override
   public MapMessage createMapMessage()
   {
      return new ServerJMSMapMessage(newMessage(org.apache.activemq.api.core.Message.MAP_TYPE), 0);
   }

   @Override
   public void setJMSXUserID(Message message, String s)
   {
   }

   @Override
   public Destination createDestination(String name)
   {
      return super.createDestination(name);
   }

   @Override
   public <T extends Destination> T createDestination(String name, Class<T> kind)
   {
      return super.createDestination(name, kind);
   }

   @Override
   public void setJMSXGroupID(Message message, String s)
   {

   }

   @Override
   public void setJMSXGroupSequence(Message message, int i)
   {

   }

   @Override
   public void setJMSXDeliveryCount(Message message, long l)
   {

   }


   public ServerJMSMessage wrapMessage(int messageType, ServerMessage wrapped, int deliveryCount)
   {
      switch (messageType)
      {
         case org.apache.activemq.api.core.Message.STREAM_TYPE:
            return new ServerJMSStreamMessage(wrapped, deliveryCount);
         case org.apache.activemq.api.core.Message.BYTES_TYPE:
            return new ServerJMSBytesMessage(wrapped, deliveryCount);
         case org.apache.activemq.api.core.Message.MAP_TYPE:
            return new ServerJMSMapMessage(wrapped, deliveryCount);
         case org.apache.activemq.api.core.Message.TEXT_TYPE:
            return new ServerJMSTextMessage(wrapped, deliveryCount);
         default:
            return new ServerJMSMessage(wrapped, deliveryCount);
      }

   }


   @Override
   public String toAddress(Destination destination)
   {
      return null;
   }


   private ServerMessageImpl newMessage(byte messageType)
   {
      ServerMessageImpl message = new ServerMessageImpl(serverGenerator.generateID(), 512);
      message.setType(messageType);
      ((ResetLimitWrappedActiveMQBuffer)message.getBodyBuffer()).setMessage(null);
      return message;
   }

}
