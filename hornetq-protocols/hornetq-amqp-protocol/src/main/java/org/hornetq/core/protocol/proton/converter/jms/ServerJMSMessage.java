/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.protocol.proton.converter.jms;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Collections;
import java.util.Enumeration;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.reader.MessageUtil;

/**
 * @author Clebert Suconic
 */

public class ServerJMSMessage implements Message
{
   protected final MessageInternal message;

   protected int deliveryCount;

   public MessageInternal getInnerMessage()
   {
      return message;
   }


   public ServerJMSMessage(MessageInternal message, int deliveryCount)
   {
      this.message = message;
      this.deliveryCount = deliveryCount;
   }


   @Override
   public final String getJMSMessageID() throws JMSException
   {
      return null;
   }

   @Override
   public final void setJMSMessageID(String id) throws JMSException
   {
   }

   @Override
   public final long getJMSTimestamp() throws JMSException
   {
      return message.getTimestamp();
   }

   @Override
   public final void setJMSTimestamp(long timestamp) throws JMSException
   {
      message.setTimestamp(timestamp);
   }


   @Override
   public final byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      return MessageUtil.getJMSCorrelationIDAsBytes(message);
   }

   @Override
   public final void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      try
      {
         MessageUtil.setJMSCorrelationIDAsBytes(message, correlationID);
      }
      catch (HornetQException e)
      {
         throw new JMSException(e.getMessage());
      }
   }

   @Override
   public final void setJMSCorrelationID(String correlationID) throws JMSException
   {
      MessageUtil.setJMSCorrelationID(message, correlationID);
   }

   @Override
   public final String getJMSCorrelationID() throws JMSException
   {
      return MessageUtil.getJMSCorrelationID(message);
   }

   @Override
   public final Destination getJMSReplyTo() throws JMSException
   {
      SimpleString reply = MessageUtil.getJMSReplyTo(message);
      if (reply != null)
      {
         return HornetQDestination.fromAddress(reply.toString());
      }
      else
      {
         return null;
      }
   }

   @Override
   public final void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      MessageUtil.setJMSReplyTo(message, replyTo == null ? null : ((HornetQDestination) replyTo).getSimpleAddress());

   }

   public final Destination getJMSDestination() throws JMSException
   {
      SimpleString sdest = message.getAddress();

      if (sdest == null)
      {
         return null;
      }
      else
      {
         if (!sdest.toString().startsWith("jms."))
         {
            return new HornetQQueue(sdest.toString(), sdest.toString());
         }
         else
         {
            return HornetQDestination.fromAddress(sdest.toString());
         }
      }
   }

   @Override
   public final void setJMSDestination(Destination destination) throws JMSException
   {
      if (destination == null)
      {
         message.setAddress(null);
      }
      else
      {
         message.setAddress(((HornetQDestination) destination).getSimpleAddress());
      }

   }

   @Override
   public final int getJMSDeliveryMode() throws JMSException
   {
      return message.isDurable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
   }

   @Override
   public final void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      if (deliveryMode == DeliveryMode.PERSISTENT)
      {
         message.setDurable(true);
      }
      else if (deliveryMode == DeliveryMode.NON_PERSISTENT)
      {
         message.setDurable(false);
      }
      else
      {
         throw new JMSException("Invalid mode " + deliveryMode);
      }
   }

   @Override
   public final boolean getJMSRedelivered() throws JMSException
   {
      return false;
   }

   @Override
   public final void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      // no op
   }

   @Override
   public final String getJMSType() throws JMSException
   {
      return MessageUtil.getJMSType(message);
   }

   @Override
   public final void setJMSType(String type) throws JMSException
   {
      MessageUtil.setJMSType(message, type);
   }

   @Override
   public final long getJMSExpiration() throws JMSException
   {
      return message.getExpiration();
   }

   @Override
   public final void setJMSExpiration(long expiration) throws JMSException
   {
      message.setExpiration(expiration);
   }

   @Override
   public final long getJMSDeliveryTime() throws JMSException
   {
      // no op
      return 0;
   }

   @Override
   public final void setJMSDeliveryTime(long deliveryTime) throws JMSException
   {
      // no op
   }

   @Override
   public final int getJMSPriority() throws JMSException
   {
      return message.getPriority();
   }

   @Override
   public final void setJMSPriority(int priority) throws JMSException
   {
      message.setPriority((byte) priority);
   }

   @Override
   public final void clearProperties() throws JMSException
   {
      MessageUtil.clearProperties(message);

   }

   @Override
   public final boolean propertyExists(String name) throws JMSException
   {
      return MessageUtil.propertyExists(message, name);
   }

   @Override
   public final boolean getBooleanProperty(String name) throws JMSException
   {
      return message.getBooleanProperty(name);
   }

   @Override
   public final byte getByteProperty(String name) throws JMSException
   {
      return message.getByteProperty(name);
   }

   @Override
   public final short getShortProperty(String name) throws JMSException
   {
      return message.getShortProperty(name);
   }

   @Override
   public final int getIntProperty(String name) throws JMSException
   {
      if (MessageUtil.JMSXDELIVERYCOUNT.equals(name))
      {
         return deliveryCount;
      }

      return message.getIntProperty(name);
   }

   @Override
   public final long getLongProperty(String name) throws JMSException
   {
      if (MessageUtil.JMSXDELIVERYCOUNT.equals(name))
      {
         return deliveryCount;
      }

      return message.getLongProperty(name);
   }

   @Override
   public final float getFloatProperty(String name) throws JMSException
   {
      return message.getFloatProperty(name);
   }

   @Override
   public final double getDoubleProperty(String name) throws JMSException
   {
      return message.getDoubleProperty(name);
   }

   @Override
   public final String getStringProperty(String name) throws JMSException
   {
      if (MessageUtil.JMSXDELIVERYCOUNT.equals(name))
      {
         return String.valueOf(deliveryCount);
      }


      return message.getStringProperty(name);
   }

   @Override
   public final Object getObjectProperty(String name) throws JMSException
   {
      Object val = message.getObjectProperty(name);
      if (val instanceof SimpleString)
      {
         val = ((SimpleString)val).toString();
      }
      return val;
   }

   @Override
   public final Enumeration getPropertyNames() throws JMSException
   {
      return Collections.enumeration(MessageUtil.getPropertyNames(message));
   }

   @Override
   public final void setBooleanProperty(String name, boolean value) throws JMSException
   {
      message.putBooleanProperty(name, value);
   }

   @Override
   public final void setByteProperty(String name, byte value) throws JMSException
   {
      message.putByteProperty(name, value);
   }

   @Override
   public final void setShortProperty(String name, short value) throws JMSException
   {
      message.putShortProperty(name, value);
   }

   @Override
   public final void setIntProperty(String name, int value) throws JMSException
   {
      message.putIntProperty(name, value);
   }

   @Override
   public final void setLongProperty(String name, long value) throws JMSException
   {
      message.putLongProperty(name, value);
   }

   @Override
   public final void setFloatProperty(String name, float value) throws JMSException
   {
      message.putFloatProperty(name, value);
   }

   @Override
   public final void setDoubleProperty(String name, double value) throws JMSException
   {
      message.putDoubleProperty(name, value);
   }

   @Override
   public final void setStringProperty(String name, String value) throws JMSException
   {
      message.putStringProperty(name, value);
   }

   @Override
   public final void setObjectProperty(String name, Object value) throws JMSException
   {
      message.putObjectProperty(name, value);
   }

   @Override
   public final void acknowledge() throws JMSException
   {
      // no op
   }

   @Override
   public void clearBody() throws JMSException
   {
      message.getBodyBuffer().clear();
   }

   @Override
   public final <T> T getBody(Class<T> c) throws JMSException
   {
      // no op.. jms2 not used on the conversion
      return null;
   }

   /**
    * Encode the body into the internal message
    */
   public void encode() throws Exception
   {
      message.getBodyBuffer().resetReaderIndex();
   }


   public void decode() throws Exception
   {
      message.getBodyBuffer().resetReaderIndex();
   }

   @Override
   public final boolean isBodyAssignableTo(Class c) throws JMSException
   {
      // no op.. jms2 not used on the conversion
      return false;
   }
}
