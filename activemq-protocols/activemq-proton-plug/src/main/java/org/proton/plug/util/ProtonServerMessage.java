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
package org.proton.plug.util;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.MessageError;
import org.apache.qpid.proton.message.MessageFormat;
import org.apache.qpid.proton.message.ProtonJMessage;

/**
 * This is a serverMessage that won't deal with the body
 *
 * @author Clebert Suconic
 */
public class ProtonServerMessage implements ProtonJMessage
{
   private Header header;
   private DeliveryAnnotations deliveryAnnotations;
   private MessageAnnotations messageAnnotations;
   private Properties properties;
   private ApplicationProperties applicationProperties;

   // This should include a raw body of both footer and body
   private byte[] rawBody;

   private Section parsedBody;
   private Footer parsedFooter;

   private final int EOF = 0;

   // TODO: Enumerations maybe?
   private static final int HEADER_TYPE = 0x070;
   private static final int DELIVERY_ANNOTATIONS = 0x071;
   private static final int MESSAGE_ANNOTATIONS = 0x072;
   private static final int PROPERTIES = 0x073;
   private static final int APPLICATION_PROPERTIES = 0x074;


   /**
    * This will decode a ByteBuffer tha represents the entire message.
    * Set the limits around the parameter.
    *
    * @param buffer a limited buffer for the message
    */
   public void decode(ByteBuffer buffer)
   {

      DecoderImpl decoder = CodecCache.getDecoder();


      header = null;
      deliveryAnnotations = null;
      messageAnnotations = null;
      properties = null;
      applicationProperties = null;
      rawBody = null;

      decoder.setByteBuffer(buffer);
      try
      {
         int type = readType(buffer, decoder);
         if (type == HEADER_TYPE)
         {
            header = (Header) readSection(buffer, decoder);
            type = readType(buffer, decoder);

         }

         if (type == DELIVERY_ANNOTATIONS)
         {
            deliveryAnnotations = (DeliveryAnnotations) readSection(buffer, decoder);
            type = readType(buffer, decoder);

         }

         if (type == MESSAGE_ANNOTATIONS)
         {
            messageAnnotations = (MessageAnnotations) readSection(buffer, decoder);
            type = readType(buffer, decoder);
         }

         if (type == PROPERTIES)
         {
            properties = (Properties) readSection(buffer, decoder);
            type = readType(buffer, decoder);

         }

         if (type == APPLICATION_PROPERTIES)
         {
            applicationProperties = (ApplicationProperties) readSection(buffer, decoder);
            type = readType(buffer, decoder);
         }

         if (type != EOF)
         {
            rawBody = new byte[buffer.limit() - buffer.position()];
            buffer.get(rawBody);
         }
      }
      finally
      {
         decoder.setByteBuffer(null);
      }

   }


   public void encode(ByteBuffer buffer)
   {
      WritableBuffer writableBuffer = new WritableBuffer.ByteBufferWrapper(buffer);
      encode(writableBuffer);
   }

   public int encode(WritableBuffer writableBuffer)
   {
      final int firstPosition = writableBuffer.position();

      EncoderImpl encoder = CodecCache.getEncoder();
      encoder.setByteBuffer(writableBuffer);

      try
      {
         if (header != null)
         {
            encoder.writeObject(header);
         }
         if (deliveryAnnotations != null)
         {
            encoder.writeObject(deliveryAnnotations);
         }
         if (messageAnnotations != null)
         {
            encoder.writeObject(messageAnnotations);
         }
         if (properties != null)
         {
            encoder.writeObject(properties);
         }
         if (applicationProperties != null)
         {
            encoder.writeObject(applicationProperties);
         }

         // It should write either the parsed one or the rawBody
         if (parsedBody != null)
         {
            encoder.writeObject(parsedBody);
            if (parsedFooter != null)
            {
               encoder.writeObject(parsedFooter);
            }
         }
         else if (rawBody != null)
         {
            writableBuffer.put(rawBody, 0, rawBody.length);
         }

         return writableBuffer.position() - firstPosition;
      }
      finally
      {
         encoder.setByteBuffer((WritableBuffer) null);
      }
   }


   private int readType(ByteBuffer buffer, DecoderImpl decoder)
   {

      int pos = buffer.position();

      if (!buffer.hasRemaining())
      {
         return EOF;
      }
      try
      {
         if (buffer.get() != 0)
         {
            return EOF;
         }
         else
         {
            return ((Number) decoder.readObject()).intValue();
         }
      }
      finally
      {
         buffer.position(pos);
      }
   }


   private Section readSection(ByteBuffer buffer, DecoderImpl decoder)
   {
      if (buffer.hasRemaining())
      {
         return (Section) decoder.readObject();
      }
      else
      {
         return null;
      }
   }


   // At the moment we only need encode implemented!!!
   @Override
   public boolean isDurable()
   {
      return false;
   }

   @Override
   public long getDeliveryCount()
   {
      return 0;
   }

   @Override
   public short getPriority()
   {
      return 0;
   }

   @Override
   public boolean isFirstAcquirer()
   {
      return false;
   }

   @Override
   public long getTtl()
   {
      return 0;
   }

   @Override
   public void setDurable(boolean durable)
   {

   }

   @Override
   public void setTtl(long ttl)
   {

   }

   @Override
   public void setDeliveryCount(long deliveryCount)
   {

   }

   @Override
   public void setFirstAcquirer(boolean firstAcquirer)
   {

   }

   @Override
   public void setPriority(short priority)
   {

   }

   @Override
   public Object getMessageId()
   {
      return null;
   }

   @Override
   public long getGroupSequence()
   {
      return 0;
   }

   @Override
   public String getReplyToGroupId()
   {
      return null;
   }

   @Override
   public long getCreationTime()
   {
      return 0;
   }

   @Override
   public String getAddress()
   {
      return null;
   }

   @Override
   public byte[] getUserId()
   {
      return new byte[0];
   }

   @Override
   public String getReplyTo()
   {
      return null;
   }

   @Override
   public String getGroupId()
   {
      return null;
   }

   @Override
   public String getContentType()
   {
      return null;
   }

   @Override
   public long getExpiryTime()
   {
      return 0;
   }

   @Override
   public Object getCorrelationId()
   {
      return null;
   }

   @Override
   public String getContentEncoding()
   {
      return null;
   }

   @Override
   public String getSubject()
   {
      return null;
   }

   @Override
   public void setGroupSequence(long groupSequence)
   {

   }

   @Override
   public void setUserId(byte[] userId)
   {

   }

   @Override
   public void setCreationTime(long creationTime)
   {

   }

   @Override
   public void setSubject(String subject)
   {

   }

   @Override
   public void setGroupId(String groupId)
   {

   }

   @Override
   public void setAddress(String to)
   {

   }

   @Override
   public void setExpiryTime(long absoluteExpiryTime)
   {

   }

   @Override
   public void setReplyToGroupId(String replyToGroupId)
   {

   }

   @Override
   public void setContentEncoding(String contentEncoding)
   {

   }

   @Override
   public void setContentType(String contentType)
   {

   }

   @Override
   public void setReplyTo(String replyTo)
   {

   }

   @Override
   public void setCorrelationId(Object correlationId)
   {

   }

   @Override
   public void setMessageId(Object messageId)
   {

   }

   @Override
   public Header getHeader()
   {
      return null;
   }

   @Override
   public DeliveryAnnotations getDeliveryAnnotations()
   {
      return null;
   }

   @Override
   public MessageAnnotations getMessageAnnotations()
   {
      return null;
   }

   @Override
   public Properties getProperties()
   {
      return null;
   }

   @Override
   public ApplicationProperties getApplicationProperties()
   {
      return null;
   }

   @Override
   public Section getBody()
   {
      return null;
   }

   @Override
   public Footer getFooter()
   {
      return null;
   }

   @Override
   public void setHeader(Header header)
   {

   }

   @Override
   public void setDeliveryAnnotations(DeliveryAnnotations deliveryAnnotations)
   {

   }

   @Override
   public void setMessageAnnotations(MessageAnnotations messageAnnotations)
   {

   }

   @Override
   public void setProperties(Properties properties)
   {

   }

   @Override
   public void setApplicationProperties(ApplicationProperties applicationProperties)
   {

   }

   @Override
   public void setBody(Section body)
   {

   }

   @Override
   public void setFooter(Footer footer)
   {

   }

   @Override
   public int decode(byte[] data, int offset, int length)
   {
      return 0;
   }

   @Override
   public int encode(byte[] data, int offset, int length)
   {
      return 0;
   }

   @Override
   public void load(Object data)
   {

   }

   @Override
   public Object save()
   {
      return null;
   }

   @Override
   public String toAMQPFormat(Object value)
   {
      return null;
   }

   @Override
   public Object parseAMQPFormat(String value)
   {
      return null;
   }

   @Override
   public void setMessageFormat(MessageFormat format)
   {

   }

   @Override
   public MessageFormat getMessageFormat()
   {
      return null;
   }

   @Override
   public void clear()
   {

   }

   @Override
   public MessageError getError()
   {
      return null;
   }

   @Override
   public int encode2(byte[] data, int offset, int length)
   {
      return 0;
   }
}
