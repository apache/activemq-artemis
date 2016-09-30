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
package org.apache.activemq.artemis.rest;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Providers;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Type;

import org.apache.activemq.artemis.rest.util.HttpMessageHelper;
import org.jboss.resteasy.core.Headers;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.util.GenericType;

public class Jms {

   /**
    * Set a JMS Message property to the value of an HTTP header
    *
    * @param message
    * @param name
    * @param value
    */
   public static void setHttpHeader(Message message, String name, String value) {
      try {
         message.setStringProperty(HttpHeaderProperty.toPropertyName(name), value);
      } catch (JMSException e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Get an HTTP header value from a JMS Message
    *
    * @param message
    * @param name
    * @return the header or {@code null} if not present
    */
   public static String getHttpHeader(Message message, String name) {
      try {
         return message.getStringProperty(HttpHeaderProperty.toPropertyName(name));
      } catch (JMSException e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Extract an object using a built-in RESTEasy JAX-RS MessageBodyReader
    *
    * @param message
    * @param type
    * @param <T>
    * @return
    */
   public static <T> T getEntity(Message message, Class<T> type) {
      return getEntity(message, type, null, ResteasyProviderFactory.getInstance());
   }

   /**
    * Extract an object using a built-in RESTEasy JAX-RS MessageBodyReader
    *
    * @param message
    * @param type
    * @param factory
    * @param <T>
    * @return
    */
   public static <T> T getEntity(Message message, Class<T> type, ResteasyProviderFactory factory) {
      return getEntity(message, type, null, factory);
   }

   /**
    * Extract an object using a built-in RESTEasy JAX-RS MessageBodyReader
    *
    * @param message
    * @param type
    * @param factory
    * @param <T>
    * @return
    * @throws UnknownMediaType
    * @throws UnmarshalException
    */
   public static <T> T getEntity(Message message,
                                 GenericType<T> type,
                                 ResteasyProviderFactory factory) throws UnknownMediaType {
      return getEntity(message, type.getType(), type.getGenericType(), factory);
   }

   public static boolean isHttpMessage(Message message) {
      try {
         return message.getBooleanProperty(HttpMessageHelper.POSTED_AS_HTTP_MESSAGE);
      } catch (JMSException e) {
         return false;
      }
   }

   /**
    * Extract an object using a built-in RESTEasy JAX-RS MessageBodyReader
    *
    * @param message
    * @param type
    * @param genericType
    * @param factory
    * @param <T>
    * @return
    * @throws UnknownMediaType
    * @throws UnmarshalException
    */
   public static <T> T getEntity(Message message,
                                 Class<T> type,
                                 Type genericType,
                                 ResteasyProviderFactory factory) throws UnknownMediaType {
      if (!isHttpMessage(message)) {
         try {
            return (T) ((ObjectMessage) message).getObject();
         } catch (JMSException e) {
            throw new RuntimeException(e);
         }
      }
      BytesMessage bytesMessage = (BytesMessage) message;

      try {
         long size = bytesMessage.getBodyLength();
         if (size <= 0) {
            return null;
         }

         byte[] body = new byte[(int) size];
         bytesMessage.readBytes(body);

         String contentType = message.getStringProperty(HttpHeaderProperty.CONTENT_TYPE);
         if (contentType == null) {
            throw new UnknownMediaType("Message did not have a Content-Type header cannot extract entity");
         }
         MediaType ct = MediaType.valueOf(contentType);
         MessageBodyReader<T> reader = factory.getMessageBodyReader(type, genericType, null, ct);
         if (reader == null) {
            throw new UnmarshalException("Unable to find a JAX-RS reader for type " + type.getName() + " and media type " + contentType);
         }

         Providers current = ResteasyProviderFactory.getContextData(Providers.class);
         ResteasyProviderFactory.pushContext(Providers.class, factory);
         try {
            return reader.readFrom(type, genericType, null, ct, new Headers<String>(), new ByteArrayInputStream(body));
         } finally {
            ResteasyProviderFactory.popContextData(Providers.class);
            if (current != null)
               ResteasyProviderFactory.pushContext(Providers.class, current);
         }
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

}
