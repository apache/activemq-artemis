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
package org.apache.activemq.artemis.rest.queue;

import javax.ws.rs.core.Response;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.HttpHeaderProperty;

public abstract class ConsumedMessage {

   public static final String POSTED_AS_HTTP_MESSAGE = "postedAsHttpMessage";
   protected ClientMessage message;

   public ConsumedMessage(ClientMessage message) {
      this.message = message;
   }

   public long getMessageID() {
      return message.getMessageID();
   }

   public abstract void build(Response.ResponseBuilder builder);

   protected void buildHeaders(Response.ResponseBuilder builder) {
      for (SimpleString key : message.getPropertyNames()) {
         String k = key.toString();
         String headerName = HttpHeaderProperty.fromPropertyName(k);
         if (headerName == null) {
            continue;
         }
         builder.header(headerName, message.getStringProperty(k));
         ActiveMQRestLogger.LOGGER.debug("Adding " + headerName + "=" + message.getStringProperty(k));
      }
   }

   public static ConsumedMessage createConsumedMessage(ClientMessage message, ConnectionFactoryOptions options) {
      Boolean aBoolean = message.getBooleanProperty(POSTED_AS_HTTP_MESSAGE);
      if (aBoolean != null && aBoolean.booleanValue()) {
         return new ConsumedHttpMessage(message);
      } else if (message.getType() == Message.OBJECT_TYPE) {
         return new ConsumedObjectMessage(message, options);
      } else {
         throw new IllegalArgumentException("ClientMessage must be an HTTP message or an Object message: " + message + " type: " + message.getType());
      }
   }
}
