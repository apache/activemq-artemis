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

package org.apache.activemq.artemis.spi.core.protocol;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.jboss.logging.Logger;

public class EmbedMessageUtil {

   private static final byte[] signature = new byte[]{(byte) 'E', (byte) 'M', (byte) 'B'};

   private static final Logger logger = Logger.getLogger(EmbedMessageUtil.class);

   public static ICoreMessage embedAsCoreMessage(Message source) {

      if (source instanceof ICoreMessage) {
         return (ICoreMessage) source;
      } else {
         Persister persister = source.getPersister();

         CoreMessage message = new CoreMessage(source.getMessageID(), persister.getEncodeSize(source) + signature.length + CoreMessage.BODY_OFFSET).setType(Message.EMBEDDED_TYPE);

         ActiveMQBuffer buffer = message.getBodyBuffer();
         buffer.writeBytes(signature);
         persister.encode(buffer, source);
         return message;
      }
   }

   public static Message extractEmbedded(ICoreMessage message) {
      if (message.getType() == Message.EMBEDDED_TYPE) {
         ActiveMQBuffer buffer = message.getReadOnlyBodyBuffer();

         if (buffer.readableBytes() < signature.length || !checkSignature(buffer)) {
            if (!logger.isTraceEnabled()) {
               logger.trace("Message type " + Message.EMBEDDED_TYPE + " was used for something other than embed messages, ignoring content and treating as a regular message");
            }
            return message;
         }

         try {
            return MessagePersister.getInstance().decode(buffer, null);
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return message;
         }
      } else {
         return message;
      }
   }

   private static boolean checkSignature(final ActiveMQBuffer buffer) {
      return buffer.readByte() == signature[0] && buffer.readByte() == signature[1] && buffer.readByte() == signature[2];
   }
}
