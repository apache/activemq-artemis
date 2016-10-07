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
package org.apache.activemq.artemis.protocol.amqp.converter.message;

import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_NATIVE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.createBytesMessage;

import javax.jms.DeliveryMode;
import javax.jms.Message;

import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.utils.IDGenerator;

public class AMQPRawInboundTransformer extends InboundTransformer {

   public AMQPRawInboundTransformer(IDGenerator idGenerator) {
      super(idGenerator);
   }

   @Override
   public String getTransformerName() {
      return TRANSFORMER_RAW;
   }

   @Override
   public InboundTransformer getFallbackTransformer() {
      return null;  // No fallback from full raw transform
   }

   @Override
   public ServerJMSMessage transform(EncodedMessage amqpMessage) throws Exception {
      ServerJMSBytesMessage message = createBytesMessage(idGenerator);
      message.writeBytes(amqpMessage.getArray(), amqpMessage.getArrayOffset(), amqpMessage.getLength());

      // We cannot decode the message headers to check so err on the side of caution
      // and mark all messages as persistent.
      message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
      message.setJMSPriority(Message.DEFAULT_PRIORITY);
      message.setJMSTimestamp(System.currentTimeMillis());

      message.setLongProperty(JMS_AMQP_MESSAGE_FORMAT, amqpMessage.getMessageFormat());
      message.setBooleanProperty(JMS_AMQP_NATIVE, true);

      return message;
   }
}
