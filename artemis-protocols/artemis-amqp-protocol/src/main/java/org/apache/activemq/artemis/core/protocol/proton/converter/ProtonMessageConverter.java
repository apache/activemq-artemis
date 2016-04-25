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
package org.apache.activemq.artemis.core.protocol.proton.converter;

import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.transport.amqp.message.EncodedMessage;
import org.apache.activemq.transport.amqp.message.InboundTransformer;
import org.apache.activemq.transport.amqp.message.JMSMappingInboundTransformer;
import org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer;
import org.apache.activemq.artemis.core.protocol.proton.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.utils.IDGenerator;

import javax.jms.BytesMessage;
import java.io.IOException;

public class ProtonMessageConverter implements MessageConverter {

   ActiveMQJMSVendor activeMQJMSVendor;

   private final String prefixVendor;

   public ProtonMessageConverter(IDGenerator idGenerator) {
      activeMQJMSVendor = new ActiveMQJMSVendor(idGenerator);
      inboundTransformer = new JMSMappingInboundTransformer(activeMQJMSVendor);
      outboundTransformer = new JMSMappingOutboundTransformer(activeMQJMSVendor);
      prefixVendor = outboundTransformer.getPrefixVendor();
   }

   private final InboundTransformer inboundTransformer;
   private final JMSMappingOutboundTransformer outboundTransformer;

   @Override
   public ServerMessage inbound(Object messageSource) throws Exception {
      ServerJMSMessage jmsMessage = inboundJMSType((EncodedMessage) messageSource);

      return (ServerMessage) jmsMessage.getInnerMessage();
   }

   /**
    * Just create the JMS Part of the inbound (for testing)
    *
    * @param messageSource
    * @return
    * @throws Exception                    https://issues.jboss.org/browse/ENTMQ-1560
    */
   public ServerJMSMessage inboundJMSType(EncodedMessage messageSource) throws Exception {
      EncodedMessage encodedMessageSource = messageSource;
      ServerJMSMessage transformedMessage = null;

      InboundTransformer transformer = inboundTransformer;

      while (transformer != null) {
         try {
            transformedMessage = (ServerJMSMessage) transformer.transform(encodedMessageSource);
            break;
         }
         catch (Exception e) {
            ActiveMQClientLogger.LOGGER.debug("Transform of message using [{}] transformer, failed" + inboundTransformer.getTransformerName());
            ActiveMQClientLogger.LOGGER.trace("Transformation error:", e);

            transformer = transformer.getFallbackTransformer();
         }
      }

      if (transformedMessage == null) {
         throw new IOException("Failed to transform incoming delivery, skipping.");
      }

      transformedMessage.encode();

      return transformedMessage;
   }

   @Override
   public Object outbound(ServerMessage messageOutbound, int deliveryCount) throws Exception {
      ServerJMSMessage jmsMessage = activeMQJMSVendor.wrapMessage(messageOutbound.getType(), messageOutbound, deliveryCount);

      jmsMessage.decode();

      if (jmsMessage.getBooleanProperty(prefixVendor + "NATIVE")) {
         if (jmsMessage instanceof BytesMessage) {
            return AMQPNativeOutboundTransformer.transform(outboundTransformer, (BytesMessage) jmsMessage);
         }
         else {
            return null;
         }
      }
      else {
         return outboundTransformer.convert(jmsMessage);
      }
   }
}
