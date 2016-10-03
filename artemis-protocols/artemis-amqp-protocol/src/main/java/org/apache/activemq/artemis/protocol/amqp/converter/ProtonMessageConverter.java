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
package org.apache.activemq.artemis.protocol.amqp.converter;

import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_NATIVE;

import java.io.IOException;

import javax.jms.BytesMessage;

import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport;
import org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPNativeOutboundTransformer;
import org.apache.activemq.artemis.protocol.amqp.converter.message.EncodedMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.message.InboundTransformer;
import org.apache.activemq.artemis.protocol.amqp.converter.message.JMSMappingInboundTransformer;
import org.apache.activemq.artemis.protocol.amqp.converter.message.JMSMappingOutboundTransformer;
import org.apache.activemq.artemis.protocol.amqp.converter.message.OutboundTransformer;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ProtonMessageConverter implements MessageConverter {

   public ProtonMessageConverter(IDGenerator idGenerator) {
      inboundTransformer = new JMSMappingInboundTransformer(idGenerator);
      outboundTransformer = new JMSMappingOutboundTransformer(idGenerator);
   }

   private final InboundTransformer inboundTransformer;
   private final OutboundTransformer outboundTransformer;

   @Override
   public ServerMessage inbound(Object messageSource) throws Exception {
      EncodedMessage encodedMessageSource = (EncodedMessage) messageSource;
      ServerJMSMessage transformedMessage = null;

      try {
         transformedMessage = inboundTransformer.transform(encodedMessageSource);
      } catch (Exception e) {
         ActiveMQClientLogger.LOGGER.debug("Transform of message using [{}] transformer, failed" + inboundTransformer.getTransformerName());
         ActiveMQClientLogger.LOGGER.trace("Transformation error:", e);

         throw new IOException("Failed to transform incoming delivery, skipping.");
      }

      transformedMessage.encode();

      return (ServerMessage) transformedMessage.getInnerMessage();
   }

   @Override
   public Object outbound(ServerMessage messageOutbound, int deliveryCount) throws Exception {
      // Useful for testing but not recommended for real life use.
      ByteBuf nettyBuffer = Unpooled.buffer(1024);
      NettyWritable buffer = new NettyWritable(nettyBuffer);
      long messageFormat = (long) outbound(messageOutbound, deliveryCount, buffer);

      EncodedMessage encoded = new EncodedMessage(messageFormat, nettyBuffer.array(), nettyBuffer.arrayOffset() + nettyBuffer.readerIndex(),
         nettyBuffer.readableBytes());

      return encoded;
   }

   public Object outbound(ServerMessage messageOutbound, int deliveryCount, WritableBuffer buffer) throws Exception {
      ServerJMSMessage jmsMessage = AMQPMessageSupport.wrapMessage(messageOutbound.getType(), messageOutbound, deliveryCount);

      jmsMessage.decode();

      if (jmsMessage.getBooleanProperty(JMS_AMQP_NATIVE)) {
         if (jmsMessage instanceof BytesMessage) {
            return AMQPNativeOutboundTransformer.transform(outboundTransformer, (ServerJMSBytesMessage) jmsMessage, buffer);
         } else {
            return 0;
         }
      } else {
         return outboundTransformer.transform(jmsMessage, buffer);
      }
   }
}
