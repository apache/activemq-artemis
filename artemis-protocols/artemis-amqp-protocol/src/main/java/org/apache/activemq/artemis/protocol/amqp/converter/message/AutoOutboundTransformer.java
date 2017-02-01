/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.converter.message;

import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.qpid.proton.codec.WritableBuffer;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import java.io.UnsupportedEncodingException;

import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_NATIVE;

public class AutoOutboundTransformer extends JMSMappingOutboundTransformer {

   private final JMSMappingOutboundTransformer transformer;
   private final AMQPNativeOutboundTransformer nativeOutboundTransformer;

   public AutoOutboundTransformer(IDGenerator idGenerator) {
      super(idGenerator);
      transformer = new JMSMappingOutboundTransformer(idGenerator);
      nativeOutboundTransformer = new AMQPNativeOutboundTransformer(idGenerator);
   }

   @Override
   public long transform(ServerJMSMessage msg, WritableBuffer buffer) throws JMSException, UnsupportedEncodingException {
      if (msg == null)
         return 0;
      if (msg.getBooleanProperty(JMS_AMQP_NATIVE)) {
         if (msg instanceof BytesMessage) {
            return nativeOutboundTransformer.transform(msg, buffer);
         } else {
            return 0;
         }
      } else {
         return transformer.transform(msg, buffer);
      }
   }
}
