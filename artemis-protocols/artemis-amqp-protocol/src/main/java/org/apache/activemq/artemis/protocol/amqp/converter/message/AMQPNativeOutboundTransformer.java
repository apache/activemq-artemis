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

import java.io.UnsupportedEncodingException;

import javax.jms.JMSException;

import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.ProtonJMessage;

public class AMQPNativeOutboundTransformer extends OutboundTransformer {

   public AMQPNativeOutboundTransformer(IDGenerator idGenerator) {
      super(idGenerator);
   }

   @Override
   public long transform(ServerJMSMessage message, WritableBuffer buffer) throws JMSException, UnsupportedEncodingException {
      if (message == null || !(message instanceof ServerJMSBytesMessage)) {
         return 0;
      }

      return transform(this, (ServerJMSBytesMessage) message, buffer);
   }

   public static long transform(OutboundTransformer options, ServerJMSBytesMessage message, WritableBuffer buffer) throws JMSException {
      byte[] data = new byte[(int) message.getBodyLength()];
      message.readBytes(data);
      message.reset();

      // The AMQP delivery-count field only includes prior failed delivery attempts,
      int amqpDeliveryCount = message.getDeliveryCount() - 1;
      if (amqpDeliveryCount >= 1) {

         // decode...
         ProtonJMessage amqp = (ProtonJMessage) org.apache.qpid.proton.message.Message.Factory.create();
         int offset = 0;
         int len = data.length;
         while (len > 0) {
            final int decoded = amqp.decode(data, offset, len);
            assert decoded > 0 : "Make progress decoding the message";
            offset += decoded;
            len -= decoded;
         }

         // Update the DeliveryCount header which might require adding a Header
         if (amqp.getHeader() == null && amqpDeliveryCount > 0) {
            amqp.setHeader(new Header());
         }

         amqp.getHeader().setDeliveryCount(new UnsignedInteger(amqpDeliveryCount));

         amqp.encode(buffer);
      } else {
         buffer.put(data, 0, data.length);
      }

      return 0;
   }
}
