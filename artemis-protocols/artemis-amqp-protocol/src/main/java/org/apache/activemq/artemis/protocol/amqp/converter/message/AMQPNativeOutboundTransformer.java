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

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.message.ProtonJMessage;

public class AMQPNativeOutboundTransformer extends OutboundTransformer {

   public AMQPNativeOutboundTransformer(JMSVendor vendor) {
      super(vendor);
   }

   public static ProtonJMessage transform(OutboundTransformer options, BytesMessage msg) throws JMSException {
      byte[] data = new byte[(int) msg.getBodyLength()];
      msg.readBytes(data);
      msg.reset();
      int count = msg.getIntProperty("JMSXDeliveryCount");

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

      // Update the DeliveryCount header...
      // The AMQP delivery-count field only includes prior failed delivery attempts,
      // whereas JMSXDeliveryCount includes the first/current delivery attempt. Subtract 1.
      if (amqp.getHeader() == null) {
         amqp.setHeader(new Header());
      }

      amqp.getHeader().setDeliveryCount(new UnsignedInteger(count - 1));

      return amqp;
   }
}
