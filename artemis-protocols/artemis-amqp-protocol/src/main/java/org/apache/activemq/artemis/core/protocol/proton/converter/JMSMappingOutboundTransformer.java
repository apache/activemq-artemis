/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.proton.converter;

import org.apache.activemq.transport.amqp.message.JMSVendor;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.message.ProtonJMessage;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.UnsupportedEncodingException;
import java.util.Map;

class JMSMappingOutboundTransformer extends org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer {
   JMSMappingOutboundTransformer(JMSVendor vendor) {
      super(vendor);
   }

   @Override
   public ProtonJMessage convert(Message msg) throws JMSException, UnsupportedEncodingException {
      ProtonJMessage protonJMessage = super.convert(msg);

      Map properties = protonJMessage.getApplicationProperties().getValue();

      if (properties.containsKey(this.getPrefixVendor() + "NATIVE_LONG_MESSAGE_ID")) {
         Long id = (Long) properties.remove(this.getPrefixVendor() + "NATIVE_LONG_MESSAGE_ID");
         protonJMessage.setMessageId(id);
      }
      else if (properties.containsKey(this.getPrefixVendor() + "NATIVE_UNSIGNED_LONG_MESSAGE_ID")) {
         Long id = (Long) properties.remove(this.getPrefixVendor() + "NATIVE_UNSIGNED_LONG_MESSAGE_ID");
         protonJMessage.setMessageId(new UnsignedLong(id));
      }
      else if (properties.containsKey(this.getPrefixVendor() + "NATIVE_STRING_MESSAGE_ID")) {
         String id = (String) properties.remove(this.getPrefixVendor() + "NATIVE_STRING_MESSAGE_ID");
         protonJMessage.setMessageId(id);
      }
      return protonJMessage;
   }
}
