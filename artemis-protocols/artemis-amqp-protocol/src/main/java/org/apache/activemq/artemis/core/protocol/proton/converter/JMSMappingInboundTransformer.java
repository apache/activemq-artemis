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
import org.apache.qpid.proton.amqp.messaging.Properties;

import javax.jms.Message;

class JMSMappingInboundTransformer extends org.apache.activemq.transport.amqp.message.JMSMappingInboundTransformer {

   JMSMappingInboundTransformer(JMSVendor vendor) {
      super(vendor);
   }

   @Override
   protected void populateMessage(Message jms, org.apache.qpid.proton.message.Message amqp) throws Exception {
      super.populateMessage(jms, amqp);
      final Properties properties = amqp.getProperties();
      if (properties != null) {
         if (properties.getMessageId() != null) {
            if (properties.getMessageId() instanceof Long) {
               jms.setLongProperty(this.getPrefixVendor() + "NATIVE_LONG_MESSAGE_ID", (Long) properties.getMessageId());
            }
            else if (properties.getMessageId() instanceof UnsignedLong) {
               jms.setLongProperty(this.getPrefixVendor() + "NATIVE_UNSIGNED_LONG_MESSAGE_ID", ((UnsignedLong) properties.getMessageId()).longValue());
            }
            else {
               jms.setStringProperty(this.getPrefixVendor() + "NATIVE_STRING_MESSAGE_ID", properties.getMessageId().toString());
            }
         }
      }
   }
}
