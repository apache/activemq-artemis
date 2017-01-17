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
package org.apache.activemq.artemis.core.protocol.openwire.util;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.util.ByteSequence;

public class OpenWireUtil {

   public static class OpenWireWildcardConfiguration extends WildcardConfiguration {
      public OpenWireWildcardConfiguration() {
         setDelimiter('.');
         setSingleWord('*');
         setAnyWords('>');
      }
   }

   public static final WildcardConfiguration OPENWIRE_WILDCARD = new OpenWireWildcardConfiguration();

   public static ActiveMQBuffer toActiveMQBuffer(ByteSequence bytes) {
      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(bytes.length);

      buffer.writeBytes(bytes.data, bytes.offset, bytes.length);
      return buffer;
   }

   /**
    * We convert the core address to an ActiveMQ Destination. We use the actual address on the message rather than the
    * destination set on the consumer because it maybe different and the JMS spec says that it should be what ever was
    * set on publish/send so a divert or wildcard may mean thats its different to the destination subscribed to by the
    * consumer
    */
   public static ActiveMQDestination toAMQAddress(ServerMessage message, ActiveMQDestination actualDestination) {
      String address = message.getAddress().toString();
      String strippedAddress = address;//.replace(JMS_QUEUE_ADDRESS_PREFIX, "").replace(JMS_TEMP_QUEUE_ADDRESS_PREFIX, "").replace(JMS_TOPIC_ADDRESS_PREFIX, "").replace(JMS_TEMP_TOPIC_ADDRESS_PREFIX, "");
      if (actualDestination.isQueue()) {
         return new ActiveMQQueue(strippedAddress);
      } else {
         return new ActiveMQTopic(strippedAddress);
      }
   }

   public static XidImpl toXID(TransactionId xaXid) {
      return toXID((XATransactionId) xaXid);
   }

   public static XidImpl toXID(XATransactionId xaXid) {
      return new XidImpl(xaXid.getBranchQualifier(), xaXid.getFormatId(), xaXid.getGlobalTransactionId());
   }
}
