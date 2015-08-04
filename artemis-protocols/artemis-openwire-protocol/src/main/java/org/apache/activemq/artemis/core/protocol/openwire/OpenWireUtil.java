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
package org.apache.activemq.artemis.core.protocol.openwire;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQServerSession;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSession;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.artemis.api.core.SimpleString;

public class OpenWireUtil {

   public static ActiveMQBuffer toActiveMQBuffer(ByteSequence bytes) {
      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(bytes.length);

      buffer.writeBytes(bytes.data, bytes.offset, bytes.length);
      return buffer;
   }

   public static SimpleString toCoreAddress(ActiveMQDestination dest) {
      if (dest.isQueue()) {
         return new SimpleString("jms.queue." + dest.getPhysicalName());
      }
      else {
         return new SimpleString("jms.topic." + dest.getPhysicalName());
      }
   }

   /**
    * Checks to see if this destination exists.  If it does not throw an invalid destination exception.
    *
    * @param destination
    * @param amqSession
    */
   public static void validateDestination(ActiveMQDestination destination, AMQSession amqSession) throws Exception {
      if (destination.isQueue()) {
         AMQServerSession coreSession = amqSession.getCoreSession();
         SimpleString physicalName = OpenWireUtil.toCoreAddress(destination);
         BindingQueryResult result = coreSession.executeBindingQuery(physicalName);
         if (!result.isExists() && !result.isAutoCreateJmsQueues()) {
            throw ActiveMQMessageBundle.BUNDLE.noSuchQueue(physicalName);
         }
      }
   }

   /*
    *This util converts amq wildcards to compatible core wildcards
    *The conversion is like this:
    *AMQ * wildcard --> Core * wildcard (no conversion)
    *AMQ > wildcard --> Core # wildcard
    */
   public static String convertWildcard(String physicalName) {
      return physicalName.replaceAll("(\\.>)+", ".#");
   }

}
