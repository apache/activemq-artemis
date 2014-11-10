/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.protocol.openwire;


import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.ByteSequence;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;

public class OpenWireUtil
{

   public static HornetQBuffer toHornetQBuffer(ByteSequence bytes)
   {
      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(bytes.length);

      buffer.writeBytes(bytes.data, bytes.offset, bytes.length);
      return buffer;
   }


   public static SimpleString toCoreAddress(ActiveMQDestination dest)
   {
      if (dest.isQueue())
      {
         return new SimpleString("jms.queue." + dest.getPhysicalName());
      }
      else
      {
         return new SimpleString("jms.topic." + dest.getPhysicalName());
      }
   }

   /*
    *This util converts amq wildcards to compatible core wildcards
    *The conversion is like this:
    *AMQ * wildcard --> Core * wildcard (no conversion)
    *AMQ > wildcard --> Core # wildcard
    */
   public static String convertWildcard(String physicalName)
   {
      return physicalName.replaceAll("(\\.>)+", ".#");
   }

}
