/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.cli.commands.address;

import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.AbstractAction;

public abstract class AddressAbstract extends AbstractAction {

   @Option(name = "--name", description = "The name of this address")
   private String name;

   @Option(name = "--anycast", description = "It will determine this address as anycast")
   private Boolean anycast;

   @Option(name = "--no-anycast", description = "It will determine this address as anycast")
   private Boolean noAnycast;

   @Option(name = "--multicast", description = "It will determine this address as multicast")
   private Boolean multicast;

   @Option(name = "--no-multicast", description = "It will determine this address as multicast")
   private Boolean noMulticast;


   public AbstractAction setName(String name) {
      this.name = name;
      return this;
   }

   public String getName(boolean requireInput) {
      if (name == null && requireInput) {
         name = input("--name", "Provide the name of the address", null);
      }
      return name;
   }

   public String getRoutingTypes(boolean useDefault) {
      StringBuffer buffer = new StringBuffer();

      if (isAnycast()) {
         buffer.append("ANYCAST");
      }

      if (isMulticast()) {
         if (isAnycast()) {
            buffer.append(",");
         }
         buffer.append("MULTICAST");
      }

      if (!isAnycast() && !isMulticast()) {
         if (useDefault) {
            return "MULTICAST"; // the default;
         } else {
            return null;
         }

      }

      return buffer.toString();
   }

   public boolean isAnycast() {
      if (noAnycast != null) {
         anycast = !noAnycast.booleanValue();
      }
      if (anycast == null) {
         anycast = inputBoolean("--anycast", "Will this address support anycast queues", false);
      }
      return anycast;
   }

   public AddressAbstract setAnycast(boolean anycast) {
      this.anycast = anycast;
      return this;
   }

   public boolean isMulticast() {
      if (noMulticast != null) {
         multicast = !noMulticast.booleanValue();
      }
      if (multicast == null) {
         multicast = inputBoolean("--multicast", "Will this address support multicast queues", true);
      }
      return multicast;
   }

   public AddressAbstract setMulticast(boolean multicast) {
      this.multicast = multicast;
      return this;
   }
}
