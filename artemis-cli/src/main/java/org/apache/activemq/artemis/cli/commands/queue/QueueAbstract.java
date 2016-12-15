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

package org.apache.activemq.artemis.cli.commands.queue;

import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.AbstractAction;

public class QueueAbstract extends AbstractAction {

   @Option(name = "--name", description = "queue name")
   private String name;

   @Option(name = "--filter", description = "queue's filter string (default null)")
   private String filter = null;

   @Option(name = "--address", description = "address of the queue (default queue's name)")
   private String address;

   @Option(name = "--durable", description = "whether the queue is durable or not (default false)")
   private boolean durable = false;

   @Option(name = "--delete-on-no-consumers", description = "whether to delete this queue when it's last consumers disconnects)")
   private boolean deleteOnNoConsumers = false;

   @Option(name = "--keep-on-no-consumers", description = "whether to queue this queue when it's last consumers disconnects)")
   private boolean keepOnNoConsumers = false;

   @Option(name = "--max-consumers", description = "Maximum number of consumers allowed on this queue at any one time (default no limit)")
   private int maxConsumers = -1;

   @Option(name = "--auto-create-ddress", description = "Auto create the address (if it doesn't exist) with default values")
   private Boolean autoCreateAddress = false;

   @Option(name = "--anycast", description = "It will determine this queue as anycast")
   private Boolean anycast;

   @Option(name = "--multicast", description = "It will determine this queue as multicast")
   private Boolean multicast;

   public void setFilter(String filter) {
      this.filter = filter;
   }

   public String getFilter() {
      return filter;
   }

   public String getAddress() {
      if (address == null || "".equals(address.trim())) {
         address = getName();
      }
      return address;
   }

   public boolean isDurable() {
      return durable;
   }

   public QueueAbstract setDurable(boolean durable) {
      this.durable = durable;
      return this;
   }

   public boolean isDeleteOnNoConsumers() {
      return deleteOnNoConsumers;
   }

   public boolean isKeepOnNoConsumers() {
      return keepOnNoConsumers;
   }

   public QueueAbstract setKeepOnNoConsumers(boolean keepOnNoConsumers) {
      this.keepOnNoConsumers = keepOnNoConsumers;
      return this;
   }

   public int getMaxConsumers() {
      return maxConsumers;
   }

   public boolean isAutoCreateAddress() {
      if (autoCreateAddress == null) {
         autoCreateAddress = inputBoolean("--auto-create-address", "should auto create the address if it doesn't exist", false);
      }
      return autoCreateAddress;
   }

   public QueueAbstract setAutoCreateAddress(boolean autoCreateAddress) {
      this.autoCreateAddress = autoCreateAddress;
      return this;
   }

   public boolean isAnycast() {
      if (anycast == null) {
         if (multicast != null) {
            // if multicast is not null, it should be the opposite
            anycast = !multicast.booleanValue();
         }

         if (anycast == null) {
            // if it is still null
            anycast = inputBoolean("--anycast", "is this an anycast queue", false);
         }
      }
      return anycast;
   }

   public QueueAbstract setAnycast(boolean anycast) {
      this.anycast = anycast;
      return this;
   }

   public boolean isMulticast() {
      if (multicast == null) {
         if (anycast != null) {
            // if anycast is not null, it should be the opposite
            multicast = !anycast.booleanValue();
         }

         if (multicast == null) {
            // if it is still null
            multicast = inputBoolean("--multicast", "is this a multicast queue", false);
         }
      }
      return multicast;
   }

   public QueueAbstract setMulticast(boolean multicast) {
      this.multicast = multicast;
      return this;
   }

   public Boolean treatNoConsumers(boolean mandatory) {

      Boolean value = null;

      if (deleteOnNoConsumers) {
         value = Boolean.TRUE;
      } else if (keepOnNoConsumers) {
         value = Boolean.FALSE;
      }


      if (value == null && mandatory) {
         value = Boolean.FALSE;
         deleteOnNoConsumers = false;
         keepOnNoConsumers = true;
      }

      return value;
   }

   public void setMaxConsumers(int maxConsumers) {
      this.maxConsumers = maxConsumers;
   }

   public void setDeleteOnNoConsumers(boolean deleteOnNoConsumers) {
      this.deleteOnNoConsumers = deleteOnNoConsumers;
   }

   public void setAddress(String address) {
      this.address = address;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getName() {
      if (name == null) {
         name = input("--name", "Please provide the destination name:", "");
      }

      return name;
   }

   public String getRoutingType() {
      if (isAnycast() && isMulticast()) {
         throw new IllegalArgumentException("--multicast and --anycast are exclusive options for a Queue");
      }

      if (isMulticast()) {
         return "MULTICAST";
      } else if (anycast) {
         return "ANYCAST";
      } else {
         return null;
      }
   }


}
