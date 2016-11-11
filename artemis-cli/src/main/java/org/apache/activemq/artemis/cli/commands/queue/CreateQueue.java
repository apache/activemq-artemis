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

package org.apache.activemq.artemis.cli.commands.queue;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.AbstractAction;

@Command(name = "create", description = "create a queue or topic")
public class CreateQueue extends AbstractAction {

   @Option(name = "--name", description = "queue name")
   String name;

   @Option(name = "--filter", description = "queue's filter string (default null)")
   String filter = null;

   @Option(name = "--address", description = "address of the queue (default queue's name)")
   String address;

   @Option(name = "--durable", description = "whether the queue is durable or not (default false)")
   boolean durable = false;

   @Option(name = "--deleteOnNoConsumers", description = "whether to delete this queue when it's last consumers disconnects)")
   boolean deleteOnNoConsumers = false;

   @Option(name = "--maxConsumers", description = "Maximum number of consumers allowed on this queue at any one time (default no limit)")
   int maxConsumers = -1;

   @Option(name = "--autoCreateAddress", description = "Auto create the address (if it doesn't exist) with default values")
   boolean autoCreateAddress = false;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      createQueue(context);
      return null;
   }

   public String getAddress() {
      if (address == null || "".equals(address.trim())) {
         address = getName();
      }
      return address.trim();
   }

   private void createQueue(final ActionContext context) throws Exception {
      performCoreManagement(new ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            String address = getAddress();
            ManagementHelper.putOperationInvocation(message, "broker", "createQueue", address, getName(), filter, durable, maxConsumers, deleteOnNoConsumers, autoCreateAddress);
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            context.out.println("Core queue " + getName() + " created successfully.");
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to create queue " + getName() + ". Reason: " + errMsg);
         }
      });
   }

   public void setFilter(String filter) {
      this.filter = filter;
   }

   public void setAutoCreateAddress(boolean autoCreateAddress) {
      this.autoCreateAddress = autoCreateAddress;
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
}
