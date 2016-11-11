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

@Command(name = "delete", description = "delete a queue")
public class DeleteQueue extends AbstractAction {

   @Option(name = "--name", description = "queue name")
   String name;

   @Option(name = "--removeConsumers", description = "whether deleting destination with consumers or not (default false)")
   boolean removeConsumers = false;

   @Option(name = "--autoDeleteAddress", description = "delete the address if this it's last last queue")
   boolean autoDeleteAddress = false;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      deleteQueue(context);
      return null;
   }

   private void deleteQueue(final ActionContext context) throws Exception {
      performCoreManagement(new ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            ManagementHelper.putOperationInvocation(message, "broker", "destroyQueue", getName(), removeConsumers, autoDeleteAddress);
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            context.out.println("Queue " + getName() + " deleted successfully.");
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to delete queue " + getName() + ". Reason: " + errMsg);
         }
      });
   }

   public void setRemoveConsumers(boolean removeConsumers) {
      this.removeConsumers = removeConsumers;
   }

   public void setAutoDeleteAddress(boolean autoDeleteAddress) {
      this.autoDeleteAddress = autoDeleteAddress;
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
