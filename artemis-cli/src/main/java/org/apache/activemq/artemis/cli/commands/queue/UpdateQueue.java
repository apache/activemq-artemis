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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.cli.commands.ActionContext;

@Command(name = "update", description = "update a core queue")
public class UpdateQueue extends AbstractAction {

   @Option(name = "--name", description = "name", required = true)
   String name;

   @Option(name = "--deleteOnNoConsumers", description = "whether to delete when it's last consumers disconnects)")
   Boolean deleteOnNoConsumers = null;

   @Option(name = "--maxConsumers", description = "Maximum number of consumers allowed at any one time")
   Integer maxConsumers = null;

   @Option(name = "--routingType", description = "The routing type supported by this queue, options are 'anycast' or 'multicast'")
   String routingType = null;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      createQueue(context);
      return null;
   }

   private void createQueue(final ActionContext context) throws Exception {
      performCoreManagement(new ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            ManagementHelper.putOperationInvocation(message, "broker", "updateQueue", name, routingType, maxConsumers, deleteOnNoConsumers);
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            final String result = ManagementHelper.getResult(reply, String.class) + " updated successfully.";
            context.out.println(result);
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to update " + name + ". Reason: " + errMsg);
         }
      });
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public Boolean getDeleteOnNoConsumers() {
      return deleteOnNoConsumers;
   }

   public void setDeleteOnNoConsumers(boolean deleteOnNoConsumers) {
      this.deleteOnNoConsumers = deleteOnNoConsumers;
   }

   public Integer getMaxConsumers() {
      return maxConsumers;
   }

   public void setMaxConsumers(int maxConsumers) {
      this.maxConsumers = maxConsumers;
   }

   public String getRoutingType() {
      return routingType;
   }

   public void setRoutingType(String routingType) {
      this.routingType = routingType;
   }
}

