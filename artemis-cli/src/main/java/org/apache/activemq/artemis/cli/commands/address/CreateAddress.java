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

package org.apache.activemq.artemis.cli.commands.address;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.cli.commands.ActionContext;

@Command(name = "create", description = "create an address")
public class CreateAddress extends AbstractAction {

   @Option(name = "--name", description = "The name of this address")
   String name;

   @Option(name = "--routingType", description = "The routing type of the address, options are 'anycast' or 'multicast', defaults to 1 = 'multicast'")
   String routingType = "multicast";

   @Option(name = "--defaultMaxConsumers", description = "Sets the default max consumers for any queues created under this address, default = -1 (no limit)")
   int defaultMaxConsumers = -1;

   @Option(name = "--defaultDeleteOnNoConsumers", description = "Sets the default delete on no consumers for any queues created under this address, default = false")
   boolean defaultDeleteOnNoConsumers = false;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      createAddress(context);
      return null;
   }

   private void createAddress(final ActionContext context) throws Exception {
      performCoreManagement(new ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            ManagementHelper.putOperationInvocation(message, "broker", "createAddress", getName(), routingType, defaultDeleteOnNoConsumers, defaultMaxConsumers);
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            context.out.println("Address " + getName() + " created successfully.");
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to create address " + getName() + ". Reason: " + errMsg);
         }
      });
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getName() {
      return name;
   }

   public String getRoutingType() {
      return routingType;
   }

   public void setRoutingType(String routingType) {
      this.routingType = routingType;
   }

   public int getDefaultMaxConsumers() {
      return defaultMaxConsumers;
   }

   public void setDefaultMaxConsumers(int defaultMaxConsumers) {
      this.defaultMaxConsumers = defaultMaxConsumers;
   }

   public boolean getDefaultDeleteOnNoConsumers() {
      return defaultDeleteOnNoConsumers;
   }

   public void setDefaultDeleteOnNoConsumers(boolean defaultDeleteOnNoConsumers) {
      this.defaultDeleteOnNoConsumers = defaultDeleteOnNoConsumers;
   }
}
