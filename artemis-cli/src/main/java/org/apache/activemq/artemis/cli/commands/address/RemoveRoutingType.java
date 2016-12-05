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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.cli.commands.ActionContext;

@Command(name = "removeRoutingType", description = "remove the provided routing types from an address")
public class RemoveRoutingType extends AbstractAction {

   @Option(name = "--name", description = "The name of the address", required = true)
   String name;

   @Option(name = "--routingType", description = "The routing types to be removed from this address, options are 'anycast' or 'multicast'", required = true)
   String routingType;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      removeRoutingType(context);
      return null;
   }

   private void removeRoutingType(final ActionContext context) throws Exception {
      performCoreManagement(new AbstractAction.ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            ManagementHelper.putOperationInvocation(message, "broker", "removeRoutingType", name, routingType);
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            context.out.println("Address " + name + " updated successfully.");
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to update address " + name + ". Reason: " + errMsg);
         }
      });
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getRoutingType() {
      return routingType;
   }

   public void setRoutingType(String routingType) {
      this.routingType = routingType;
   }
}
