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
import org.apache.activemq.artemis.cli.commands.ActionContext;

@Command(name = "create", description = "create a queue or topic")
public class CreateAddress extends AddressAction {

   @Option(name = "--address", description = "address of the core queue (default queue's name)")
   String address;

   @Option(name = "--durable", description = "whether the queue is durable or not (default false)")
   boolean durable = false;

   @Option(name = "--autoCreateAddress", description = "auto create an address for this queue if one doesn't exist")
   boolean autoCreateAddress = true;

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
            ManagementHelper.putOperationInvocation(message, "broker", "createQueue", address, getName(), durable);
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
}
