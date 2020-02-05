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
package org.apache.activemq.artemis.cli.commands.user;

import java.util.Map;
import java.util.Set;

import io.airlift.airline.Command;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModuleConfigurator;

/**
 * list existing users, example:
 * ./artemis user list --user guest
 */
@Command(name = "list", description = "List existing user(s)")
public class ListUser extends UserAction {

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      list();

      return null;
   }

   /**
    * list a single user or all users
    * if username is not specified
    */
   private void list() throws Exception {
      PropertiesLoginModuleConfigurator config = new PropertiesLoginModuleConfigurator(entry, getBrokerEtc());
      Map<String, Set<String>> result = config.listUser(username);
      StringBuilder logMessage = new StringBuilder("--- \"user\"(roles) ---\n");
      int userCount = 0;
      for (Map.Entry<String, Set<String>> entry : result.entrySet()) {
         logMessage.append("\"").append(entry.getKey()).append("\"(");
         int roleCount = 0;
         for (String role : entry.getValue()) {
            logMessage.append(role);
            if (++roleCount < entry.getValue().size()) {
               logMessage.append(",");
            }
         }
         logMessage.append(")\n");
         userCount++;
      }
      logMessage.append("\n Total: ").append(userCount);
      context.out.println(logMessage);
   }

}
