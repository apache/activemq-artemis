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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.util.HashUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * Reset a user's password or roles, example:
 * ./artemis user reset --username guest --role admin --password ***
 */
@Command(name = "reset", description = "Reset user's password or roles")
public class ResetUser extends UserAction {

   @Option(name = "--password", description = "the password (Default: input)")
   String password;

   @Option(name = "--role", description = "user's role(s), comma separated")
   String role;

   @Option(name = "--plaintext", description = "using plaintext (Default false)")
   boolean plaintext = false;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      if (password != null) {
         password = plaintext ? password : HashUtil.tryHash(context, password);
      }

      String[] roles = null;
      if (role != null) {
         roles = StringUtils.split(role, ",");
      }

      reset(password, roles);
      return null;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public void setRole(String role) {
      this.role = role;
   }
}
