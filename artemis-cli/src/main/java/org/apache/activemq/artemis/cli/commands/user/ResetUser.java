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
public class ResetUser extends PasswordAction {

   @Option(name = "--plaintext", description = "using plaintext (Default false)")
   boolean plaintext = false;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      checkInputUser();
      checkInputPassword();

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

   private void reset(String password, String[] roles) throws Exception {
      if (password == null && roles == null) {
         context.err.println("Nothing to update.");
         return;
      }
      FileBasedSecStoreConfig config = getConfiguration();
      config.updateUser(username, password, roles);
      config.save();
      context.out.println("User updated");
   }
}
