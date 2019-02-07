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

import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.InputAbstract;

public abstract class UserAction extends InputAbstract {

   @Option(name = "--role", description = "user's role(s), comma separated")
   String role;

   @Option(name = "--user", description = "The user name (Default: input)")
   String username = null;

   @Option(name = "--entry", description = "The appConfigurationEntry (default: activemq)")
   String entry = "activemq";

   void checkInputUser() {
      if (username == null) {
         username = input("--user", "Please provider the userName:", null);
      }
   }

   void checkInputRole() {
      if (role == null) {
         role = input("--role", "type a comma separated list of roles", null);
      }
   }

   public void setUsername(String username) {
      this.username = username;
   }

   public void setRole(String role) {
      this.role = role;
   }
}
