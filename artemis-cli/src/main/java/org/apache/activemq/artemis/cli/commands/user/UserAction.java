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
import org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule;
import org.apache.activemq.artemis.util.FileBasedSecStoreConfig;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.util.List;

import static org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule.ROLE_FILE_PROP_NAME;
import static org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule.USER_FILE_PROP_NAME;

public abstract class UserAction extends InputAbstract {

   @Option(name = "--user", description = "The user name")
   String username = null;

   /**
    * Adding a new user
    * @param hash the password
    * @param role the role
    * @throws IllegalArgumentException if user exists
    */
   protected void add(String hash, String... role) throws Exception {
      FileBasedSecStoreConfig config = getConfiguration();
      config.addNewUser(username, hash, role);
      config.save();
      context.out.println("User added successfully.");
   }

   /**
    * list a single user or all users
    * if username is not specified
    */
   protected void list() throws Exception {
      FileBasedSecStoreConfig config = getConfiguration();
      List<String> result = config.listUser(username);
      for (String str : result) {
         context.out.println(str);
      }
   }

   protected void remove() throws Exception {
      FileBasedSecStoreConfig config = getConfiguration();
      config.removeUser(username);
      config.save();
      context.out.println("User removed.");
   }

   protected void reset(String password, String[] roles) throws Exception {
      if (password == null && roles == null) {
         context.err.println("Nothing to update.");
         return;
      }
      FileBasedSecStoreConfig config = getConfiguration();
      config.updateUser(username, password, roles);
      config.save();
      context.out.println("User updated");
   }

   private FileBasedSecStoreConfig getConfiguration() throws Exception {

      Configuration securityConfig = Configuration.getConfiguration();
      AppConfigurationEntry[] entries = securityConfig.getAppConfigurationEntry("activemq");

      for (AppConfigurationEntry entry : entries) {
         if (entry.getLoginModuleName().equals(PropertiesLoginModule.class.getName())) {
            String userFileName = (String) entry.getOptions().get(USER_FILE_PROP_NAME);
            String roleFileName = (String) entry.getOptions().get(ROLE_FILE_PROP_NAME);

            File etcDir = new File(getBrokerInstance(), "etc");
            File userFile = new File(etcDir, userFileName);
            File roleFile = new File(etcDir, roleFileName);

            if (!userFile.exists() || !roleFile.exists()) {
               throw new IllegalArgumentException("Couldn't find user file or role file!");
            }

            return new FileBasedSecStoreConfig(userFile, roleFile);
         }
      }
      throw new IllegalArgumentException("Failed to load security file");
   }

   public void setUsername(String username) {
      this.username = username;
   }
}
