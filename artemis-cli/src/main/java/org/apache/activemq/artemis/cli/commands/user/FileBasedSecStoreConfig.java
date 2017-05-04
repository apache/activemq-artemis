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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.utils.StringUtil;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;

class FileBasedSecStoreConfig {

   private static final String LICENSE_HEADER =
           "## ---------------------------------------------------------------------------\n" +
           "## Licensed to the Apache Software Foundation (ASF) under one or more\n" +
           "## contributor license agreements.  See the NOTICE file distributed with\n" +
           "## this work for additional information regarding copyright ownership.\n" +
           "## The ASF licenses this file to You under the Apache License, Version 2.0\n" +
           "## (the \"License\"); you may not use this file except in compliance with\n" +
           "## the License.  You may obtain a copy of the License at\n" +
           "##\n" +
           "## http://www.apache.org/licenses/LICENSE-2.0\n" +
           "##\n" +
           "## Unless required by applicable law or agreed to in writing, software\n" +
           "## distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
           "## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
           "## See the License for the specific language governing permissions and\n" +
           "## limitations under the License.\n" +
           "## ---------------------------------------------------------------------------\n";
   private FileBasedConfigurationBuilder<PropertiesConfiguration> userBuilder;
   private FileBasedConfigurationBuilder<PropertiesConfiguration> roleBuilder;
   private PropertiesConfiguration userConfig;
   private PropertiesConfiguration roleConfig;

   FileBasedSecStoreConfig(File userFile, File roleFile) throws Exception {
      Configurations configs = new Configurations();
      userBuilder = configs.propertiesBuilder(userFile);
      roleBuilder = configs.propertiesBuilder(roleFile);
      userConfig = userBuilder.getConfiguration();
      roleConfig = roleBuilder.getConfiguration();

      String roleHeader = roleConfig.getLayout().getHeaderComment();
      String userHeader = userConfig.getLayout().getHeaderComment();

      if (userHeader == null) {
         if (userConfig.isEmpty()) {
            //clean and reset header
            userConfig.clear();
            userConfig.setHeader(LICENSE_HEADER);
         }
      }

      if (roleHeader == null) {
         if (roleConfig.isEmpty()) {
            //clean and reset header
            roleConfig.clear();
            roleConfig.setHeader(LICENSE_HEADER);
         }
      }
   }

   void addNewUser(String username, String hash, String... roles) throws Exception {
      if (userConfig.getString(username) != null) {
         throw new IllegalArgumentException("User already exist: " + username);
      }
      userConfig.addProperty(username, hash);
      addRoles(username, roles);
   }

   void save() throws Exception {
      userBuilder.save();
      roleBuilder.save();
   }

   void removeUser(String username) throws Exception {
      if (userConfig.getProperty(username) == null) {
         throw new IllegalArgumentException("user " + username + " doesn't exist.");
      }
      userConfig.clearProperty(username);
      removeRoles(username);
   }

   List<String> listUser(String username) {
      List<String> result = new ArrayList<>();
      result.add("--- \"user\"(roles) ---\n");

      int totalUsers = 0;
      if (username != null) {
         String roles = findRoles(username);
         result.add("\"" + username + "\"(" + roles + ")");
         totalUsers++;
      } else {
         Iterator<String> iter = userConfig.getKeys();
         while (iter.hasNext()) {
            String keyUser = iter.next();
            String roles = findRoles(keyUser);
            result.add("\"" + keyUser + "\"(" + roles + ")");
            totalUsers++;
         }
      }
      result.add("\n Total: " + totalUsers);
      return result;
   }

   void updateUser(String username, String password, String[] roles) {
      String oldPassword = (String) userConfig.getProperty(username);
      if (oldPassword == null) {
         throw new IllegalArgumentException("user " + username + " doesn't exist.");
      }

      if (password != null) {
         userConfig.setProperty(username, password);
      }

      if (roles != null && roles.length > 0) {

         removeRoles(username);
         addRoles(username, roles);
      }
   }

   private String findRoles(String uname) {
      Iterator<String> iter = roleConfig.getKeys();
      StringBuilder builder = new StringBuilder();
      boolean first = true;
      while (iter.hasNext()) {
         String role = iter.next();
         List<String> names = roleConfig.getList(String.class, role);
         for (String value : names) {
            //each value may be a comma separated list
            String[] items = value.split(",");
            for (String item : items) {
               if (item.equals(uname)) {
                  if (!first) {
                     builder.append(",");
                  }
                  builder.append(role);
                  first = false;
               }
            }
         }
      }

      return builder.toString();
   }

   private void addRoles(String username, String[] roles) {
      for (String role : roles) {
         List<String> users = roleConfig.getList(String.class, role);
         if (users == null) {
            users = new ArrayList<>();
         }
         users.add(username);
         roleConfig.setProperty(role, StringUtil.joinStringList(users, ","));
      }
   }

   private void removeRoles(String username) {

      Iterator<String> iterKeys = roleConfig.getKeys();

      List<Pair<String, List<String>>> updateMap = new ArrayList<>();
      while (iterKeys.hasNext()) {
         String theRole = iterKeys.next();

         List<String> userList = roleConfig.getList(String.class, theRole);
         List<String> newList = new ArrayList<>();

         boolean roleChaned = false;
         for (String value : userList) {
            //each value may be comma separated.
            List<String> update = new ArrayList<>();
            String[] items = value.split(",");
            boolean found = false;
            for (String item : items) {
               if (!item.equals(username)) {
                  update.add(item);
               } else {
                  found = true;
                  roleChaned = true;
               }
            }
            if (found) {
               if (update.size() > 0) {
                  newList.add(StringUtil.joinStringList(update, ","));
               }
            }
         }
         if (roleChaned) {
            updateMap.add(new Pair(theRole, newList));
         }
      }
      //do update
      Iterator<Pair<String, List<String>>> iterUpdate = updateMap.iterator();
      while (iterUpdate.hasNext()) {
         Pair<String, List<String>> entry = iterUpdate.next();
         roleConfig.clearProperty(entry.getA());
         if (entry.getB().size() > 0) {
            roleConfig.addProperty(entry.getA(), entry.getB());
         }
      }
   }
}
