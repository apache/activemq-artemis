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

package org.apache.activemq.artemis.jms.example;

import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager3;

public class JAASSecurityManagerWrapper implements ActiveMQSecurityManager3 {
   ActiveMQJAASSecurityManager activeMQJAASSecurityManager;

   @Override
   public String validateUser(String user, String password, RemotingConnection remotingConnection) {
      System.out.println("validateUser(" + user + ", " + password + ", " + remotingConnection.getRemoteAddress() + ")");
      return activeMQJAASSecurityManager.validateUser(user, password, remotingConnection);
   }


   @Override
   public String validateUserAndRole(String user,
                              String password,
                              Set<Role> roles,
                              CheckType checkType,
                              String address,
                              RemotingConnection remotingConnection) {
      System.out.println("validateUserAndRole(" + user + ", " + password + ", " + roles + ", " + checkType + ", " + address + ", " + remotingConnection.getRemoteAddress() + ")");
      return activeMQJAASSecurityManager.validateUserAndRole(user, password, roles, checkType, address, remotingConnection);
   }

   @Override
   public String getDomain() {
      return activeMQJAASSecurityManager.getDomain();
   }

   @Override
   public boolean validateUser(String user, String password) {
      return activeMQJAASSecurityManager.validateUser(user, password);
   }

   @Override
   public boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType) {
      return activeMQJAASSecurityManager.validateUserAndRole(user, password, roles, checkType);
   }

   @Override
   public ActiveMQSecurityManager init(Map<String, String> properties) {
      activeMQJAASSecurityManager = new ActiveMQJAASSecurityManager(properties.get("domain"));
      return this;
   }

}
