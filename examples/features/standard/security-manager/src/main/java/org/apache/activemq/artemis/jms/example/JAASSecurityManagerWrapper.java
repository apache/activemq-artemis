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

import javax.security.auth.Subject;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager5;

public class JAASSecurityManagerWrapper implements ActiveMQSecurityManager5 {
   ActiveMQJAASSecurityManager activeMQJAASSecurityManager;

   @Override
   public Subject authenticate(String user, String password, RemotingConnection remotingConnection, String securityDomain) {
      System.out.println("authenticate(" + user + ", " + password + ", " + remotingConnection.getRemoteAddress() + ", " + securityDomain + ")");
      return activeMQJAASSecurityManager.authenticate(user, password, remotingConnection, securityDomain);
   }

   @Override
   public boolean authorize(Subject subject,
                            Set<Role> roles,
                            CheckType checkType,
                            String address) {
      System.out.println("authorize(" + subject + ", " + roles + ", " + checkType + ", " + address + ")");
      return activeMQJAASSecurityManager.authorize(subject, roles, checkType, address);
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
