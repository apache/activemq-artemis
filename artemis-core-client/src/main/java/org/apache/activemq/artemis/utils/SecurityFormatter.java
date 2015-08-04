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
package org.apache.activemq.artemis.utils;

import org.apache.activemq.artemis.core.security.Role;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SecurityFormatter {

   public static Set<Role> createSecurity(String sendRoles,
                                          String consumeRoles,
                                          String createDurableQueueRoles,
                                          String deleteDurableQueueRoles,
                                          String createNonDurableQueueRoles,
                                          String deleteNonDurableQueueRoles,
                                          String manageRoles) {
      List<String> createDurableQueue = toList(createDurableQueueRoles);
      List<String> deleteDurableQueue = toList(deleteDurableQueueRoles);
      List<String> createNonDurableQueue = toList(createNonDurableQueueRoles);
      List<String> deleteNonDurableQueue = toList(deleteNonDurableQueueRoles);
      List<String> send = toList(sendRoles);
      List<String> consume = toList(consumeRoles);
      List<String> manage = toList(manageRoles);

      Set<String> allRoles = new HashSet<String>();
      allRoles.addAll(createDurableQueue);
      allRoles.addAll(deleteDurableQueue);
      allRoles.addAll(createNonDurableQueue);
      allRoles.addAll(deleteNonDurableQueue);
      allRoles.addAll(send);
      allRoles.addAll(consume);
      allRoles.addAll(manage);

      Set<Role> roles = new HashSet<Role>(allRoles.size());
      for (String role : allRoles) {
         roles.add(new Role(role, send.contains(role), consume.contains(role), createDurableQueue.contains(role), deleteDurableQueue.contains(role), createNonDurableQueue.contains(role), deleteNonDurableQueue.contains(role), manageRoles.contains(role)));
      }
      return roles;
   }

   private static List<String> toList(final String commaSeparatedString) {
      List<String> list = new ArrayList<String>();
      if (commaSeparatedString == null || commaSeparatedString.trim().length() == 0) {
         return list;
      }
      String[] values = commaSeparatedString.split(",");
      for (int i = 0; i < values.length; i++) {
         list.add(values[i].trim());
      }
      return list;
   }
}
