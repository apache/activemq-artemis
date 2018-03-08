/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.management;

import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JMXAccessControlList {

   private Access defaultAccess = new Access("*");
   private Map<String, Access> domainAccess = new HashMap<>();
   private ConcurrentHashMap<String, List<String>> whitelist = new ConcurrentHashMap<>();


   public void addToWhiteList(String domain, String match) {
      List<String> list = new ArrayList<>();
      list = whitelist.putIfAbsent(domain, list);
      if (list == null) {
         list = whitelist.get(domain);
      }
      list.add(match != null ? match : "*");
   }


   public List<String> getRolesForObject(ObjectName objectName, String methodName) {
      Hashtable<String, String> keyPropertyList = objectName.getKeyPropertyList();
      for (Map.Entry<String, String> keyEntry : keyPropertyList.entrySet()) {
         String key = keyEntry.getKey() + "=" + keyEntry.getValue();
         Access access = domainAccess.get(getObjectID(objectName.getDomain(), key));
         if (access != null) {
            return access.getMatchingRolesForMethod(methodName);
         }
      }
      for (Map.Entry<String, String> keyEntry : keyPropertyList.entrySet()) {
         String key = keyEntry.getKey() + "=*";
         Access access = domainAccess.get(getObjectID(objectName.getDomain(), key));
         if (access != null) {
            return access.getMatchingRolesForMethod(methodName);
         }
      }
      Access access = domainAccess.get(objectName.getDomain());
      if (access == null) {
         access = defaultAccess;
      }
      return access.getMatchingRolesForMethod(methodName);
   }

   public boolean isInWhiteList(ObjectName objectName) {
      List<String> matches = whitelist.get(objectName.getDomain());
      if (matches != null) {
         for (String match : matches) {
            if (match.equals("*")) {
               return true;
            } else {
               String[] split = match.split("=");
               String key = split[0];
               String val = split[1];
               String propVal = objectName.getKeyProperty(key);
               if (propVal != null && (val.equals("*") || propVal.equals(val))) {
                  return true;
               }
            }
         }
      }
      return false;
   }

   public void addToDefaultAccess(String method, String... roles) {
      if (roles != null) {
         if ( method.equals("*")) {
            defaultAccess.addCatchAll(roles);
         } else if (method.endsWith("*")) {
            String prefix = method.replace("*", "");
            defaultAccess.addMethodsPrefixes(prefix, roles);
         } else {
            defaultAccess.addMethods(method, roles);
         }
      }
   }

   public void addToRoleAccess(String domain,String key, String method, String... roles) {
      String id = getObjectID(domain, key);
      Access access = domainAccess.get(id);
      if (access == null) {
         access = new Access(domain);
         domainAccess.put(id, access);
      }

      if (method.equals("*")) {
         access.addCatchAll(roles);
      } else if (method.endsWith("*")) {
         String prefix = method.replace("*", "");
         access.addMethodsPrefixes(prefix, roles);
      } else {
         access.addMethods(method, roles);
      }
   }

   private String getObjectID(String domain, String key) {
      String id = domain;

      if (key != null) {
         String actualKey = key;
         if (key.endsWith("\"")) {
            actualKey = actualKey.replace("\"", "");
         }
         id += ":" + actualKey;
      }
      return id;
   }

   static class Access {
      private final String domain;
      List<String> catchAllRoles = new ArrayList<>();
      Map<String, List<String>> methodRoles = new HashMap<>();
      Map<String, List<String>> methodPrefixRoles = new LinkedHashMap<>();

      Access(String domain) {
         this.domain = domain;
      }

      public synchronized void addMethods(String prefix, String... roles) {
         List<String> rolesList = methodRoles.get(prefix);
         if (rolesList == null) {
            rolesList = new ArrayList<>();
            methodRoles.put(prefix, rolesList);
         }
         for (String role : roles) {
            rolesList.add(role);
         }
      }

      public synchronized void addMethodsPrefixes(String prefix, String... roles) {
         List<String> rolesList = methodPrefixRoles.get(prefix);
         if (rolesList == null) {
            rolesList = new ArrayList<>();
            methodPrefixRoles.put(prefix, rolesList);
         }
         for (String role : roles) {
            rolesList.add(role);
         }
      }

      public void addCatchAll(String... roles) {
         for (String role : roles) {
            catchAllRoles.add(role);
         }
      }

      public String getDomain() {
         return domain;
      }

      public List<String> getMatchingRolesForMethod(String methodName) {
         List<String> roles = methodRoles.get(methodName);
         if (roles != null) {
            return roles;
         }
         for (Map.Entry<String, List<String>> entry : methodPrefixRoles.entrySet()) {
            if (methodName.startsWith(entry.getKey())) {
               return entry.getValue();
            }
         }
         return catchAllRoles;
      }
   }
   public static JMXAccessControlList createDefaultList() {
      JMXAccessControlList accessControlList = new JMXAccessControlList();

      accessControlList.addToWhiteList("hawtio", "type=*");

      accessControlList.addToRoleAccess("org.apache.activemq.artemis", null, "list*", "view", "update", "amq");
      accessControlList.addToRoleAccess("org.apache.activemq.artemis", null,"get*", "view", "update", "amq");
      accessControlList.addToRoleAccess("org.apache.activemq.artemis", null,"is*", "view", "update", "amq");
      accessControlList.addToRoleAccess("org.apache.activemq.artemis", null,"set*","update", "amq");
      accessControlList.addToRoleAccess("org.apache.activemq.artemis", null,"*", "amq");

      accessControlList.addToDefaultAccess("list*", "view", "update", "amq");
      accessControlList.addToDefaultAccess("get*", "view", "update", "amq");
      accessControlList.addToDefaultAccess("is*", "view", "update", "amq");
      accessControlList.addToDefaultAccess("set*", "update", "amq");
      accessControlList.addToDefaultAccess("*", "amq");

      return accessControlList;
   }
}
