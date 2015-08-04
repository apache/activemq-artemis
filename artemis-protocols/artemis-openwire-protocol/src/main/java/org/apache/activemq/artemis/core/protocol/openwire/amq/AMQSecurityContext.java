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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.command.ActiveMQDestination;

public abstract class AMQSecurityContext {

   public static final AMQSecurityContext BROKER_SECURITY_CONTEXT = new AMQSecurityContext("ActiveMQBroker") {
      @Override
      public boolean isBrokerContext() {
         return true;
      }

      public Set<Principal> getPrincipals() {
         return Collections.emptySet();
      }
   };

   final String userName;

   final ConcurrentMap<ActiveMQDestination, ActiveMQDestination> authorizedReadDests = new ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination>();
   final ConcurrentMap<ActiveMQDestination, ActiveMQDestination> authorizedWriteDests = new ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination>();

   public AMQSecurityContext(String userName) {
      this.userName = userName;
   }

   public boolean isInOneOf(Set<?> allowedPrincipals) {
      Iterator<?> allowedIter = allowedPrincipals.iterator();
      HashSet<?> userPrincipals = new HashSet<Object>(getPrincipals());
      while (allowedIter.hasNext()) {
         Iterator<?> userIter = userPrincipals.iterator();
         Object allowedPrincipal = allowedIter.next();
         while (userIter.hasNext()) {
            if (allowedPrincipal.equals(userIter.next()))
               return true;
         }
      }
      return false;
   }

   public abstract Set<Principal> getPrincipals();

   public String getUserName() {
      return userName;
   }

   public ConcurrentMap<ActiveMQDestination, ActiveMQDestination> getAuthorizedReadDests() {
      return authorizedReadDests;
   }

   public ConcurrentMap<ActiveMQDestination, ActiveMQDestination> getAuthorizedWriteDests() {
      return authorizedWriteDests;
   }

   public boolean isBrokerContext() {
      return false;
   }

}
