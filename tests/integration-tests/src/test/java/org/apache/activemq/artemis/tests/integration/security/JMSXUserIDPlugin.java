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
package org.apache.activemq.artemis.tests.integration.security;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;

import java.util.Map;

public class JMSXUserIDPlugin implements ActiveMQServerPlugin {

   private static String POPULATE_VALIDATED_USER = "POPULATE_VALIDATED_USER";

   private String populateValidatedUser;

   /**
    * used to pass configured properties to Plugin
    *
    * @param properties
    */
   @Override
   public void init(Map<String, String> properties) {
      populateValidatedUser = properties.getOrDefault(POPULATE_VALIDATED_USER, null);
   }

   @Override
   public void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {
      if (populateValidatedUser != null && !message.containsProperty(Message.HDR_VALIDATED_USER)) {
         message.messageChanged();
         message.putStringProperty(Message.HDR_VALIDATED_USER, populateValidatedUser);
         message.reencode();
      }
   }

   public String getPopulateValidatedUser() {
      return populateValidatedUser;
   }

   public void setPopulateValidatedUser(String populateValidatedUser) {
      this.populateValidatedUser = populateValidatedUser;
   }
}
