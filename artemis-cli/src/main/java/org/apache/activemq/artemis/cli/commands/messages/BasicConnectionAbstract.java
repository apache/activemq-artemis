/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.cli.commands.messages;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class BasicConnectionAbstract extends ConnectionConfigurationAbtract {

   protected ConnectionFactory createConnectionFactory() throws Exception {
      recoverConnectionInformation();
      return createConnectionFactory(brokerURL, user, password);
   }

   protected ConnectionFactory createConnectionFactory(String brokerURL,
                                                               String user,
                                                               String password) throws Exception {
      recoverConnectionInformation();

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, user, password);
      try {
         tryConnect(brokerURL, user, password, cf);
         return cf;
      } catch (JMSSecurityException e) {
         // if a security exception will get the user and password through an input
         if (getActionContext() != null) {
            getActionContext().err.println("Connection failed::" + e.getMessage());
         }
         user = inputUser(user);
         password = inputPassword(password);
         cf = new ActiveMQConnectionFactory(brokerURL, user, password);
         try {
            tryConnect(brokerURL, user, password, cf);
         } catch (Exception e2) {
         }
         return cf;
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         if (getActionContext() != null) {
            getActionContext().err.println("Connection failed::" + e.getMessage());
         }
         brokerURL = inputBrokerURL(brokerURL);
         user = inputUser(user);
         password = inputPassword(password);
         cf = new ActiveMQConnectionFactory(brokerURL, user, password);
         try {
            tryConnect(brokerURL, user, password, cf);
         } catch (Exception e2) {
         }
         return cf;
      }
   }

   protected void tryConnect(String brokerURL,
                          String user,
                          String password,
                          ConnectionFactory cf) throws JMSException {
      Connection connection = cf.createConnection();
      connection.close();
      saveConnectionInfo(brokerURL, user, password);
   }

   protected void performCoreManagement(ManagementHelper.MessageAcceptor setup, ManagementHelper.MessageAcceptor ok, ManagementHelper.MessageAcceptor failed) throws Exception {
      try (ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) createConnectionFactory()) {
         ManagementHelper.doManagement(factory.getServerLocator(), user, password, setup, ok, failed);
      }
   }

   protected void performCoreManagement(String uri, String user, String password, ManagementHelper.MessageAcceptor setup, ManagementHelper.MessageAcceptor ok, ManagementHelper.MessageAcceptor failed) throws Exception {
      try (ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) createConnectionFactory(uri, user, password)) {
         ManagementHelper.doManagement(factory.getServerLocator(), user, password, setup, ok, failed);
      }
   }
}