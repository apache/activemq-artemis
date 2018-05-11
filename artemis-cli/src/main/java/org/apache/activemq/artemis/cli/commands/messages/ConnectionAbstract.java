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

package org.apache.activemq.artemis.cli.commands.messages;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.InputAbstract;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

public class ConnectionAbstract extends InputAbstract {

   @Option(name = "--url", description = "URL towards the broker. (default: tcp://localhost:61616)")
   protected String brokerURL = "tcp://localhost:61616";

   @Option(name = "--user", description = "User used to connect")
   protected String user;

   @Option(name = "--password", description = "Password used to connect")
   protected String password;

   @Option(name = "--clientID", description = "ClientID to be associated with connection")
   String clientID;

   @Option(name = "--protocol", description = "Protocol used. Valid values are amqp or core. Default=core.")
   String protocol = "core";

   protected ConnectionFactory createConnectionFactory() throws Exception {
      if (protocol.equals("core")) {
         return createCoreConnectionFactory();
      } else if (protocol.equals("amqp")) {
         return createAMQPConnectionFactory();
      } else {
         throw new IllegalStateException("protocol " + protocol + " not supported");
      }
   }

   private ConnectionFactory createAMQPConnectionFactory() {
      if (brokerURL.startsWith("tcp://")) {
         // replacing tcp:// by amqp://
         brokerURL = "amqp" + brokerURL.substring(3);
      }
      JmsConnectionFactory cf = new JmsConnectionFactory(user, password, brokerURL);
      if (clientID != null) {
         cf.setClientID(clientID);
      }

      try {
         Connection connection = cf.createConnection();
         connection.close();
         return cf;
      } catch (JMSSecurityException e) {
         // if a security exception will get the user and password through an input
         context.err.println("Connection failed::" + e.getMessage());
         userPassword();
         return new JmsConnectionFactory(user, password, brokerURL);
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         context.err.println("Connection failed::" + e.getMessage());
         brokerURL = input("--url", "Type in the broker URL for a retry (e.g. tcp://localhost:61616)", brokerURL);
         userPassword();
         return new JmsConnectionFactory(user, password, brokerURL);
      }
   }

   protected ActiveMQConnectionFactory createCoreConnectionFactory() {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, user, password);

      if (clientID != null) {
         System.out.println("Consumer:: clientID = " + clientID);
         cf.setClientID(clientID);
      }
      try {
         Connection connection = cf.createConnection();
         connection.close();
         return cf;
      } catch (JMSSecurityException e) {
         // if a security exception will get the user and password through an input
         context.err.println("Connection failed::" + e.getMessage());
         userPassword();
         return new ActiveMQConnectionFactory(brokerURL, user, password);
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         context.err.println("Connection failed::" + e.getMessage());
         brokerURL = input("--url", "Type in the broker URL for a retry (e.g. tcp://localhost:61616)", brokerURL);
         userPassword();
         return new ActiveMQConnectionFactory(brokerURL, user, password);
      }
   }

   private void userPassword() {
      if (user == null) {
         user = input("--user", "Type the username for a retry", null);
      }
      if (password == null) {
         password = inputPassword("--password", "Type the password for a retry", null);
      }
   }

}
