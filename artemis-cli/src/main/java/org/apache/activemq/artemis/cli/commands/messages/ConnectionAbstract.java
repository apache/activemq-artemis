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
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.InputAbstract;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

public class ConnectionAbstract extends InputAbstract {

   @Option(name = "--url", description = "URL towards the broker. (default: Build URL from acceptors defined in the broker.xml or tcp://localhost:61616 if the default cannot be parsed)")
   protected String brokerURL = DEFAULT_BROKER_URL;

   @Option(name = "--acceptor", description = "Acceptor used to build URL towards the broker")
   protected String acceptor;

   @Option(name = "--user", description = "User used to connect")
   protected String user;

   @Option(name = "--password", description = "Password used to connect")
   protected String password;

   @Option(name = "--clientID", description = "ClientID to be associated with connection")
   protected String clientID;

   @Option(name = "--protocol", description = "Protocol used. Valid values are amqp or core. Default=core.")
   protected String protocol = "core";

   public String getBrokerURL() {
      return brokerURL;
   }

   public void setBrokerURL(String brokerURL) {
      this.brokerURL = brokerURL;
   }

   public String getAcceptor() {
      return acceptor;
   }

   public ConnectionAbstract setAcceptor(String acceptor) {
      this.acceptor = acceptor;
      return this;
   }

   public String getUser() {
      return user;
   }

   public ConnectionAbstract setUser(String user) {
      this.user = user;
      return this;
   }

   public String getPassword() {
      return password;
   }

   public ConnectionAbstract setPassword(String password) {
      this.password = password;
      return this;
   }

   public String getClientID() {
      return clientID;
   }

   public ConnectionAbstract setClientID(String clientID) {
      this.clientID = clientID;
      return this;
   }

   public String getProtocol() {
      return protocol;
   }

   public void setProtocol(String protocol) {
      this.protocol = protocol;
   }

   @SuppressWarnings("StringEquality")
   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      // it is intentional to make a comparison on the String object here
      // this is to test if the original option was switched or not.
      // we don't care about being .equals at all.
      // as a matter of fact if you pass brokerURL in a way it's equals to DEFAULT_BROKER_URL,
      // we should not the broker URL Instance
      // and still honor the one passed by parameter.
      // SupressWarnings was added to this method to supress the false positive here from error-prone.
      if (brokerURL == DEFAULT_BROKER_URL) {
         String brokerURLInstance = getBrokerURLInstance(acceptor);

         if (brokerURLInstance != null) {
            brokerURL = brokerURLInstance;
         }
      }

      context.out.println("Connection brokerURL = " + brokerURL);

      return null;
   }

   protected ConnectionFactory createConnectionFactory() throws Exception {
      return createConnectionFactory(brokerURL, user, password, clientID, protocol);
   }

   protected ConnectionFactory createConnectionFactory(String brokerURL,
                                                       String user,
                                                       String password,
                                                       String clientID,
                                                       String protocol) throws Exception {
      if (protocol.equals("core")) {
         return createCoreConnectionFactory(brokerURL, user, password, clientID);
      } else if (protocol.equals("amqp")) {
         return createAMQPConnectionFactory(brokerURL, user, password, clientID);
      } else {
         throw new IllegalStateException("protocol " + protocol + " not supported");
      }
   }

   private ConnectionFactory createAMQPConnectionFactory(String brokerURL,
                                                         String user,
                                                         String password,
                                                         String clientID) {
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
         getActionContext().err.println("Connection failed::" + e.getMessage());
         cf = new JmsConnectionFactory(inputUser(user), inputPassword(password), brokerURL);
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         getActionContext().err.println("Connection failed::" + e.getMessage());
         cf = new JmsConnectionFactory(inputUser(user), inputPassword(password), inputBrokerURL(brokerURL));
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      }
   }

   protected ActiveMQConnectionFactory createCoreConnectionFactory() {
      return createCoreConnectionFactory(brokerURL, user, password, clientID);
   }

   protected ActiveMQConnectionFactory createCoreConnectionFactory(String brokerURL,
                                                                   String user,
                                                                   String password,
                                                                   String clientID) {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, user, password);
      if (clientID != null) {
         getActionContext().out.println("Consumer:: clientID = " + clientID);
         cf.setClientID(clientID);
      }
      try {
         Connection connection = cf.createConnection();
         connection.close();
         return cf;
      } catch (JMSSecurityException e) {
         // if a security exception will get the user and password through an input
         if (getActionContext() != null) {
            getActionContext().err.println("Connection failed::" + e.getMessage());
         }
         cf = new ActiveMQConnectionFactory(brokerURL, inputUser(user), inputPassword(password));
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         if (getActionContext() != null) {
            getActionContext().err.println("Connection failed::" + e.getMessage());
         }
         cf = new ActiveMQConnectionFactory(inputBrokerURL(brokerURL), inputUser(user), inputPassword(password));
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      }
   }

   private String inputBrokerURL(String defaultValue) {
      return input("--url", "Type in the broker URL for a retry (e.g. tcp://localhost:61616)", defaultValue);
   }

   private String inputUser(String user) {
      if (user == null) {
         user = input("--user", "Type the username for a retry", null);
      }
      return user;
   }

   private String inputPassword(String password) {
      if (password == null) {
         password = inputPassword("--password", "Type the password for a retry", null);
      }
      return password;
   }
}
