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
import org.apache.activemq.artemis.cli.Shell;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.InputAbstract;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import picocli.CommandLine.Option;

public class ConnectionAbstract extends InputAbstract {
   @Option(names = "--url", description = "Connection URL. Default: build URL from the 'artemis' acceptor defined in the broker.xml or tcp://localhost:61616 if the acceptor cannot be parsed.")
   protected String brokerURL = DEFAULT_BROKER_URL;

   @Option(names = "--acceptor", description = "Name used to find the default connection URL on the acceptor list. If an acceptor with that name cannot be found the CLI will look for a connector with the same name.")
   protected String acceptor;

   @Option(names = "--user", description = "User used to connect.")
   protected String user;

   @Option(names = "--password", description = "Password used to connect.")
   protected String password;

   @Option(names = "--clientID", description = "ClientID set on the connection.")
   protected String clientID;

   @Option(names = "--protocol", description = "Protocol used. Valid values are ${COMPLETION-CANDIDATES}", converter = ConnectionProtocol.ProtocolConverter.class)
   protected ConnectionProtocol protocol = ConnectionProtocol.CORE;

   protected static ThreadLocal<ConnectionInformation> CONNECTION_INFORMATION = new ThreadLocal<>();

   static class ConnectionInformation {
      String uri, user, password;

      private ConnectionInformation(String uri, String user, String password) {
         this.uri = uri;
         this.user = user;
         this.password = password;
      }
   }

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

   public ConnectionProtocol getProtocol() {
      return protocol;
   }

   public void setProtocol(ConnectionProtocol protocol) {
      this.protocol = protocol;
   }

   public void setProtocol(String protocol) {
      this.protocol = ConnectionProtocol.fromString(protocol);
   }

   @SuppressWarnings("StringEquality")
   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      recoverConnectionInformation();

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
      recoverConnectionInformation();
      return createConnectionFactory(brokerURL, user, password, clientID, protocol);
   }

   protected ConnectionFactory createConnectionFactory(String brokerURL,
                                                       String user,
                                                       String password,
                                                       String clientID,
                                                       ConnectionProtocol protocol) throws Exception {
      if (protocol == ConnectionProtocol.CORE) {
         return createCoreConnectionFactory(brokerURL, user, password, clientID);
      } else if (protocol == ConnectionProtocol.AMQP) {
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
         tryConnect(brokerURL, user, password, cf);
         return cf;
      } catch (JMSSecurityException e) {
         // if a security exception will get the user and password through an input
         getActionContext().err.println("Connection failed::" + e.getMessage());
         user = inputUser(user);
         password = inputPassword(password);
         cf = new JmsConnectionFactory(user, password, brokerURL);
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         try {
            tryConnect(brokerURL, user, password, cf);
         } catch (Exception e2) {
         }
         return cf;
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         getActionContext().err.println("Connection failed::" + e.getMessage());
         brokerURL = inputBrokerURL(brokerURL);
         user = inputUser(user);
         password = inputPassword(password);
         cf = new JmsConnectionFactory(user, password, brokerURL);
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         try {
            tryConnect(brokerURL, user, password, cf);
         } catch (Exception e2) {
         }
         return cf;
      }
   }

   protected ActiveMQConnectionFactory createCoreConnectionFactory() {
      recoverConnectionInformation();
      return createCoreConnectionFactory(brokerURL, user, password, clientID);
   }

   protected void recoverConnectionInformation() {
      if (CONNECTION_INFORMATION.get() != null) {
         ConnectionInformation connectionInfo = CONNECTION_INFORMATION.get();
         if (this.user == null) {
            this.user  = connectionInfo.user;
         }
         if (this.password == null) {
            this.password  = connectionInfo.password;
         }
         if (this.brokerURL == null || this.brokerURL == DEFAULT_BROKER_URL) {
            this.brokerURL  = connectionInfo.uri;
         }

      }
   }

   void saveConnectionInfo(String brokerURL, String user, String password) {
      if (Shell.inShell() && CONNECTION_INFORMATION.get() == null) {
         CONNECTION_INFORMATION.set(new ConnectionInformation(brokerURL, user, password));
         System.out.println("CLI connected to broker " + brokerURL + ", user:" + user);
         this.brokerURL = brokerURL;
         this.user = user;
         this.password = password;
      }
   }

   protected ActiveMQConnectionFactory createCoreConnectionFactory(String brokerURL,
                                                                   String user,
                                                                   String password,
                                                                   String clientID) {
      if (brokerURL.startsWith("amqp://")) {
         // replacing amqp:// by tcp://
         brokerURL = "tcp" + brokerURL.substring(4);
      }

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, user, password);
      if (clientID != null) {
         getActionContext().out.println("Consumer:: clientID = " + clientID);
         cf.setClientID(clientID);
      }
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
         if (clientID != null) {
            cf.setClientID(clientID);
         }
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
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         try {
            tryConnect(brokerURL, user, password, cf);
         } catch (Exception e2) {
         }
         return cf;
      }
   }

   private void tryConnect(String brokerURL,
                          String user,
                          String password,
                          ConnectionFactory cf) throws JMSException {
      Connection connection = cf.createConnection();
      connection.close();
      saveConnectionInfo(brokerURL, user, password);
   }

   private String inputBrokerURL(String defaultValue) {
      return input("--url", "Type in the connection URL for a retry (e.g. tcp://localhost:61616)", defaultValue);
   }

   private String inputUser(String user) {
      if (user == null) {
         this.user = input("--user", "Type the username for a retry", null);
         return this.user;
      }
      return user;
   }

   private String inputPassword(String password) {
      if (password == null) {
         this.password = inputPassword("--password", "Type the password for a retry", null);
         return this.password;
      }
      return password;
   }

   protected void performCoreManagement(ManagementHelper.MessageAcceptor setup, ManagementHelper.MessageAcceptor ok, ManagementHelper.MessageAcceptor failed) throws Exception {
      try (ActiveMQConnectionFactory factory = createCoreConnectionFactory()) {
         ManagementHelper.doManagement(factory.getServerLocator(), user, password, setup, ok, failed);
      }
   }

   protected void performCoreManagement(String uri, String user, String password, ManagementHelper.MessageAcceptor setup, ManagementHelper.MessageAcceptor ok, ManagementHelper.MessageAcceptor failed) throws Exception {
      try (ActiveMQConnectionFactory factory = createCoreConnectionFactory(uri, user, password, null)) {
         ManagementHelper.doManagement(factory.getServerLocator(), user, password, setup, ok, failed);
      }
   }
}