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
package org.apache.activemq.artemis.core.config;

import java.security.KeyStore;

public class JMXConnectorConfiguration {
   private int rmiRegistryPort;
   private String connectorHost = "localhost";
   private int connectorPort = 1099;

   private String connectorPath = "/jmxrmi";

   private String jmxRealm = "activemq";
   private String objectName = "connector:name=rmi";
   private String authenticatorType = "password";
   private boolean secured =  false;
   private String keyStoreProvider = KeyStore.getDefaultType();
   private String keyStorePath;
   private String keyStorePassword;
   private String trustStoreProvider = KeyStore.getDefaultType();
   private String trustStorePath;
   private String trustStorePassword;

   public int getRmiRegistryPort() {
      return rmiRegistryPort;
   }

   public void setRmiRegistryPort(int rmiRegistryPort) {
      this.rmiRegistryPort = rmiRegistryPort;
   }

   public String getConnectorHost() {
      return connectorHost;
   }

   public void setConnectorHost(String connectorHost) {
      this.connectorHost = connectorHost;
   }

   public int getConnectorPort() {
      return connectorPort;
   }

   public void setConnectorPort(int connectorPort) {
      this.connectorPort = connectorPort;
   }

   public String getJmxRealm() {
      return jmxRealm;
   }

   public void setJmxRealm(String jmxRealm) {
      this.jmxRealm = jmxRealm;
   }

   public String getServiceUrl() {
      String rmiServer = "";
      if (rmiRegistryPort != 0) {
         // This is handy to use if you have a firewall and need to force JMX to use fixed ports.
         rmiServer = "" + getConnectorHost() + ":" + rmiRegistryPort;
      }
      return "service:jmx:rmi://" + rmiServer + "/jndi/rmi://" + getConnectorHost() + ":" + connectorPort + connectorPath;
   }

   public String getAuthenticatorType() {
      return authenticatorType;
   }

   public void setAuthenticatorType(String authenticatorType) {
      this.authenticatorType = authenticatorType;
   }

   public boolean isSecured() {
      return secured;
   }

   public String getKeyStoreProvider() {
      return keyStoreProvider;
   }

   public void setKeyStoreProvider(String keyStoreProvider) {
      this.keyStoreProvider = keyStoreProvider;
   }

   public String getKeyStorePath() {
      return keyStorePath;
   }

   public void setKeyStorePath(String keyStorePath) {
      this.keyStorePath = keyStorePath;
   }

   public String getKeyStorePassword() {
      return keyStorePassword;
   }

   public void setKeyStorePassword(String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
   }

   public String getTrustStoreProvider() {
      return trustStoreProvider;
   }

   public void setTrustStoreProvider(String trustStoreProvider) {
      this.trustStoreProvider = trustStoreProvider;
   }

   public String getTrustStorePath() {
      return trustStorePath;
   }

   public void setTrustStorePath(String trustStorePath) {
      this.trustStorePath = trustStorePath;
   }

   public String getTrustStorePassword() {
      return trustStorePassword;
   }

   public void setTrustStorePassword(String trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
   }

   public void setObjectName(String objectName) {
      this.objectName = objectName;
   }

   public String getObjectName() {
      return objectName;
   }

   public void setSecured(Boolean secured) {
      this.secured = secured;
   }
}
