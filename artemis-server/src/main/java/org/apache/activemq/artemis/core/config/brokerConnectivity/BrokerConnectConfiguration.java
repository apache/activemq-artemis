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
package org.apache.activemq.artemis.core.config.brokerConnectivity;

import java.io.Serializable;

/** This is an extension point for outgoing broker configuration.
 *  This is a new feature that at the time we introduced, is only being used for AMQP.
 *  Where the broker will create a connection towards another broker using a specific protocol.
 *  */
public abstract class BrokerConnectConfiguration implements Serializable {
   private static final long serialVersionUID = 8026604526022462048L;

   private String name;
   private String uri;
   private String user;
   private String password;
   private int reconnectAttempts = -1;
   private int retryInterval = 5000;
   private boolean autostart = true;

   public BrokerConnectConfiguration(String name, String uri) {
      this.name = name;
      this.uri = uri;
   }

   public abstract void parseURI() throws Exception;



   public int getReconnectAttempts() {
      return reconnectAttempts;
   }

   public BrokerConnectConfiguration setReconnectAttempts(int reconnectAttempts) {
      this.reconnectAttempts = reconnectAttempts;
      return this;
   }


   public String getUser() {
      return user;
   }

   public BrokerConnectConfiguration setUser(String user) {
      this.user = user;
      return this;
   }


   public String getPassword() {
      return password;
   }

   public BrokerConnectConfiguration setPassword(String password) {
      this.password = password;
      return this;
   }

   public int getRetryInterval() {
      return retryInterval;
   }

   public BrokerConnectConfiguration setRetryInterval(int retryInterval) {
      this.retryInterval = retryInterval;
      return this;
   }

   public String getUri() {
      return uri;
   }

   public BrokerConnectConfiguration setUri(String uri) {
      this.uri = uri;
      return this;
   }


   public String getName() {
      return name;
   }

   public BrokerConnectConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public boolean isAutostart() {
      return autostart;
   }

   public BrokerConnectConfiguration setAutostart(boolean autostart) {
      this.autostart = autostart;
      return this;
   }
}
