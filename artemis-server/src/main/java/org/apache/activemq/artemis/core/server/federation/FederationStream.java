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

package org.apache.activemq.artemis.core.server.federation;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.federation.FederationStreamConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.federation.address.FederatedAddress;
import org.apache.activemq.artemis.core.server.federation.queue.FederatedQueue;
import org.jboss.logging.Logger;

public abstract class FederationStream {


   private static final Logger logger = Logger.getLogger(FederationStream.class);
   protected final ActiveMQServer server;
   protected final Federation federation;
   protected final SimpleString name;
   protected final FederationConnection connection;
   private FederationStreamConfiguration config;
   protected Map<String, FederatedQueue> federatedQueueMap = new HashMap<>();
   protected Map<String, FederatedAddress> federatedAddressMap = new HashMap<>();


   public FederationStream(ActiveMQServer server, Federation federation, String name, FederationStreamConfiguration config) {
      this(server, federation, name, config, null);
   }

   public FederationStream(final ActiveMQServer server, final Federation federation, final String name, final FederationStreamConfiguration config,
                           final FederationConnection connection) {
      this.server = server;
      this.federation = federation;
      Objects.requireNonNull(config.getName());
      this.name = SimpleString.toSimpleString(config.getName());
      this.config = config;
      this.connection = connection != null ? connection : new FederationConnection(server.getConfiguration(), name, config.getConnectionConfiguration());
   }

   public synchronized void start() {
      if (connection != null) {
         connection.start();
      }
   }

   public synchronized void stop() {
      if (connection != null) {
         connection.stop();
      }
   }

   public Federation getFederation() {
      return federation;
   }

   public FederationStreamConfiguration getConfig() {
      return config;
   }

   public SimpleString getName() {
      return name;
   }

   public FederationConnection getConnection() {
      return connection;
   }


   public String getUser() {
      String user = config.getConnectionConfiguration().getUsername();
      if (user == null || user.isEmpty()) {
         return federation.getFederationUser();
      } else {
         return user;
      }
   }

   public String getPassword() {
      String password = config.getConnectionConfiguration().getPassword();
      if (password == null || password.isEmpty()) {
         return federation.getFederationPassword();
      } else {
         return password;
      }
   }

   public int getPriorityAdjustment() {
      return config.getConnectionConfiguration().getPriorityAdjustment();
   }

}
