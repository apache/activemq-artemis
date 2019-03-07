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
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationPolicy;
import org.apache.activemq.artemis.core.config.federation.FederationUpstreamConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;

public class Federation {


   private final ActiveMQServer server;
   private final SimpleString name;

   private final Map<String, FederationUpstream> upstreams = new HashMap<>();
   private final FederationConfiguration config;
   private FederationManager.State state;

   enum State {
      STOPPED,
      STOPPING,
      /**
       * Deployed means {@link FederationManager#deploy()} was called but
       * {@link FederationManager#start()} was not called.
       * <p>
       * We need the distinction if {@link FederationManager#stop()} is called before 'start'. As
       * otherwise we would leak locators.
       */
      DEPLOYED, STARTED,
   }


   public Federation(final ActiveMQServer server, final FederationConfiguration config) {
      this.server = server;
      this.config = config;
      Objects.requireNonNull(config.getName());
      this.name = SimpleString.toSimpleString(config.getName());
   }

   public synchronized void start() throws ActiveMQException {
      if (state == FederationManager.State.STARTED) return;
      deploy();
      for (FederationUpstream connection : upstreams.values()) {
         connection.start();
      }
      state = FederationManager.State.STARTED;
   }

   public synchronized void stop() {
      if (state == FederationManager.State.STOPPED) return;
      state = FederationManager.State.STOPPING;

      for (FederationUpstream connection : upstreams.values()) {
         connection.stop();
      }
      upstreams.clear();
      state = FederationManager.State.STOPPED;
   }

   public synchronized void deploy() throws ActiveMQException {
      for (FederationUpstreamConfiguration upstreamConfiguration : config.getUpstreamConfigurations()) {
         deploy(upstreamConfiguration, config.getFederationPolicyMap());
      }
      if (state != FederationManager.State.STARTED) {
         state = FederationManager.State.DEPLOYED;
      }
   }

   public boolean isStarted() {
      return state == FederationManager.State.STARTED;
   }

   public synchronized boolean undeploy(String name) {
      FederationUpstream federationConnection = upstreams.remove(name);
      if (federationConnection != null) {
         federationConnection.stop();
      }
      return true;
   }



   public synchronized boolean deploy(FederationUpstreamConfiguration upstreamConfiguration, Map<String, FederationPolicy> federationPolicyMap) throws ActiveMQException {
      String name = upstreamConfiguration.getName();
      FederationUpstream upstream = upstreams.get(name);

      //If connection has changed we will need to do a full undeploy and redeploy.
      if (upstream == null) {
         undeploy(name);
         upstream = deploy(name, upstreamConfiguration);
      } else if (!upstream.getConnection().getConfig().equals(upstreamConfiguration.getConnectionConfiguration())) {
         undeploy(name);
         upstream = deploy(name, upstreamConfiguration);
      }

      upstream.deploy(upstreamConfiguration.getPolicyRefs(), federationPolicyMap);
      return true;
   }

   private synchronized FederationUpstream deploy(String name, FederationUpstreamConfiguration upstreamConfiguration) {
      FederationUpstream upstream = new FederationUpstream(server, this, name, upstreamConfiguration);
      upstreams.put(name, upstream);
      if (state == FederationManager.State.STARTED) {
         upstream.start();
      }
      return upstream;
   }

   public FederationUpstream get(String name) {
      return upstreams.get(name);
   }



   public void register(FederatedAbstract federatedAbstract) {
      server.registerBrokerPlugin(federatedAbstract);
   }

   public void unregister(FederatedAbstract federatedAbstract) {
      server.unRegisterBrokerPlugin(federatedAbstract);
   }

   String getFederationPassword() {
      return config.getCredentials() == null ? null : config.getCredentials().getPassword();
   }

   String getFederationUser() {
      return config.getCredentials() == null ? null : config.getCredentials().getUser();
   }

   public FederationConfiguration getConfig() {
      return config;
   }

   public SimpleString getName() {
      return name;
   }
}
