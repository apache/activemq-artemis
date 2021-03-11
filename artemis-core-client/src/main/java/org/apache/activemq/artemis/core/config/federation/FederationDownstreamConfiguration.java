/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.config.federation;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;

public class FederationDownstreamConfiguration extends FederationStreamConfiguration<FederationDownstreamConfiguration> {

   private String upstreamConfigurationRef;
   private TransportConfiguration upstreamConfiguration;

   public String getUpstreamConfigurationRef() {
      return upstreamConfigurationRef;
   }

   public void setUpstreamConfigurationRef(String upstreamConfigurationRef) {
      this.upstreamConfigurationRef = upstreamConfigurationRef;
   }

   public TransportConfiguration getUpstreamConfiguration() {
      return upstreamConfiguration;
   }

   public void setUpstreamConfiguration(TransportConfiguration transportConfiguration) {

      final Map<String, Object> params = new HashMap<>(transportConfiguration.getParams());

      //clear any TLS settings as they won't apply to the federated server that uses this config
      //The federated server that creates the upstream back will rely on its config from the acceptor for TLS
      params.remove(TransportConstants.SSL_ENABLED_PROP_NAME);
      params.remove(TransportConstants.SSL_PROVIDER);
      params.remove(TransportConstants.SSL_KRB5_CONFIG_PROP_NAME);
      params.remove(TransportConstants.KEYSTORE_PATH_PROP_NAME);
      params.remove(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME);
      params.remove(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME);
      params.remove(TransportConstants.KEYSTORE_TYPE_PROP_NAME);
      params.remove(TransportConstants.TRUSTSTORE_PATH_PROP_NAME);
      params.remove(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME);
      params.remove(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME);
      params.remove(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME);

      this.upstreamConfiguration = new TransportConfiguration(transportConfiguration.getFactoryClassName(), params,
                                                              transportConfiguration.getName(), transportConfiguration.getExtraParams());
   }

   @Override
   public void encode(ActiveMQBuffer buffer) {
      super.encode(buffer);
      upstreamConfiguration.encode(buffer);
   }

   @Override
   public void decode(ActiveMQBuffer buffer) {
      super.decode(buffer);
      upstreamConfiguration = new TransportConfiguration();
      upstreamConfiguration.decode(buffer);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }
      FederationDownstreamConfiguration that = (FederationDownstreamConfiguration) o;
      return Objects.equals(upstreamConfigurationRef, that.upstreamConfigurationRef) &&
         Objects.equals(upstreamConfiguration, that.upstreamConfiguration);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), upstreamConfigurationRef, upstreamConfiguration);
   }
}
