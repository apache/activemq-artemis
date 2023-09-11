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
package org.apache.activemq.artemis.core.config.amqpBrokerConnectivity;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.brokerConnectivity.BrokerConnectConfiguration;
import org.apache.activemq.artemis.uri.ConnectorTransportConfigurationParser;

/**
 * This is a specific AMQP Broker Connection Configuration
 * */
public class AMQPBrokerConnectConfiguration extends BrokerConnectConfiguration {

   List<TransportConfiguration> transportConfigurations;

   List<AMQPBrokerConnectionElement> connectionElements = new ArrayList<>();

   public AMQPBrokerConnectConfiguration() {
      super(null, null);
   }

   public AMQPBrokerConnectConfiguration(String name, String uri) {
      super(name, uri);
   }

   public AMQPBrokerConnectConfiguration addElement(AMQPBrokerConnectionElement amqpBrokerConnectionElement) {
      amqpBrokerConnectionElement.setParent(this);

      if (amqpBrokerConnectionElement.getType() == AMQPBrokerConnectionAddressType.MIRROR && !(amqpBrokerConnectionElement instanceof AMQPMirrorBrokerConnectionElement)) {
         throw new IllegalArgumentException("must be an AMQPMirrorConnectionElement");
      }

      connectionElements.add(amqpBrokerConnectionElement);

      return this;
   }

   public AMQPBrokerConnectConfiguration addConnectionElement(AMQPMirrorBrokerConnectionElement amqpBrokerConnectionElement) {
      return addElement(amqpBrokerConnectionElement);
   }

   public List<AMQPBrokerConnectionElement> getConnectionElements() {
      return connectionElements;
   }

   public AMQPBrokerConnectConfiguration addFederation(AMQPFederatedBrokerConnectionElement amqpFederationElement) {
      return addElement(amqpFederationElement);
   }

   public List<AMQPBrokerConnectionElement> getFederations() {
      // This returns all elements not just federation elements, broker properties relies on being able
      // to modify the collection from the getter...it does not actually call the add method, it only
      // uses the method to infer the type.
      return connectionElements;
   }

   @Override
   public void parseURI() throws Exception {
      ConnectorTransportConfigurationParser parser = new ConnectorTransportConfigurationParser(false);
      URI transportURI = parser.expandURI(getUri());
      this.transportConfigurations = parser.newObject(transportURI, getName());
   }

   public List<TransportConfiguration> getTransportConfigurations() throws Exception {
      if (transportConfigurations == null) {
         parseURI();
      }
      return transportConfigurations;
   }

   @Override
   public AMQPBrokerConnectConfiguration setReconnectAttempts(int reconnectAttempts) {
      super.setReconnectAttempts(reconnectAttempts);
      return this;
   }

   @Override
   public AMQPBrokerConnectConfiguration setUser(String user) {
      super.setUser(user);
      return this;
   }

   @Override
   public AMQPBrokerConnectConfiguration setRetryInterval(int retryInterval) {
      super.setRetryInterval(retryInterval);
      return this;
   }

   @Override
   public AMQPBrokerConnectConfiguration setPassword(String password) {
      super.setPassword(password);
      return this;
   }

   @Override
   public AMQPBrokerConnectConfiguration setUri(String uri) {
      super.setUri(uri);
      return this;
   }

   @Override
   public AMQPBrokerConnectConfiguration setName(String name) {
      super.setName(name);
      return this;
   }

   @Override
   public AMQPBrokerConnectConfiguration setAutostart(boolean autostart) {
      super.setAutostart(autostart);
      return this;
   }
}
