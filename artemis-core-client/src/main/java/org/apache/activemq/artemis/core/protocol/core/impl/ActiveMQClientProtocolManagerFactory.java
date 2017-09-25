/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.core.impl;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManagerFactory;

public class ActiveMQClientProtocolManagerFactory implements ClientProtocolManagerFactory {

   private static final long serialVersionUID = 1;

   private ActiveMQClientProtocolManagerFactory() {
   }

   ServerLocator locator;

   @Override
   public ServerLocator getLocator() {
      return locator;
   }

   @Override
   public void setLocator(ServerLocator locator) {
      this.locator = locator;
   }

   public static final ActiveMQClientProtocolManagerFactory getInstance(ServerLocator locator) {
      ActiveMQClientProtocolManagerFactory factory = new ActiveMQClientProtocolManagerFactory();
      factory.setLocator(locator);
      return factory;
   }

   @Override
   public ClientProtocolManager newProtocolManager() {
      return new ActiveMQClientProtocolManager();
   }

   @Override
   public TransportConfiguration adaptTransportConfiguration(TransportConfiguration tc) {
      return tc;
   }
}
