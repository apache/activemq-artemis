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
package org.apache.activemq.artemis.tests.unit.core.config.impl.fakes;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ConnectorService;
import org.apache.activemq.artemis.core.server.ConnectorServiceFactory;

public class FakeConnectorServiceFactory implements ConnectorServiceFactory {

   private ConnectorService connectorService;

   public FakeConnectorServiceFactory() {
      this.connectorService = new FakeConnectorService();
   }

   @Override
   public ConnectorService createConnectorService(String connectorName,
                                                  Map<String, Object> configuration,
                                                  StorageManager storageManager,
                                                  PostOffice postOffice,
                                                  ScheduledExecutorService scheduledThreadPool) {
      if (connectorService == null) {
         return new FakeConnectorService();
      }
      return connectorService;
   }

   @Override
   public Set<String> getAllowableProperties() {
      return new HashSet<>();
   }

   @Override
   public Set<String> getRequiredProperties() {
      return new HashSet<>();
   }

   public ConnectorService getConnectorService() {
      return connectorService;
   }
}
