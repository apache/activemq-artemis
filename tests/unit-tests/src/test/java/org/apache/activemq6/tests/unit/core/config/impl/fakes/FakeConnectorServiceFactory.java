/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.activemq6.tests.unit.core.config.impl.fakes;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq6.core.persistence.StorageManager;
import org.apache.activemq6.core.postoffice.PostOffice;
import org.apache.activemq6.core.server.ConnectorService;
import org.apache.activemq6.core.server.ConnectorServiceFactory;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class FakeConnectorServiceFactory implements ConnectorServiceFactory
{
   private ConnectorService connectorService;

   public FakeConnectorServiceFactory()
   {
      this.connectorService = new FakeConnectorService();
   }

   @Override
   public ConnectorService createConnectorService(String connectorName, Map<String, Object> configuration, StorageManager storageManager, PostOffice postOffice, ScheduledExecutorService scheduledThreadPool)
   {
      if (connectorService == null)
      {
         return new FakeConnectorService();
      }
      return connectorService;
   }

   @Override
   public Set<String> getAllowableProperties()
   {
      return new HashSet<String>();
   }

   @Override
   public Set<String> getRequiredProperties()
   {
      return new HashSet<String>();
   }

   public ConnectorService getConnectorService()
   {
      return connectorService;
   }
}
