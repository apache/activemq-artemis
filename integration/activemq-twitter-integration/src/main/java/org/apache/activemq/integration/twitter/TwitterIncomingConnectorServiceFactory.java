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
package org.apache.activemq.integration.twitter;

import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.server.ConnectorService;
import org.apache.activemq.core.server.ConnectorServiceFactory;
import org.apache.activemq.integration.twitter.impl.IncomingTweetsHandler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jun 29, 2010
 */
public class TwitterIncomingConnectorServiceFactory implements ConnectorServiceFactory
{
   public ConnectorService createConnectorService(String connectorName, final Map<String, Object> configuration,
                                                  final StorageManager storageManager,
                                                  final PostOffice postOffice,
                                                  final ScheduledExecutorService scheduledThreadPool)
   {
      return new IncomingTweetsHandler(connectorName, configuration, storageManager, postOffice, scheduledThreadPool);
   }

   public Set<String> getAllowableProperties()
   {
      return TwitterConstants.ALLOWABLE_INCOMING_CONNECTOR_KEYS;
   }

   public Set<String> getRequiredProperties()
   {
      return TwitterConstants.REQUIRED_INCOMING_CONNECTOR_KEYS;
   }
}
