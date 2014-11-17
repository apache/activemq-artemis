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
package org.apache.activemq.core.server.cluster;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.server.HornetQComponent;
import org.apache.activemq.core.server.management.NotificationService;

/**
 * A BroadcastGroup
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 18 Nov 2008 09:29:45
 *
 *
 */
public interface BroadcastGroup extends HornetQComponent
{
   void setNotificationService(NotificationService notificationService);

   String getName();

   void addConnector(TransportConfiguration tcConfig);

   void removeConnector(TransportConfiguration tcConfig);

   int size();

   void broadcastConnectors() throws Exception;
}
