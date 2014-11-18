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

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.server.Consumer;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.management.NotificationService;
import org.apache.activemq.spi.core.protocol.RemotingConnection;

/**
 * A Core Bridge
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 15 Nov 2008 09:42:31
 *
 *
 */
public interface Bridge extends Consumer, ActiveMQComponent
{
   SimpleString getName();

   Queue getQueue();

   SimpleString getForwardingAddress();

   void flushExecutor();

   void setNotificationService(NotificationService notificationService);

   RemotingConnection getForwardingConnection();

   void pause() throws Exception;

   void resume() throws Exception;

   /**
    * To be called when the server sent a disconnect to the client.
    * Basically this is for cluster bridges being disconnected
    */
   void disconnect();

   boolean isConnected();
}
