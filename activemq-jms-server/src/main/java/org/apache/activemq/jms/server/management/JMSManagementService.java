/**
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
package org.apache.activemq.jms.server.management;

import org.apache.activemq.api.jms.management.JMSServerControl;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQQueue;
import org.apache.activemq.jms.client.ActiveMQTopic;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.config.ConnectionFactoryConfiguration;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public interface JMSManagementService
{
   JMSServerControl registerJMSServer(JMSServerManager server) throws Exception;

   void unregisterJMSServer() throws Exception;

   void registerQueue(ActiveMQQueue queue, Queue serverQueue) throws Exception;

   void unregisterQueue(String name) throws Exception;

   void registerTopic(ActiveMQTopic topic) throws Exception;

   void unregisterTopic(String name) throws Exception;

   void registerConnectionFactory(String name, ConnectionFactoryConfiguration config, ActiveMQConnectionFactory connectionFactory) throws Exception;

   void unregisterConnectionFactory(String name) throws Exception;

   void stop() throws Exception;
}
