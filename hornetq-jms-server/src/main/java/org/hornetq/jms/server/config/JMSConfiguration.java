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
package org.hornetq.jms.server.config;

import java.util.List;

import javax.naming.Context;

/**
 * A JMSConfiguration
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public interface JMSConfiguration
{
   JMSConfiguration setContext(Context context);

   Context getContext();

   List<JMSQueueConfiguration> getQueueConfigurations();

   JMSConfiguration setQueueConfigurations(List<JMSQueueConfiguration> queueConfigurations);

   List<TopicConfiguration> getTopicConfigurations();

   JMSConfiguration setTopicConfigurations(List<TopicConfiguration> topicConfigurations);

   List<ConnectionFactoryConfiguration> getConnectionFactoryConfigurations();

   JMSConfiguration setConnectionFactoryConfigurations(List<ConnectionFactoryConfiguration> connectionFactoryConfigurations);

   String getDomain();

   JMSConfiguration setDomain(String domain);
}
