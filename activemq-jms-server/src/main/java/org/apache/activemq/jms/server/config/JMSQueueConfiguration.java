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
package org.apache.activemq.jms.server.config;

/**
 * A QeueConfiguration
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public interface JMSQueueConfiguration
{
   String getName();

   JMSQueueConfiguration setName(String name);

   String getSelector();

   JMSQueueConfiguration setSelector(String selector);

   boolean isDurable();

   JMSQueueConfiguration setDurable(boolean durable);

   String[] getBindings();

   JMSQueueConfiguration setBindings(String[] bindings);
}
