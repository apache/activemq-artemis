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
package org.apache.activemq.ra;

import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnectionFactory;
import javax.resource.Referenceable;
import javax.resource.ResourceException;
import java.io.Serializable;

import org.apache.activemq.jms.client.ActiveMQConnectionFactory;

/**
 * An aggregate interface for the JMS connection factories
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.com">Jesper Pedersen</a>
 */
public interface ActiveMQRAConnectionFactory extends XAQueueConnectionFactory,
   XATopicConnectionFactory, Serializable, Referenceable
{
   /**
    * Connection factory capable of handling connections
    */
   int CONNECTION = 0;

   /**
    * Connection factory capable of handling queues
    */
   int QUEUE_CONNECTION = 1;

   /**
    * Connection factory capable of handling topics
    */
   int TOPIC_CONNECTION = 2;

   /**
    * Connection factory capable of handling XA connections
    */
   int XA_CONNECTION = 3;

   /**
    * Connection factory capable of handling XA queues
    */
   int XA_QUEUE_CONNECTION = 4;

   /**
    * Connection factory capable of handling XA topics
    */
   int XA_TOPIC_CONNECTION = 5;

   ActiveMQConnectionFactory getDefaultFactory() throws ResourceException;

   ActiveMQResourceAdapter getResourceAdapter();
}
