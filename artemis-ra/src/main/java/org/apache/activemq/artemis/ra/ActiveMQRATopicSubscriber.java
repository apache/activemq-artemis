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
package org.apache.activemq.artemis.ra;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * A wrapper for a topic subscriber
 */
public class ActiveMQRATopicSubscriber extends ActiveMQRAMessageConsumer implements TopicSubscriber {

   /**
    * Whether trace is enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * Create a new wrapper
    *
    * @param consumer the topic subscriber
    * @param session  the session
    */
   public ActiveMQRATopicSubscriber(final TopicSubscriber consumer, final ActiveMQRASession session) {
      super(consumer, session);

      if (ActiveMQRATopicSubscriber.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + consumer + ", " + session + ")");
      }
   }

   /**
    * Get the no local value
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean getNoLocal() throws JMSException {
      if (ActiveMQRATopicSubscriber.trace) {
         ActiveMQRALogger.LOGGER.trace("getNoLocal()");
      }

      checkState();
      return ((TopicSubscriber) consumer).getNoLocal();
   }

   /**
    * Get the topic
    *
    * @return The topic
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Topic getTopic() throws JMSException {
      if (ActiveMQRATopicSubscriber.trace) {
         ActiveMQRALogger.LOGGER.trace("getTopic()");
      }

      checkState();
      return ((TopicSubscriber) consumer).getTopic();
   }
}
