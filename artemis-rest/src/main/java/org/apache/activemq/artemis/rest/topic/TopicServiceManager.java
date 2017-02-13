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
package org.apache.activemq.artemis.rest.topic;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.queue.DestinationServiceManager;

public class TopicServiceManager extends DestinationServiceManager {

   protected TopicPushStore pushStore;
   protected List<TopicDeployment> topics = new ArrayList<>();
   protected TopicDestinationsResource destination;

   public TopicServiceManager(ConnectionFactoryOptions jmsOptions) {
      super(jmsOptions);
   }

   public TopicPushStore getPushStore() {
      return pushStore;
   }

   public void setPushStore(TopicPushStore pushStore) {
      this.pushStore = pushStore;
   }

   public List<TopicDeployment> getTopics() {
      return topics;
   }

   public void setTopics(List<TopicDeployment> topics) {
      this.topics = topics;
   }

   public TopicDestinationsResource getDestination() {
      return destination;
   }

   public void setDestination(TopicDestinationsResource destination) {
      this.destination = destination;
   }

   @Override
   public void start() throws Exception {
      initDefaults();

      started = true;

      if (pushStoreFile != null && pushStore == null) {
         pushStore = new FileTopicPushStore(pushStoreFile);
      }

      if (destination == null) {
         destination = new TopicDestinationsResource(this);
      }

      for (TopicDeployment topic : topics) {
         deploy(topic);
      }
   }

   public void deploy(TopicDeployment topicDeployment) throws Exception {
      if (!started) {
         throw new Exception("You must start() this class instance before deploying");
      }
      String queueName = topicDeployment.getName();
      boolean defaultDurable;

      try (ClientSession session = sessionFactory.createSession(false, false, false)) {
         defaultDurable = topicDeployment.isDurableSend();
         ClientSession.AddressQuery query = session.addressQuery(new SimpleString(queueName));
         if (!query.isExists())
            session.createAddress(SimpleString.toSimpleString(queueName), RoutingType.MULTICAST, true);
      }

      destination.createTopicResource(queueName, defaultDurable, topicDeployment.getConsumerSessionTimeoutSeconds(), topicDeployment.isDuplicatesAllowed());
   }

   @Override
   public void stop() {
      if (started == false)
         return;
      for (TopicResource topic : destination.getTopics().values()) {
         topic.stop();
      }
      try {
         sessionFactory.close();
      } catch (Exception e) {
      }
   }
}
