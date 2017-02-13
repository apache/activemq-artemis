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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.activemq.artemis.jms.server.config.TopicConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.DestinationSettings;
import org.apache.activemq.artemis.rest.queue.PostMessage;
import org.apache.activemq.artemis.rest.queue.PostMessageDupsOk;
import org.apache.activemq.artemis.rest.queue.PostMessageNoDups;
import org.w3c.dom.Document;

@Path("/topics")
public class TopicDestinationsResource {

   private final Map<String, TopicResource> topics = new ConcurrentHashMap<>();
   private final TopicServiceManager manager;

   public TopicDestinationsResource(TopicServiceManager manager) {
      this.manager = manager;
   }

   @POST
   @Consumes("application/activemq.jms.topic+xml")
   public Response createJmsTopic(@Context UriInfo uriInfo, Document document) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      try {
         TopicConfiguration topic = FileJMSConfiguration.parseTopicConfiguration(document.getDocumentElement());
         ActiveMQTopic activeMQTopic = ActiveMQDestination.createTopic(topic.getName());
         String topicName = activeMQTopic.getAddress();
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try {

            ClientSession.AddressQuery query = session.addressQuery(new SimpleString(topicName));
            if (!query.isExists()) {
               session.createAddress(SimpleString.toSimpleString(topicName), RoutingType.MULTICAST, true);

            } else {
               throw new WebApplicationException(Response.status(412).type("text/plain").entity("Queue already exists.").build());
            }
         } finally {
            try {
               session.close();
            } catch (Exception ignored) {
            }
         }
         URI uri = uriInfo.getRequestUriBuilder().path(topicName).build();
         return Response.created(uri).build();
      } catch (Exception e) {
         if (e instanceof WebApplicationException)
            throw (WebApplicationException) e;
         throw new WebApplicationException(e, Response.serverError().type("text/plain").entity("Failed to create queue.").build());
      }
   }

   @Path("/{topic-name}")
   public TopicResource findTopic(@PathParam("topic-name") String name) throws Exception {
      TopicResource topic = topics.get(name);
      if (topic == null) {
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try {
            ClientSession.AddressQuery query = session.addressQuery(new SimpleString(name));
            if (!query.isExists()) {
               System.err.println("Topic '" + name + "' does not exist");
               throw new WebApplicationException(Response.status(404).type("text/plain").entity("Topic '" + name + "' does not exist").build());
            }
            DestinationSettings queueSettings = manager.getDefaultSettings();
            boolean defaultDurable = queueSettings.isDurableSend();

            topic = createTopicResource(name, defaultDurable, queueSettings.getConsumerSessionTimeoutSeconds(), queueSettings.isDuplicatesAllowed());
         } finally {
            try {
               session.close();
            } catch (ActiveMQException e) {
            }
         }
      }
      return topic;
   }

   public Map<String, TopicResource> getTopics() {
      return topics;
   }

   public TopicResource createTopicResource(String topicName,
                                            boolean defaultDurable,
                                            int timeoutSeconds,
                                            boolean duplicates) throws Exception {
      TopicResource topicResource = new TopicResource();
      topicResource.setTopicDestinationsResource(this);
      topicResource.setDestination(topicName);
      topicResource.setServiceManager(manager);
      SubscriptionsResource subscriptionsResource = new SubscriptionsResource();
      topicResource.setSubscriptions(subscriptionsResource);
      subscriptionsResource.setConsumerTimeoutSeconds(timeoutSeconds);
      subscriptionsResource.setServiceManager(manager);

      subscriptionsResource.setDestination(topicName);
      subscriptionsResource.setSessionFactory(manager.getConsumerSessionFactory());
      PushSubscriptionsResource push = new PushSubscriptionsResource(manager.getJmsOptions());
      push.setDestination(topicName);
      push.setSessionFactory(manager.getConsumerSessionFactory());
      topicResource.setPushSubscriptions(push);

      PostMessage sender = null;
      if (duplicates) {
         sender = new PostMessageDupsOk();
      } else {
         sender = new PostMessageNoDups();
      }
      sender.setDefaultDurable(defaultDurable);
      sender.setDestination(topicName);
      sender.setSessionFactory(manager.getSessionFactory());
      sender.setPoolSize(manager.getProducerPoolSize());
      sender.setProducerTimeToLive(manager.getProducerTimeToLive());
      sender.setServiceManager(manager);
      sender.init();
      topicResource.setSender(sender);

      if (manager.getPushStore() != null) {
         push.setPushStore(manager.getPushStore());
         List<PushTopicRegistration> regs = manager.getPushStore().getByTopic(topicName);
         for (PushTopicRegistration reg : regs) {
            push.addRegistration(reg);
         }
      }

      getTopics().put(topicName, topicResource);
      topicResource.start();
      return topicResource;
   }
}
