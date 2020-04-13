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

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumer;

public class PushSubscriptionsResource {

   protected Map<String, PushSubscription> consumers = new ConcurrentHashMap<>();
   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected final String startup = Long.toString(System.currentTimeMillis());
   protected final AtomicLong sessionCounter = new AtomicLong(1);
   protected TopicPushStore pushStore;

   private ConnectionFactoryOptions jmsOptions;

   public PushSubscriptionsResource(ConnectionFactoryOptions jmsOptions) {
      this.jmsOptions = jmsOptions;
   }

   public void stop() {
      for (PushConsumer consumer : consumers.values()) {
         consumer.stop();
         if (consumer.getRegistration().isDurable() == false) {
            deleteSubscriberQueue(consumer);
         }
      }
   }

   public TopicPushStore getPushStore() {
      return pushStore;
   }

   public void setPushStore(TopicPushStore pushStore) {
      this.pushStore = pushStore;
   }

   public ClientSession createSubscription(String subscriptionName, boolean durable) {
      ClientSession session = null;
      try {
         session = sessionFactory.createSession();

         session.createQueue(new QueueConfiguration(subscriptionName).setAddress(destination).setDurable(durable).setTemporary(!durable));
         return session;
      } catch (ActiveMQException e) {
         throw new RuntimeException(e);
      }
   }

   public void addRegistration(PushTopicRegistration reg) throws Exception {
      if (reg.isEnabled() == false)
         return;
      String destination = reg.getDestination();
      ClientSession session = sessionFactory.createSession(false, false, false);
      ClientSession.QueueQuery query = session.queueQuery(new SimpleString(destination));
      ClientSession createSession = null;
      if (!query.isExists()) {
         createSession = createSubscription(destination, reg.isDurable());
      }
      PushSubscription consumer = new PushSubscription(sessionFactory, reg.getDestination(), reg.getId(), reg, pushStore, jmsOptions);
      try {
         consumer.start();
      } catch (Exception e) {
         consumer.stop();
         throw new Exception("Failed starting push subscriber for " + destination + " of push subscriber: " + reg.getTarget(), e);
      } finally {
         closeSession(createSession);
         closeSession(session);
      }

      consumers.put(reg.getId(), consumer);

   }

   private void closeSession(ClientSession createSession) {
      if (createSession != null) {
         try {
            createSession.close();
         } catch (ActiveMQException e) {
         }
      }
   }

   @POST
   public Response create(@Context UriInfo uriInfo, PushTopicRegistration registration) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      //System.out.println("PushRegistration: " + registration);
      // todo put some logic here to check for duplicates
      String genId = sessionCounter.getAndIncrement() + "-topic-" + destination + "-" + startup;
      if (registration.getDestination() == null) {
         registration.setDestination(genId);
      }
      registration.setId(genId);
      registration.setTopic(destination);
      ClientSession createSession = createSubscription(genId, registration.isDurable());
      try {
         PushSubscription consumer = new PushSubscription(sessionFactory, genId, genId, registration, pushStore, jmsOptions);
         try {
            consumer.start();
            if (registration.isDurable() && pushStore != null) {
               pushStore.add(registration);
            }
         } catch (Exception e) {
            consumer.stop();
            throw new WebApplicationException(e, Response.serverError().entity("Failed to start consumer.").type("text/plain").build());
         }

         consumers.put(genId, consumer);
         UriBuilder location = uriInfo.getAbsolutePathBuilder();
         location.path(genId);
         return Response.created(location.build()).build();
      } finally {
         closeSession(createSession);
      }
   }

   @GET
   @Path("{consumer-id}")
   @Produces("application/xml")
   public PushTopicRegistration getConsumer(@Context UriInfo uriInfo, @PathParam("consumer-id") String consumerId) {
      ActiveMQRestLogger.LOGGER.debug("Handling GET request for \"" + uriInfo.getPath() + "\"");

      PushConsumer consumer = consumers.get(consumerId);
      if (consumer == null) {
         throw new WebApplicationException(Response.status(404).entity("Could not find consumer.").type("text/plain").build());
      }
      return (PushTopicRegistration) consumer.getRegistration();
   }

   @DELETE
   @Path("{consumer-id}")
   public void deleteConsumer(@Context UriInfo uriInfo, @PathParam("consumer-id") String consumerId) {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      PushConsumer consumer = consumers.remove(consumerId);
      if (consumer == null) {
         throw new WebApplicationException(Response.status(404).entity("Could not find consumer.").type("text/plain").build());
      }
      consumer.stop();
      deleteSubscriberQueue(consumer);
   }

   public Map<String, PushSubscription> getConsumers() {
      return consumers;
   }

   public ClientSessionFactory getSessionFactory() {
      return sessionFactory;
   }

   public void setSessionFactory(ClientSessionFactory sessionFactory) {
      this.sessionFactory = sessionFactory;
   }

   public String getDestination() {
      return destination;
   }

   public void setDestination(String destination) {
      this.destination = destination;
   }

   private void deleteSubscriberQueue(PushConsumer consumer) {
      String subscriptionName = consumer.getDestination();
      ClientSession session = null;
      try {
         session = sessionFactory.createSession();

         session.deleteQueue(subscriptionName);
      } catch (ActiveMQException e) {
      } finally {
         closeSession(session);
      }
   }
}
