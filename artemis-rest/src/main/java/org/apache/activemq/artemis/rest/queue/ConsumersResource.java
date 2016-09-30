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
package org.apache.activemq.artemis.rest.queue;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.util.TimeoutTask;

public class ConsumersResource implements TimeoutTask.Callback {

   protected ConcurrentMap<String, QueueConsumer> queueConsumers = new ConcurrentHashMap<>();
   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected final String startup = Long.toString(System.currentTimeMillis());
   protected AtomicLong sessionCounter = new AtomicLong(1);
   protected int consumerTimeoutSeconds;
   protected DestinationServiceManager serviceManager;

   protected static final int ACKNOWLEDGED = 0x01;
   protected static final int SELECTOR_SET = 0x02;

   public DestinationServiceManager getServiceManager() {
      return serviceManager;
   }

   public void setServiceManager(DestinationServiceManager serviceManager) {
      this.serviceManager = serviceManager;
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

   public int getConsumerTimeoutSeconds() {
      return consumerTimeoutSeconds;
   }

   public void setConsumerTimeoutSeconds(int consumerTimeoutSeconds) {
      this.consumerTimeoutSeconds = consumerTimeoutSeconds;
   }

   @Override
   public boolean testTimeout(String target, boolean autoShutdown) {
      QueueConsumer consumer = queueConsumers.get(target);
      if (consumer == null)
         return false;
      if (System.currentTimeMillis() - consumer.getLastPingTime() > consumerTimeoutSeconds * 1000) {
         ActiveMQRestLogger.LOGGER.shutdownRestConsumer(consumer.getId());
         if (autoShutdown) {
            shutdown(consumer);
         }
         return true;
      } else {
         return false;
      }
   }

   @Override
   public void shutdown(String target) {
      QueueConsumer consumer = queueConsumers.get(target);
      if (consumer == null)
         return;
      shutdown(consumer);
   }

   private void shutdown(QueueConsumer consumer) {
      synchronized (consumer) {
         consumer.shutdown();
         queueConsumers.remove(consumer.getId());
      }
   }

   public void stop() {
      for (QueueConsumer consumer : queueConsumers.values()) {
         consumer.shutdown();
      }
   }

   @POST
   public Response createSubscription(@FormParam("autoAck") @DefaultValue("true") boolean autoAck,
                                      @FormParam("selector") String selector,
                                      @Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      try {
         QueueConsumer consumer = null;
         int attributes = 0;
         if (selector != null) {
            attributes = attributes | SELECTOR_SET;
         }

         if (autoAck) {
            consumer = createConsumer(selector);
         } else {
            attributes |= ACKNOWLEDGED;
            consumer = createAcknowledgedConsumer(selector);
         }

         String attributesSegment = "attributes-" + attributes;
         UriBuilder location = uriInfo.getAbsolutePathBuilder();
         location.path(attributesSegment);
         location.path(consumer.getId());
         Response.ResponseBuilder builder = Response.created(location.build());

         if (autoAck) {
            QueueConsumer.setConsumeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(0) + "/" + attributesSegment + "/" + consumer.getId(), "-1");
         } else {
            AcknowledgedQueueConsumer.setAcknowledgeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(0) + "/" + attributesSegment + "/" + consumer.getId(), "-1");

         }
         return builder.build();

      } catch (ActiveMQException e) {
         throw new RuntimeException(e);
      } finally {
      }
   }

   protected void addConsumer(QueueConsumer consumer) {
      queueConsumers.put(consumer.getId(), consumer);
      serviceManager.getTimeoutTask().add(this, consumer.getId());
   }

   public QueueConsumer createConsumer(String selector) throws ActiveMQException {
      String genId = sessionCounter.getAndIncrement() + "-queue-" + destination + "-" + startup;
      QueueConsumer consumer = new QueueConsumer(sessionFactory, destination, genId, serviceManager, selector);
      addConsumer(consumer);
      return consumer;
   }

   public QueueConsumer createAcknowledgedConsumer(String selector) throws ActiveMQException {
      String genId = sessionCounter.getAndIncrement() + "-queue-" + destination + "-" + startup;
      QueueConsumer consumer = new AcknowledgedQueueConsumer(sessionFactory, destination, genId, serviceManager, selector);
      addConsumer(consumer);
      return consumer;
   }

   @Path("attributes-{attributes}/{consumer-id}")
   @GET
   public Response getConsumer(@PathParam("attributes") int attributes,
                               @PathParam("consumer-id") String consumerId,
                               @Context UriInfo uriInfo) throws Exception {
      ActiveMQRestLogger.LOGGER.debug("Handling GET request for \"" + uriInfo.getPath() + "\"");

      return headConsumer(attributes, consumerId, uriInfo);
   }

   @Path("attributes-{attributes}/{consumer-id}")
   @HEAD
   public Response headConsumer(@PathParam("attributes") int attributes,
                                @PathParam("consumer-id") String consumerId,
                                @Context UriInfo uriInfo) throws Exception {
      ActiveMQRestLogger.LOGGER.debug("Handling HEAD request for \"" + uriInfo.getPath() + "\"");

      QueueConsumer consumer = findConsumer(attributes, consumerId, uriInfo);
      Response.ResponseBuilder builder = Response.noContent();
      // we synchronize just in case a failed request is still processing
      synchronized (consumer) {
         if ((attributes & ACKNOWLEDGED) > 0) {
            AcknowledgedQueueConsumer ackedConsumer = (AcknowledgedQueueConsumer) consumer;
            Acknowledgement ack = ackedConsumer.getAck();
            if (ack == null || ack.wasSet()) {
               AcknowledgedQueueConsumer.setAcknowledgeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/attributes-" + attributes + "/" + consumer.getId(), Long.toString(consumer.getConsumeIndex()));
            } else {
               ackedConsumer.setAcknowledgementLink(builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/attributes-" + attributes + "/" + consumer.getId());
            }

         } else {
            QueueConsumer.setConsumeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/attributes-" + attributes + "/" + consumer.getId(), Long.toString(consumer.getConsumeIndex()));
         }
      }
      return builder.build();
   }

   @Path("attributes-{attributes}/{consumer-id}")
   public QueueConsumer findConsumer(@PathParam("attributes") int attributes,
                                     @PathParam("consumer-id") String consumerId,
                                     @Context UriInfo uriInfo) throws Exception {
      QueueConsumer consumer = queueConsumers.get(consumerId);
      if (consumer == null) {
         if ((attributes & SELECTOR_SET) > 0) {

            Response.ResponseBuilder builder = Response.status(Response.Status.GONE).entity("Cannot reconnect to selector-based consumer.  You must recreate the consumer session.").type("text/plain");
            UriBuilder uriBuilder = uriInfo.getBaseUriBuilder();
            uriBuilder.path(uriInfo.getMatchedURIs().get(1));
            serviceManager.getLinkStrategy().setLinkHeader(builder, "pull-consumers", "pull-consumers", uriBuilder.build().toString(), null);
            throw new WebApplicationException(builder.build());

         }
         if ((attributes & ACKNOWLEDGED) > 0) {
            QueueConsumer tmp = new AcknowledgedQueueConsumer(sessionFactory, destination, consumerId, serviceManager, null);
            consumer = addReconnectedConsumerToMap(consumerId, tmp);

         } else {
            QueueConsumer tmp = new QueueConsumer(sessionFactory, destination, consumerId, serviceManager, null);
            consumer = addReconnectedConsumerToMap(consumerId, tmp);
         }
      }
      return consumer;
   }

   private QueueConsumer addReconnectedConsumerToMap(String consumerId, QueueConsumer tmp) {
      QueueConsumer consumer;
      consumer = queueConsumers.putIfAbsent(consumerId, tmp);
      if (consumer != null) {
         tmp.shutdown();
      } else {
         consumer = tmp;
         serviceManager.getTimeoutTask().add(this, consumer.getId());
      }
      return consumer;
   }

   @Path("attributes-{attributes}/{consumer-id}")
   @DELETE
   public void closeSession(@PathParam("consumer-id") String consumerId, @Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      QueueConsumer consumer = queueConsumers.remove(consumerId);
      if (consumer == null) {
         throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).entity("Failed to match a consumer to URL" + consumerId).type("text/plain").build());
      }
      consumer.shutdown();
   }
}
