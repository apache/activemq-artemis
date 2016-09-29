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

import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.util.HttpMessageHelper;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;

public class PostMessage {

   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected boolean defaultDurable = false;
   protected DestinationServiceManager serviceManager;
   private AtomicLong counter = new AtomicLong(1);
   private final String startupTime = Long.toString(System.currentTimeMillis());
   protected long producerTimeToLive;
   protected ArrayBlockingQueue<Pooled> pool;
   protected int poolSize = 10;

   protected static class Pooled {

      public ClientSession session;
      public ClientProducer producer;

      private Pooled(ClientSession session, ClientProducer producer) {
         this.session = session;
         this.producer = producer;
      }
   }

   protected String generateDupId() {
      return startupTime + Long.toString(counter.incrementAndGet());
   }

   public void publish(HttpHeaders headers,
                       byte[] body,
                       String dup,
                       boolean durable,
                       Long ttl,
                       Long expiration,
                       Integer priority) throws Exception {
      Pooled pooled = getPooled();
      try {
         ClientProducer producer = pooled.producer;
         ClientMessage message = createActiveMQMessage(headers, body, durable, ttl, expiration, priority, pooled.session);
         message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID.toString(), dup);
         producer.send(message);
         ActiveMQRestLogger.LOGGER.debug("Sent message: " + message);
         pool.add(pooled);
      } catch (Exception ex) {
         try {
            pooled.session.close();
         } catch (ActiveMQException e) {
         }
         addPooled();
         throw ex;
      }
   }

   @PUT
   @Path("{id}")
   public Response putWithId(@PathParam("id") String dupId,
                             @QueryParam("durable") Boolean durable,
                             @QueryParam("ttl") Long ttl,
                             @QueryParam("expiration") Long expiration,
                             @QueryParam("priority") Integer priority,
                             @Context HttpHeaders headers,
                             @Context UriInfo uriInfo,
                             byte[] body) {
      ActiveMQRestLogger.LOGGER.debug("Handling PUT request for \"" + uriInfo.getRequestUri() + "\"");

      return internalPostWithId(dupId, durable, ttl, expiration, priority, headers, uriInfo, body);
   }

   @POST
   @Path("{id}")
   public Response postWithId(@PathParam("id") String dupId,
                              @QueryParam("durable") Boolean durable,
                              @QueryParam("ttl") Long ttl,
                              @QueryParam("expiration") Long expiration,
                              @QueryParam("priority") Integer priority,
                              @Context HttpHeaders headers,
                              @Context UriInfo uriInfo,
                              byte[] body) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getRequestUri() + "\"");

      return internalPostWithId(dupId, durable, ttl, expiration, priority, headers, uriInfo, body);
   }

   private Response internalPostWithId(String dupId,
                                       Boolean durable,
                                       Long ttl,
                                       Long expiration,
                                       Integer priority,
                                       HttpHeaders headers,
                                       UriInfo uriInfo,
                                       byte[] body) {
      String matched = uriInfo.getMatchedURIs().get(1);
      UriBuilder nextBuilder = uriInfo.getBaseUriBuilder();
      String nextId = generateDupId();
      nextBuilder.path(matched).path(nextId);
      URI next = nextBuilder.build();

      boolean isDurable = defaultDurable;
      if (durable != null) {
         isDurable = durable.booleanValue();
      }
      try {
         publish(headers, body, dupId, isDurable, ttl, expiration, priority);
      } catch (Exception e) {
         Response error = Response.serverError().entity("Problem posting message: " + e.getMessage()).type("text/plain").build();
         throw new WebApplicationException(e, error);
      }
      Response.ResponseBuilder builder = Response.status(201);
      serviceManager.getLinkStrategy().setLinkHeader(builder, "create-next", "create-next", next.toString(), "*/*");
      return builder.build();
   }

   public long getProducerTimeToLive() {
      return producerTimeToLive;
   }

   public void setProducerTimeToLive(long producerTimeToLive) {
      this.producerTimeToLive = producerTimeToLive;
   }

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

   public boolean isDefaultDurable() {
      return defaultDurable;
   }

   public void setDefaultDurable(boolean defaultDurable) {
      this.defaultDurable = defaultDurable;
   }

   public int getPoolSize() {
      return poolSize;
   }

   public void setPoolSize(int poolSize) {
      this.poolSize = poolSize;
   }

   public void init() throws Exception {
      pool = new ArrayBlockingQueue<>(poolSize);
      for (int i = 0; i < poolSize; i++) {
         addPooled();
      }
   }

   protected void addPooled() throws ActiveMQException {
      ClientSession session = sessionFactory.createSession();
      ClientProducer producer = session.createProducer(destination);
      session.start();
      pool.add(new Pooled(session, producer));
   }

   protected Pooled getPooled() throws InterruptedException {
      Pooled pooled = pool.poll(1, TimeUnit.SECONDS);
      if (pooled == null) {
         throw new WebApplicationException(Response.status(503).entity("Timed out waiting for available producer.").type("text/plain").build());
      }
      return pooled;
   }

   public void cleanup() {
      for (Pooled pooled : pool) {
         try {
            pooled.session.close();
         } catch (ActiveMQException e) {
            throw new RuntimeException(e);
         }
      }
   }

   protected ClientMessage createActiveMQMessage(HttpHeaders headers,
                                                 byte[] body,
                                                 boolean durable,
                                                 Long ttl,
                                                 Long expiration,
                                                 Integer priority,
                                                 ClientSession session) throws Exception {
      ClientMessage message = session.createMessage(Message.BYTES_TYPE, durable);

      // HORNETQ-962
      UUID uid = UUIDGenerator.getInstance().generateUUID();
      message.setUserID(uid);

      if (expiration != null) {
         message.setExpiration(expiration.longValue());
      } else if (ttl != null) {
         message.setExpiration(System.currentTimeMillis() + ttl.longValue());
      } else if (producerTimeToLive > 0) {
         message.setExpiration(System.currentTimeMillis() + producerTimeToLive);
      }
      if (priority != null) {
         byte p = priority.byteValue();
         if (p >= 0 && p <= 9) {
            message.setPriority(p);
         }
      }
      HttpMessageHelper.writeHttpMessage(headers, body, message);
      return message;
   }
}
