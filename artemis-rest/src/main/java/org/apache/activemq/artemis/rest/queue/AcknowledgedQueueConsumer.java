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

import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.util.Constants;
import org.apache.activemq.artemis.rest.util.LinkStrategy;

public class AcknowledgedQueueConsumer extends QueueConsumer {

   protected long counter;
   protected String startup = Long.toString(System.currentTimeMillis());
   protected volatile Acknowledgement ack;

   public AcknowledgedQueueConsumer(ClientSessionFactory factory,
                                    String destination,
                                    String id,
                                    DestinationServiceManager serviceManager,
                                    String selector) throws ActiveMQException {
      super(factory, destination, id, serviceManager, selector);
      autoAck = false;
   }

   public synchronized Acknowledgement getAck() {
      return ack;
   }

   @Override
   @Path("acknowledge-next{index}")
   @POST
   public synchronized Response poll(@HeaderParam(Constants.WAIT_HEADER) @DefaultValue("0") long wait,
                                     @PathParam("index") long index,
                                     @Context UriInfo info) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + info.getPath() + "\"");

      if (closed) {
         UriBuilder builder = info.getBaseUriBuilder();
         String path = info.getMatchedURIs().get(1);
         builder.path(path).path("acknowledge-next");
         String uri = builder.build().toString();

         // redirect to another acknowledge-next

         return Response.status(307).location(URI.create(uri)).build();
      }
      return checkIndexAndPoll(wait, info, info.getMatchedURIs().get(1), index);
   }

   @Override
   public synchronized void shutdown() {
      super.shutdown();
      if (ack != null) {
         ack = null;
      }
   }

   @Path("acknowledgement/{ackToken}")
   @POST
   public synchronized Response acknowledge(@PathParam("ackToken") String ackToken,
                                            @FormParam("acknowledge") boolean doAcknowledge,
                                            @Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      ping(0);
      String basePath = uriInfo.getMatchedURIs().get(1);
      if (closed) {
         Response.ResponseBuilder builder = Response.status(Response.Status.PRECONDITION_FAILED).entity("Could not acknowledge message, it was probably requeued from a timeout").type("text/plain");
         setAcknowledgeLinks(uriInfo, basePath, builder, "-1");
         return builder.build();
      }

      if (ack == null || !ack.getAckToken().equals(ackToken)) {
         Response.ResponseBuilder builder = Response.status(Response.Status.PRECONDITION_FAILED).entity("Could not acknowledge message, it was probably requeued from a timeout or you have an old link").type("text/plain");
         setAcknowledgeLinks(uriInfo, basePath, builder, "-1");
         return builder.build();
      }

      // clear indexes as we know the client got the message and won't send a duplicate ack-next
      previousIndex = -2;
      lastConsumed = null;

      if (ack.wasSet() && doAcknowledge != ack.isAcknowledged()) {
         StringBuilder msg = new StringBuilder("Could not ");
         if (doAcknowledge == false)
            msg.append("un");
         msg.append("acknowledge message because it has already been ");
         if (doAcknowledge == true)
            msg.append("un");
         msg.append("acknowledged");

         Response.ResponseBuilder builder = Response.status(Response.Status.PRECONDITION_FAILED).entity(msg.toString()).type("text/plain");
         setAcknowledgeLinks(uriInfo, basePath, builder, "-1");
         return builder.build();
      }

      if (ack.wasSet() && doAcknowledge == ack.isAcknowledged()) {
         Response.ResponseBuilder builder = Response.noContent();
         setAcknowledgeLinks(uriInfo, basePath, builder, "-1");
         return builder.build();
      }

      if (doAcknowledge) {
         try {
            ack.acknowledge();
            //System.out.println("Acknowledge message: " + ack.getMessage());
            ack.getMessage().acknowledge();
         } catch (ActiveMQException e) {
            throw new RuntimeException(e);
         }
      } else {
         ack.unacknowledge();
         unacknowledge();
      }
      Response.ResponseBuilder builder = Response.noContent();
      setAcknowledgeLinks(uriInfo, basePath, builder, "-1");
      return builder.build();
   }

   @Override
   protected ClientMessage receive(long timeoutSecs) throws Exception {
      ClientMessage msg = super.receive(timeoutSecs);
      return msg;
   }

   @Override
   protected ClientMessage receiveFromConsumer(long timeoutSecs) throws Exception {
      ClientMessage message = super.receiveFromConsumer(timeoutSecs);
      if (message != null) {
         ack = new Acknowledgement((counter++) + startup, message);
         //System.out.println("---> Setting ack: " + ack.getAckToken());
      }
      return message;
   }

   protected String getAckToken() {
      return ack.getAckToken();
   }

   protected void unacknowledge() {
      // we close current session so that message is redelivered
      // for temporary queues/topics, create a new session before closing old so we don't lose the temporary topic/queue

      ClientConsumer old = consumer;
      ClientSession oldSession = session;

      try {
         createSession();
      } catch (Exception e) {
         shutdown();
         throw new RuntimeException(e);

      } finally {
         try {
            old.close();
         } catch (ActiveMQException e) {
         }
         try {
            oldSession.close();
         } catch (ActiveMQException e) {
         }
      }
   }

   protected void setAcknowledgeLinks(UriInfo uriInfo,
                                      String basePath,
                                      Response.ResponseBuilder builder,
                                      String index) {
      setAcknowledgeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, basePath, index);
      setSessionLink(builder, uriInfo, basePath);
   }

   @Override
   protected void setMessageResponseLinks(UriInfo info,
                                          String basePath,
                                          Response.ResponseBuilder builder,
                                          String index) {
      setAcknowledgementLink(builder, info, basePath);
      setSessionLink(builder, info, basePath);
   }

   @Override
   protected void setPollTimeoutLinks(UriInfo info, String basePath, Response.ResponseBuilder builder, String index) {
      setAcknowledgeNextLink(serviceManager.getLinkStrategy(), builder, info, basePath, index);
      setSessionLink(builder, info, basePath);
   }

   public void setAcknowledgementLink(Response.ResponseBuilder response, UriInfo info, String basePath) {
      UriBuilder builder = info.getBaseUriBuilder();
      builder.path(basePath).path("acknowledgement").path(getAckToken());
      String uri = builder.build().toString();
      serviceManager.getLinkStrategy().setLinkHeader(response, "acknowledgement", "acknowledgement", uri, MediaType.APPLICATION_FORM_URLENCODED);
   }

   public static void setAcknowledgeNextLink(LinkStrategy linkStrategy,
                                             Response.ResponseBuilder response,
                                             UriInfo info,
                                             String basePath,
                                             String index) {
      if (index == null)
         throw new IllegalArgumentException("index cannot be null");
      UriBuilder builder = info.getBaseUriBuilder();
      builder.path(basePath).path("acknowledge-next" + index);
      String uri = builder.build().toString();
      linkStrategy.setLinkHeader(response, "acknowledge-next", "acknowledge-next", uri, MediaType.APPLICATION_FORM_URLENCODED);
   }

}
