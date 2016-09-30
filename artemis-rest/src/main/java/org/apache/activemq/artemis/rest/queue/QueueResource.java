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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumerResource;

public class QueueResource extends DestinationResource {

   protected ConsumersResource consumers;
   protected PushConsumerResource pushConsumers;
   private QueueDestinationsResource queueDestinationsResource;

   public void start() throws Exception {
   }

   public void stop() {
      consumers.stop();
      pushConsumers.stop();
      sender.cleanup();
   }

   @GET
   @Produces("application/xml")
   public Response get(@Context UriInfo uriInfo, @Context HttpServletRequest requestContext) {
      ActiveMQRestLogger.LOGGER.debug("Handling GET request for \"" + destination + "\" from " + requestContext.getRemoteAddr() + ":" + requestContext.getRemotePort());

      StringBuilder msg = new StringBuilder();
      msg.append("<queue>").append("<name>").append(destination).append("</name>").append("<atom:link rel=\"create\" href=\"").append(createSenderLink(uriInfo)).append("\"/>").append("<atom:link rel=\"create-with-id\" href=\"").append(createSenderWithIdLink(uriInfo)).append("\"/>").append("<atom:link rel=\"pull-consumers\" href=\"").append(createConsumersLink(uriInfo)).append("\"/>").append("<atom:link rel=\"push-consumers\" href=\"").append(createPushConsumersLink(uriInfo)).append("\"/>")

         .append("</queue>");

      Response.ResponseBuilder builder = Response.ok(msg.toString());
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setConsumersLink(builder, uriInfo);
      setPushConsumersLink(builder, uriInfo);
      return builder.build();
   }

   @HEAD
   @Produces("application/xml")
   public Response head(@Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling HEAD request for \"" + uriInfo.getRequestUri() + "\"");

      Response.ResponseBuilder builder = Response.ok();
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setConsumersLink(builder, uriInfo);
      setPushConsumersLink(builder, uriInfo);
      return builder.build();
   }

   protected void setSenderLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createSenderLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "create", "create", uri, null);
   }

   protected String createSenderLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("create");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setSenderWithIdLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createSenderWithIdLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "create-with-id", "create-with-id", uri, null);
   }

   protected String createSenderWithIdLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("create");
      String uri = builder.build().toString();
      uri += "/{id}";
      return uri;
   }

   protected void setConsumersLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createConsumersLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "pull-consumers", "pull-consumers", uri, null);
   }

   protected String createConsumersLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("pull-consumers");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setPushConsumersLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createPushConsumersLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "push-consumers", "push-consumers", uri, null);
   }

   protected String createPushConsumersLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("push-consumers");
      String uri = builder.build().toString();
      return uri;
   }

   public void setConsumers(ConsumersResource consumers) {
      this.consumers = consumers;
   }

   @Path("create")
   public PostMessage post() throws Exception {
      return sender;
   }

   @Path("pull-consumers")
   public ConsumersResource getConsumers() {
      return consumers;
   }

   public void setPushConsumers(PushConsumerResource pushConsumers) {
      this.pushConsumers = pushConsumers;
   }

   @Path("push-consumers")
   public PushConsumerResource getPushConsumers() {
      return pushConsumers;
   }

   public void setQueueDestinationsResource(QueueDestinationsResource queueDestinationsResource) {
      this.queueDestinationsResource = queueDestinationsResource;
   }

   @DELETE
   public void deleteQueue(@Context UriInfo uriInfo) throws Exception {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      queueDestinationsResource.getQueues().remove(destination);
      stop();

      ClientSession session = serviceManager.getSessionFactory().createSession(false, false, false);
      try {
         SimpleString queueName = new SimpleString(destination);
         session.deleteQueue(queueName);
      } finally {
         try {
            session.close();
         } catch (Exception ignored) {
         }
      }
   }

}
