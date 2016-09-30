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
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;

/**
 * Implements simple "create" link.  Returns 201 with Location of created resource as per HTTP
 */
public class PostMessageDupsOk extends PostMessage {

   public void publish(HttpHeaders headers,
                       byte[] body,
                       boolean durable,
                       Long ttl,
                       Long expiration,
                       Integer priority) throws Exception {
      Pooled pooled = getPooled();
      try {
         ClientProducer producer = pooled.producer;
         ClientMessage message = createActiveMQMessage(headers, body, durable, ttl, expiration, priority, pooled.session);
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

   @POST
   public Response create(@Context HttpHeaders headers,
                          @QueryParam("durable") Boolean durable,
                          @QueryParam("ttl") Long ttl,
                          @QueryParam("expiration") Long expiration,
                          @QueryParam("priority") Integer priority,
                          @Context UriInfo uriInfo,
                          byte[] body) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getRequestUri() + "\"");

      try {
         boolean isDurable = defaultDurable;
         if (durable != null) {
            isDurable = durable.booleanValue();
         }
         publish(headers, body, isDurable, ttl, expiration, priority);
      } catch (Exception e) {
         Response error = Response.serverError().entity("Problem posting message: " + e.getMessage()).type("text/plain").build();
         throw new WebApplicationException(e, error);
      }
      Response.ResponseBuilder builder = Response.status(201);
      UriBuilder nextBuilder = uriInfo.getAbsolutePathBuilder();
      URI next = nextBuilder.build();
      serviceManager.getLinkStrategy().setLinkHeader(builder, "create-next", "create-next", next.toString(), "*/*");
      return builder.build();
   }
}
