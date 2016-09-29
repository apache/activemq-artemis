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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.activemq.artemis.rest.ActiveMQRestLogger;

/**
 * implements reliable "create", "create-next" pattern defined by REST-* Messaging specification
 */
public class PostMessageNoDups extends PostMessage {

   @POST
   public Response redirectCreation(@Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      String id = generateDupId();
      Response.ResponseBuilder res = Response.status(Response.Status.TEMPORARY_REDIRECT.getStatusCode());
      res.location(uriInfo.getAbsolutePathBuilder().path(id).build());
      return res.build();
   }
}
