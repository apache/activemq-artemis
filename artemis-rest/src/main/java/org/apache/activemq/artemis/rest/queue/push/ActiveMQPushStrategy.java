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
package org.apache.activemq.artemis.rest.queue.push;

import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlHttpHeader;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.specimpl.ResteasyUriBuilder;
import org.jboss.resteasy.spi.Link;

/**
 * Forwarding to an ActiveMQ/REST-* endpoing
 */
public class ActiveMQPushStrategy extends UriTemplateStrategy {

   protected boolean initialized = false;

   @Override
   public void start() throws Exception {
      // initialize();
   }

   protected void initialize() throws Exception {
      super.start();
      initialized = true;
      initAuthentication();
      ClientRequest request = executor.createRequest(registration.getTarget().getHref());
      for (XmlHttpHeader header : registration.getHeaders()) {
         request.header(header.getName(), header.getValue());
      }
      ClientResponse<?> res = request.head();
      if (res.getStatus() != 200) {
         throw new RuntimeException("Failed to query REST destination for init information.  Status: " + res.getStatus());
      }
      String url = (String) res.getHeaders().getFirst("msg-create-with-id");
      if (url == null) {
         if (res.getLinkHeader() == null) {
            throw new RuntimeException("Could not find create-with-id URL");
         }
         Link link = res.getLinkHeader().getLinkByTitle("create-with-id");
         if (link == null) {
            throw new RuntimeException("Could not find create-with-id URL");
         }
         url = link.getHref();
      }
      targetUri = ResteasyUriBuilder.fromTemplate(url);
   }

   @Override
   public boolean push(ClientMessage message) {
      // we initialize lazily just in case target is in same VM
      if (!initialized) {
         try {
            initialize();
            initialized = true;
         } catch (Exception e) {
            throw new RuntimeException("Failed to initialize.", e);
         }
      }
      return super.push(message);
   }
}
