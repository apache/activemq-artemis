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
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumerResource;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.util.Constants;
import org.w3c.dom.Document;

@Path(Constants.PATH_FOR_QUEUES)
public class QueueDestinationsResource {

   private final Map<String, QueueResource> queues = new ConcurrentHashMap<>();
   private final QueueServiceManager manager;

   public QueueDestinationsResource(QueueServiceManager manager) {
      this.manager = manager;
   }

   @POST
   @Consumes("application/activemq.jms.queue+xml")
   public Response createJmsQueue(@Context UriInfo uriInfo, Document document) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      try {
         JMSQueueConfiguration queue = FileJMSConfiguration.parseQueueConfiguration(document.getDocumentElement());
         ActiveMQQueue activeMQQueue = ActiveMQDestination.createQueue(queue.getName());
         String queueName = activeMQQueue.getAddress();
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try {

            ClientSession.QueueQuery query = session.queueQuery(new SimpleString(queueName));
            if (!query.isExists()) {
               if (queue.getSelector() != null) {
                  session.createQueue(queueName, queueName, queue.getSelector(), queue.isDurable());
               } else {
                  session.createQueue(queueName, queueName, queue.isDurable());
               }

            } else {
               throw new WebApplicationException(Response.status(412).type("text/plain").entity("Queue already exists.").build());
            }
         } finally {
            try {
               session.close();
            } catch (Exception ignored) {
            }
         }
         URI uri = uriInfo.getRequestUriBuilder().path(queueName).build();
         return Response.created(uri).build();
      } catch (Exception e) {
         if (e instanceof WebApplicationException)
            throw (WebApplicationException) e;
         throw new WebApplicationException(e, Response.serverError().type("text/plain").entity("Failed to create queue.").build());
      }
   }

   public Map<String, QueueResource> getQueues() {
      return queues;
   }

   @Path("/{queue-name}")
   public synchronized QueueResource findQueue(@PathParam("queue-name") String name) throws Exception {
      QueueResource queue = queues.get(name);
      if (queue == null) {
         String queueName = name;
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try {
            ClientSession.QueueQuery query = session.queueQuery(new SimpleString(queueName));
            if (!query.isExists()) {
               throw new WebApplicationException(Response.status(404).type("text/plain").entity("Queue '" + name + "' does not exist").build());
            }
            DestinationSettings queueSettings = manager.getDefaultSettings();
            boolean defaultDurable = queueSettings.isDurableSend() || query.isDurable();

            queue = createQueueResource(queueName, defaultDurable, queueSettings.getConsumerSessionTimeoutSeconds(), queueSettings.isDuplicatesAllowed());
         } finally {
            try {
               session.close();
            } catch (ActiveMQException e) {
            }
         }
      }
      return queue;
   }

   public QueueResource createQueueResource(String queueName,
                                            boolean defaultDurable,
                                            int timeoutSeconds,
                                            boolean duplicates) throws Exception {
      QueueResource queueResource = new QueueResource();
      queueResource.setQueueDestinationsResource(this);
      queueResource.setDestination(queueName);
      queueResource.setServiceManager(manager);

      ConsumersResource consumers = new ConsumersResource();
      consumers.setConsumerTimeoutSeconds(timeoutSeconds);
      consumers.setDestination(queueName);
      consumers.setSessionFactory(manager.getConsumerSessionFactory());
      consumers.setServiceManager(manager);
      queueResource.setConsumers(consumers);

      PushConsumerResource push = new PushConsumerResource();
      push.setDestination(queueName);
      push.setSessionFactory(manager.getConsumerSessionFactory());
      push.setJmsOptions(manager.getJmsOptions());
      queueResource.setPushConsumers(push);

      PostMessage sender = null;
      if (duplicates) {
         sender = new PostMessageDupsOk();
      } else {
         sender = new PostMessageNoDups();
      }
      sender.setServiceManager(manager);
      sender.setDefaultDurable(defaultDurable);
      sender.setDestination(queueName);
      sender.setSessionFactory(manager.getSessionFactory());
      sender.setPoolSize(manager.getProducerPoolSize());
      sender.setProducerTimeToLive(manager.getProducerTimeToLive());
      sender.init();
      queueResource.setSender(sender);

      if (manager.getPushStore() != null) {
         push.setPushStore(manager.getPushStore());
         List<PushRegistration> regs = manager.getPushStore().getByDestination(queueName);
         for (PushRegistration reg : regs) {
            push.addRegistration(reg);
         }
      }

      queueResource.start();
      getQueues().put(queueName, queueResource);
      return queueResource;
   }
}
