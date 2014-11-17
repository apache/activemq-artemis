/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.rest.queue;

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

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.jms.client.HornetQDestination;
import org.apache.activemq6.jms.client.HornetQQueue;
import org.apache.activemq6.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq6.jms.server.impl.JMSServerConfigParserImpl;
import org.apache.activemq6.rest.HornetQRestLogger;
import org.apache.activemq6.rest.queue.push.PushConsumerResource;
import org.apache.activemq6.rest.queue.push.xml.PushRegistration;
import org.apache.activemq6.rest.util.Constants;
import org.w3c.dom.Document;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@Path(Constants.PATH_FOR_QUEUES)
public class QueueDestinationsResource
{
   private Map<String, QueueResource> queues = new ConcurrentHashMap<String, QueueResource>();
   private QueueServiceManager manager;

   public QueueDestinationsResource(QueueServiceManager manager)
   {
      this.manager = manager;
   }

   @POST
   @Consumes("application/hornetq.jms.queue+xml")
   public Response createJmsQueue(@Context UriInfo uriInfo, Document document)
   {
      HornetQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      try
      {
         JMSServerConfigParserImpl parser = new JMSServerConfigParserImpl();
         JMSQueueConfiguration queue = parser.parseQueueConfiguration(document.getDocumentElement());
         HornetQQueue hqQueue = HornetQDestination.createQueue(queue.getName());
         String queueName = hqQueue.getAddress();
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try
         {

            ClientSession.QueueQuery query = session.queueQuery(new SimpleString(queueName));
            if (!query.isExists())
            {
               if (queue.getSelector() != null)
               {
                  session.createQueue(queueName, queueName, queue.getSelector(), queue.isDurable());
               }
               else
               {
                  session.createQueue(queueName, queueName, queue.isDurable());
               }

            }
            else
            {
               throw new WebApplicationException(Response.status(412).type("text/plain").entity("Queue already exists.").build());
            }
         }
         finally
         {
            try
            {
               session.close();
            }
            catch (Exception ignored)
            {
            }
         }
         if (queue.getBindings() != null && queue.getBindings().length > 0 && manager.getRegistry() != null)
         {
            for (String binding : queue.getBindings())
            {
               manager.getRegistry().bind(binding, hqQueue);
            }
         }
         URI uri = uriInfo.getRequestUriBuilder().path(queueName).build();
         return Response.created(uri).build();
      }
      catch (Exception e)
      {
         if (e instanceof WebApplicationException) throw (WebApplicationException) e;
         throw new WebApplicationException(e, Response.serverError().type("text/plain").entity("Failed to create queue.").build());
      }
   }

   public Map<String, QueueResource> getQueues()
   {
      return queues;
   }

   @Path("/{queue-name}")
   public synchronized QueueResource findQueue(@PathParam("queue-name") String name) throws Exception
   {
      QueueResource queue = queues.get(name);
      if (queue == null)
      {
         String queueName = name;
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try
         {
            ClientSession.QueueQuery query = session.queueQuery(new SimpleString(queueName));
            if (!query.isExists())
            {
               throw new WebApplicationException(Response.status(404).type("text/plain").entity("Queue '" + name + "' does not exist").build());
            }
            DestinationSettings queueSettings = manager.getDefaultSettings();
            boolean defaultDurable = queueSettings.isDurableSend() || query.isDurable();

            queue = createQueueResource(queueName, defaultDurable, queueSettings.getConsumerSessionTimeoutSeconds(), queueSettings.isDuplicatesAllowed());
         }
         finally
         {
            try
            {
               session.close();
            }
            catch (HornetQException e)
            {
            }
         }
      }
      return queue;
   }

   public QueueResource createQueueResource(String queueName, boolean defaultDurable, int timeoutSeconds, boolean duplicates) throws Exception
   {
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
      queueResource.setPushConsumers(push);

      PostMessage sender = null;
      if (duplicates)
      {
         sender = new PostMessageDupsOk();
      }
      else
      {
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

      if (manager.getPushStore() != null)
      {
         push.setPushStore(manager.getPushStore());
         List<PushRegistration> regs = manager.getPushStore().getByDestination(queueName);
         for (PushRegistration reg : regs)
         {
            push.addRegistration(reg);
         }
      }

      queueResource.start();
      getQueues().put(queueName, queueResource);
      return queueResource;
   }
}
