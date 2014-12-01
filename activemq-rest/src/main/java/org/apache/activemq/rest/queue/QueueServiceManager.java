/**
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
package org.apache.activemq.rest.queue;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.rest.queue.push.FilePushStore;
import org.apache.activemq.rest.queue.push.PushStore;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class QueueServiceManager extends DestinationServiceManager
{
   protected PushStore pushStore;
   protected List<QueueDeployment> queues = new ArrayList<QueueDeployment>();
   protected QueueDestinationsResource destination;

   public List<QueueDeployment> getQueues()
   {
      return queues;
   }

   public void setQueues(List<QueueDeployment> queues)
   {
      this.queues = queues;
   }

   public PushStore getPushStore()
   {
      return pushStore;
   }

   public void setPushStore(PushStore pushStore)
   {
      this.pushStore = pushStore;
   }

   public QueueDestinationsResource getDestination()
   {
      return destination;
   }

   public void setDestination(QueueDestinationsResource destination)
   {
      this.destination = destination;
   }

   @Override
   public void start() throws Exception
   {
      initDefaults();

      destination = new QueueDestinationsResource(this);

      started = true;

      if (pushStoreFile != null && pushStore == null)
      {
         pushStore = new FilePushStore(pushStoreFile);
      }

      for (QueueDeployment queueDeployment : queues)
      {
         deploy(queueDeployment);
      }
   }

   public void deploy(QueueDeployment queueDeployment) throws Exception
   {
      if (!started)
      {
         throw new Exception("You must start() this class instance before deploying");
      }
      String queueName = queueDeployment.getName();
      ClientSession session = sessionFactory.createSession(false, false, false);
      ClientSession.QueueQuery query = session.queueQuery(new SimpleString(queueName));
      if (!query.isExists())
      {
         session.createQueue(queueName, queueName, queueDeployment.isDurableSend());
      }
      session.close();

      destination.createQueueResource(queueName, queueDeployment.isDurableSend(), queueDeployment.getConsumerSessionTimeoutSeconds(), queueDeployment.isDuplicatesAllowed());

   }

   @Override
   public void stop()
   {
      if (started == false) return;
      for (QueueResource queue : destination.getQueues().values())
      {
         queue.stop();
      }
      try
      {
         timeoutTask.stop();
         sessionFactory.close();
      }
      catch (Exception e)
      {
      }
   }
}
