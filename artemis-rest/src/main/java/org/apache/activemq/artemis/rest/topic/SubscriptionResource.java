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
package org.apache.activemq.artemis.rest.topic;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.rest.queue.DestinationServiceManager;
import org.apache.activemq.artemis.rest.queue.QueueConsumer;

public class SubscriptionResource extends QueueConsumer implements Subscription {

   protected boolean durable;
   protected long timeout;
   private boolean deleteWhenIdle;

   public SubscriptionResource(ClientSessionFactory factory,
                               String destination,
                               String id,
                               DestinationServiceManager serviceManager,
                               String selector,
                               boolean durable,
                               long timeout) throws ActiveMQException {
      super(factory, destination, id, serviceManager, selector);
      this.durable = durable;
      this.timeout = timeout;
   }

   public boolean isDurable() {
      return durable;
   }

   public void setDurable(boolean durable) {
      this.durable = durable;
   }

   public long getTimeout() {
      return timeout;
   }

   public void setTimeout(long timeout) {
      this.timeout = timeout;
   }

   public boolean isDeleteWhenIdle() {
      return deleteWhenIdle;
   }

   public void setDeleteWhenIdle(boolean deleteWhenIdle) {
      this.deleteWhenIdle = deleteWhenIdle;
   }
}
