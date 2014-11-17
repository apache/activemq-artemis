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
package org.apache.activemq.core.protocol.openwire.amq;

public class AMQConsumerBrokerExchange
{
   private AMQConnectionContext connectionContext;
   private AMQDestination regionDestination;
   private AMQSubscription subscription;
   private boolean wildcard;

   /**
    * @return the connectionContext
    */
   public AMQConnectionContext getConnectionContext()
   {
      return this.connectionContext;
   }

   /**
    * @param connectionContext
    *           the connectionContext to set
    */
   public void setConnectionContext(AMQConnectionContext connectionContext)
   {
      this.connectionContext = connectionContext;
   }

   /**
    * @return the regionDestination
    */
   public AMQDestination getRegionDestination()
   {
      return this.regionDestination;
   }

   /**
    * @param regionDestination
    *           the regionDestination to set
    */
   public void setRegionDestination(AMQDestination regionDestination)
   {
      this.regionDestination = regionDestination;
   }

   /**
    * @return the subscription
    */
   public AMQSubscription getSubscription()
   {
      return this.subscription;
   }

   /**
    * @param subscription
    *           the subscription to set
    */
   public void setSubscription(AMQSubscription subscription)
   {
      this.subscription = subscription;
   }

   /**
    * @return the wildcard
    */
   public boolean isWildcard()
   {
      return this.wildcard;
   }

   /**
    * @param wildcard
    *           the wildcard to set
    */
   public void setWildcard(boolean wildcard)
   {
      this.wildcard = wildcard;
   }
}
