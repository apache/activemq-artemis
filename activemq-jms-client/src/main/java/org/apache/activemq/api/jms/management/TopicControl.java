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
package org.apache.activemq.api.jms.management;

import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.apache.activemq.api.core.management.Operation;
import org.apache.activemq.api.core.management.Parameter;

/**
 * A TopicControl is used to manage a JMS Topic.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface TopicControl extends DestinationControl
{

   /**
    * Returns the number of (durable and non-durable) subscribers for this topic.
    */
   int getSubscriptionCount();

   /**
    * Returns the number of <em>durable</em> subscribers for this topic.
    */
   int getDurableSubscriptionCount();

   /**
    * Returns the number of <em>non-durable</em> subscribers for this topic.
    */
   int getNonDurableSubscriptionCount();

   /**
    * Returns the number of messages for all <em>durable</em> subscribers for this topic.
    */
   int getDurableMessageCount();

   /**
    * Returns the number of messages for all <em>non-durable</em> subscribers for this topic.
    */
   int getNonDurableMessageCount();

   /**
    * Returns the JNDI bindings associated  to this connection factory.
    */
   @Operation(desc = "Returns the list of JNDI bindings associated")
   String[] getJNDIBindings();

   /**
    * Add the JNDI binding to this destination
    */
   @Operation(desc = "Adds the queue to another JNDI binding")
   void addJNDI(@Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI") String jndi) throws Exception;



   // Operations ----------------------------------------------------

   /**
    * Lists all the subscriptions for this topic (both durable and non-durable).
    */
   @Operation(desc = "List all subscriptions")
   Object[] listAllSubscriptions() throws Exception;

   /**
    * Lists all the subscriptions for this topic (both durable and non-durable) using JSON serialization.
    * <br>
    * Java objects can be recreated from JSON serialization using {@link SubscriptionInfo#from(String)}.
    */
   @Operation(desc = "List all subscriptions")
   String listAllSubscriptionsAsJSON() throws Exception;

   /**
    * Lists all the <em>durable</em> subscriptions for this topic.
    */
   @Operation(desc = "List only the durable subscriptions")
   Object[] listDurableSubscriptions() throws Exception;

   /**
    * Lists all the <em>durable</em> subscriptions  using JSON serialization.
    * <br>
    * Java objects can be recreated from JSON serialization using {@link SubscriptionInfo#from(String)}.
    */
   @Operation(desc = "List only the durable subscriptions")
   String listDurableSubscriptionsAsJSON() throws Exception;

   /**
    * Lists all the <em>non-durable</em> subscriptions for this topic.
    */
   @Operation(desc = "List only the non durable subscriptions")
   Object[] listNonDurableSubscriptions() throws Exception;

   /**
    * Lists all the <em>non-durable</em> subscriptions  using JSON serialization.
    * <br>
    * Java objects can be recreated from JSON serialization using {@link SubscriptionInfo#from(String)}.
    */
   @Operation(desc = "List only the non durable subscriptions")
   String listNonDurableSubscriptionsAsJSON() throws Exception;

   /**
    * Lists all the messages in this queue matching the specified queue representing the subscription.
    * <br>
    * 1 Map represents 1 message, keys are the message's properties and headers, values are the corresponding values.
    */
   @Operation(desc = "List all the message for the given subscription")
   Map<String, Object>[] listMessagesForSubscription(@Parameter(name = "queueName", desc = "the name of the queue representing a subscription") String queueName) throws Exception;

   /**
    * Lists all the messages in this queue matching the specified queue representing the subscription using JSON serialization.
    */
   @Operation(desc = "List all the message for the given subscription")
   String listMessagesForSubscriptionAsJSON(@Parameter(name = "queueName", desc = "the name of the queue representing a subscription") String queueName) throws Exception;

   /**
    * Counts the number of messages in the subscription specified by the specified client ID and subscription name. Only messages matching the filter will be counted.
    * <br>
    * Using {@code null} or an empty filter will count <em>all</em> messages from this queue.
    */
   @Operation(desc = "Count the number of messages matching the filter for the given subscription")
   int countMessagesForSubscription(@Parameter(name = "clientID", desc = "the client ID") String clientID,
                                           @Parameter(name = "subscriptionName", desc = "the name of the durable subscription") String subscriptionName,
                                           @Parameter(name = "filter", desc = "a JMS filter (can be empty)") String filter) throws Exception;

   /**
    * Drops the subscription specified by the specified client ID and subscription name.
    */
   @Operation(desc = "Drop a durable subscription", impact = MBeanOperationInfo.ACTION)
   void dropDurableSubscription(@Parameter(name = "clientID", desc = "the client ID") String clientID,
                                @Parameter(name = "subscriptionName", desc = "the name of the durable subscription") String subscriptionName) throws Exception;

   /**
    * Drops all subscriptions.
    */
   @Operation(desc = "Drop all subscriptions from this topic", impact = MBeanOperationInfo.ACTION)
   void dropAllSubscriptions() throws Exception;
}
