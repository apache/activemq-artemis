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
package org.hornetq.api.core.management;

/**
 * Types of notification emitted by HornetQ servers.
 * <p>
 * These notifications can be received through:
 * <ul>
 * <li>JMX' MBeans subscriptions
 * <li>Core messages to a notification address (default value is {@code hornetq.notifications})
 * <li>JMS messages
 * </ul>
 * @see the HornetQ user manual section on "Management Notifications"
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public enum NotificationType
{
   BINDING_ADDED(0),
   BINDING_REMOVED(1),
   CONSUMER_CREATED(2),
   CONSUMER_CLOSED(3),
   SECURITY_AUTHENTICATION_VIOLATION(6),
   SECURITY_PERMISSION_VIOLATION(7),
   DISCOVERY_GROUP_STARTED(8),
   DISCOVERY_GROUP_STOPPED(9),
   BROADCAST_GROUP_STARTED(10),
   BROADCAST_GROUP_STOPPED(11),
   BRIDGE_STARTED(12),
   BRIDGE_STOPPED(13),
   CLUSTER_CONNECTION_STARTED(14),
   CLUSTER_CONNECTION_STOPPED(15),
   ACCEPTOR_STARTED(16),
   ACCEPTOR_STOPPED(17),
   PROPOSAL(18),
   PROPOSAL_RESPONSE(19),
   UNPROPOSAL(20);

   private final int value;

   private NotificationType(final int value)
   {
      this.value = value;
   }

   public int intValue()
   {
      return value;
   }
}