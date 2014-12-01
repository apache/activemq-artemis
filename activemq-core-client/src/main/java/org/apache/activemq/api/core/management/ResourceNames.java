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
package org.apache.activemq.api.core.management;

/**
 * Helper class used to build resource names used by management messages.
 * <br>
 * Resource's name is build by appending its <em>name</em> to its corresponding type.
 * For example, the resource name of the "foo" queue is {@code CORE_QUEUE + "foo"}.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public final class ResourceNames
{

   public static final String CORE_SERVER = "core.server";

   public static final String CORE_QUEUE = "core.queue.";

   public static final String CORE_ADDRESS = "core.address.";

   public static final String CORE_BRIDGE = "core.bridge.";

   public static final String CORE_ACCEPTOR = "core.acceptor.";

   public static final String CORE_DIVERT = "core.divert.";

   public static final String CORE_CLUSTER_CONNECTION = "core.clusterconnection.";

   public static final String CORE_BROADCAST_GROUP = "core.broadcastgroup.";

   public static final String CORE_DISCOVERY_GROUP = "core.discovery.";

   public static final String JMS_SERVER = "jms.server";

   public static final String JMS_QUEUE = "jms.queue.";

   public static final String JMS_TOPIC = "jms.topic.";

   public static final String JMS_CONNECTION_FACTORY = "jms.connectionfactory.";

   private ResourceNames()
   {
   }

}
