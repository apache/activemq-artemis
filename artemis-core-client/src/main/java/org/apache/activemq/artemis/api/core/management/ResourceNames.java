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
package org.apache.activemq.artemis.api.core.management;

/**
 * Helper class used to build resource names used by management messages.
 * <br>
 * Resource's name is build by appending its <em>name</em> to its corresponding type.
 * For example, the resource name of the "foo" queue is {@code QUEUE + "foo"}.
 */
public final class ResourceNames {
   public static final String BROKER = "broker";

   public static final String QUEUE = "queue.";

   public static final String ADDRESS = "address.";

   public static final String BRIDGE = "bridge.";

   public static final String ACCEPTOR = "acceptor.";

   public static final String DIVERT = "divert.";

   public static final String CORE_CLUSTER_CONNECTION = "clusterconnection.";

   public static final String BROADCAST_GROUP = "broadcastgroup.";

   private ResourceNames() {
   }

}
