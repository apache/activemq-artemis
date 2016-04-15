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
package org.apache.activemq.artemis.api.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.jgroups.JChannelManager;
import org.jboss.logging.Logger;
import org.jgroups.JChannel;

/**
 * An implementation of BroadcastEndpointFactory that uses an externally managed JChannel for JGroups clustering.
 *
 * Note - the underlying JChannel is not closed in this implementation.
 */
public class ChannelBroadcastEndpointFactory implements BroadcastEndpointFactory {

   private static final Logger logger = Logger.getLogger(ChannelBroadcastEndpointFactory.class);
   private static final boolean isTrace = logger.isTraceEnabled();

   private final JChannel channel;

   private final String channelName;

   private final JChannelManager manager;

   private static final Map<JChannel, JChannelManager> managers = new ConcurrentHashMap<>();

   private static final JChannelManager singletonManager = new JChannelManager();
//  TODO: To implement this when JForkChannel from JGroups supports multiple channels properly
//
//   private static JChannelManager recoverManager(JChannel channel) {
//      JChannelManager manager = managers.get(channel);
//      if (manager == null) {
//         if (isTrace) {
//            logger.trace("Creating a new JChannelManager for " + channel, new Exception("trace"));
//         }
//         manager = new JChannelManager();
//         managers.put(channel, manager);
//      }
//      else {
//         if (isTrace) {
//            logger.trace("Recover an already existent channelManager for " + channel, new Exception("trace"));
//         }
//
//      }
//
//      return manager;
//   }
//
   public ChannelBroadcastEndpointFactory(JChannel channel, String channelName) {
      // TODO: use recoverManager(channel)
      this(singletonManager, channel, channelName);
   }

   private ChannelBroadcastEndpointFactory(JChannelManager manager, JChannel channel, String channelName) {
      if (isTrace) {
         logger.trace("new ChannelBroadcastEndpointFactory(" + manager + ", " + channel + ", " + channelName, new Exception("trace"));
      }
      this.manager = manager;
      this.channel = channel;
      this.channelName = channelName;
   }

   public JChannel getChannel() {
      return channel;
   }

   public String getChannelName() {
      return channelName;
   }

   @Override
   public String toString() {
      return "ChannelBroadcastEndpointFactory{" +
         "channel=" + channel +
         ", channelName='" + channelName + '\'' +
         ", manager=" + manager +
         '}';
   }

   @Override
   public BroadcastEndpoint createBroadcastEndpoint() throws Exception {
      return new JGroupsChannelBroadcastEndpoint(manager, channel, channelName).initChannel();
   }
}
