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
package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.server.LiveNodeLocator;

public class NamedNodeIdNodeLocator extends LiveNodeLocator {

   private final String nodeID;

   private final Pair<TransportConfiguration, TransportConfiguration> liveConfiguration;

   public NamedNodeIdNodeLocator(String nodeID,
                                 Pair<TransportConfiguration, TransportConfiguration> liveConfiguration) {
      this.nodeID = nodeID;
      this.liveConfiguration = liveConfiguration;
   }

   @Override
   public void locateNode(long timeout) throws ActiveMQException {
      //noop
   }

   @Override
   public void locateNode() throws ActiveMQException {
      //noop
   }

   @Override
   public Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration() {
      return liveConfiguration;
   }

   @Override
   public String getNodeID() {
      return nodeID;
   }

   @Override
   public void nodeUP(TopologyMember member, boolean last) {

   }

   @Override
   public void nodeDown(long eventUID, String nodeID) {

   }
}
