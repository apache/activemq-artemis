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
package org.apache.activemq.core.server.impl;


import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.TopologyMember;
import org.apache.activemq.core.server.LiveNodeLocator;

public class NamedNodeIdNodeLocator extends LiveNodeLocator
{
   private final String nodeID;

   private final Pair<TransportConfiguration, TransportConfiguration> liveConfiguration;

   public NamedNodeIdNodeLocator(String nodeID, Pair<TransportConfiguration, TransportConfiguration> liveConfiguration)
   {
      this.nodeID = nodeID;
      this.liveConfiguration = liveConfiguration;
   }


   @Override
   public void locateNode(long timeout) throws HornetQException
   {
      //noop
   }

   @Override
   public void locateNode() throws HornetQException
   {
      //noop
   }

   @Override
   public Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration()
   {
      return liveConfiguration;
   }

   @Override
   public String getNodeID()
   {
      return nodeID;
   }

   @Override
   public void nodeUP(TopologyMember member, boolean last)
   {

   }

   @Override
   public void nodeDown(long eventUID, String nodeID)
   {

   }
}
