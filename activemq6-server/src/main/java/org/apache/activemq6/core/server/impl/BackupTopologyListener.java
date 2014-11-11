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
package org.apache.activemq6.core.server.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq6.api.core.client.ClusterTopologyListener;
import org.apache.activemq6.api.core.client.TopologyMember;

final class BackupTopologyListener implements ClusterTopologyListener
{

   private final CountDownLatch latch = new CountDownLatch(1);
   private final String ownId;
   private static final int WAIT_TIMEOUT = 60;

   public BackupTopologyListener(String ownId)
   {
      this.ownId = ownId;
   }

   @Override
   public void nodeUP(TopologyMember topologyMember, boolean last)
   {
      final String nodeID = topologyMember.getNodeId();

      if (ownId.equals(nodeID) && topologyMember.getBackup() != null)
         latch.countDown();
   }

   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      // no-op
   }

   boolean waitForBackup()
   {
      try
      {
         return latch.await(WAIT_TIMEOUT, TimeUnit.SECONDS);
      }
      catch (InterruptedException e)
      {
         return false;
      }
   }
}
