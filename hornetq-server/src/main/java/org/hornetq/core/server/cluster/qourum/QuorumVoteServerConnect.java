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

package org.hornetq.core.server.cluster.qourum;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.persistence.StorageManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A Qourum Vote for deciding if a replicated backup should become live.
 */
public class QuorumVoteServerConnect extends QuorumVote<BooleanVote, Boolean>
{
   private static final SimpleString LIVE_FAILOVER_VOTE = new SimpleString("LIVE_FAILOVER)VOTE");
   private final CountDownLatch latch;

   private double votesNeeded;

   private int total = 0;

   private boolean decision = false;

   /**
    * vote the remaining nodes not including ourself., so
    * 1 remaining nodes would be 0/2 = 0 vote needed
    * 2 remaining nodes would be 1/2 = 0 vote needed
    * 3 remaining nodes would be 2/2 = 1 vote needed
    * 4 remaining nodes would be 3/2 = 2 vote needed
    * 5 remaining nodes would be 4/2 = 3 vote needed
    * 6 remaining nodes would be 5/2 = 3 vote needed
    * */
   public QuorumVoteServerConnect(int size, StorageManager storageManager)
   {
      super(LIVE_FAILOVER_VOTE);
      //we don't count ourself
      int actualSize = size - 1;
      if (actualSize <= 2)
      {
         votesNeeded = actualSize / 2;
      }
      else
      {
         //even
         votesNeeded = actualSize / 2 + 1;
      }
      //votes needed could be say 2.5 so we add 1 in this case
      int latchSize = votesNeeded > (int) votesNeeded ? (int) votesNeeded + 1 : (int) votesNeeded;
      latch = new CountDownLatch(latchSize);
      if (votesNeeded == 0)
      {
         decision = true;
      }
   }

   /**
    * if we can connect to a node
    *
    * @return
    */
   @Override
   public Vote connected()
   {
      return new BooleanVote(true);
   }

   /**
    * if we cant connect to the node
    * @return
    */
   @Override
   public Vote notConnected()
   {
      return new BooleanVote(false);
   }

   /**
    * vote the remaining nodes not including ourself., so
    * 1 remaining nodes would be 0/2 = 0 vote needed
    * 2 remaining nodes would be 1/2 = 0 vote needed
    * 3 remaining nodes would be 2/2 = 1 vote needed
    * 4 remaining nodes would be 3/2 = 2 vote needed
    * 5 remaining nodes would be 4/2 = 3 vote needed
    * 6 remaining nodes would be 5/2 = 3 vote needed
    * @param vote the vote to make.
    */
   @Override
   public synchronized void vote(BooleanVote vote)
   {
      if (decision)
         return;
      if (vote.getVote())
      {
         total++;
         if (total >= votesNeeded)
         {
            decision = true;
            latch.countDown();
         }
      }
   }

   @Override
   public void allVotesCast(Topology voteTopology)
   {

   }

   @Override
   public Boolean getDecision()
   {
      return decision;
   }

   @Override
   public SimpleString getName()
   {
      return null;
   }

   public void await(int latchTimeout, TimeUnit unit) throws InterruptedException
   {
      latch.await(latchTimeout, unit);
   }
}
