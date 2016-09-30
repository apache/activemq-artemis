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
package org.apache.activemq.artemis.core.server.cluster.qourum;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.Topology;
import org.apache.activemq.artemis.core.persistence.StorageManager;

/**
 * A Qourum Vote for deciding if a replicated backup should become live.
 */
public class QuorumVoteServerConnect extends QuorumVote<BooleanVote, Boolean> {

   private static final SimpleString LIVE_FAILOVER_VOTE = new SimpleString("LIVE_FAILOVER)VOTE");
   private final CountDownLatch latch;

   private int votesNeeded;

   private int total = 0;

   private boolean decision = false;

   /**
    * live nodes | remaining nodes |  majority   | votes needed
    * 1      |       0         |     0       |      0
    * 2      |       1         |     1       |      1
    * n      |    r = n-1      |   n/2 + 1   |   n/2 + 1 rounded
    * 3      |       2         |     2.5     |      2
    * 4      |       3         |      3      |      3
    * 5      |       4         |     3.5     |      3
    * 6      |       5         |      4      |      4
    */
   public QuorumVoteServerConnect(int size, StorageManager storageManager) {
      super(LIVE_FAILOVER_VOTE);
      double majority;
      if (size <= 2) {
         majority = ((double) size) / 2;
      } else {
         //even
         majority = ((double) size) / 2 + 1;
      }
      //votes needed could be say 2.5 so we add 1 in this case
      votesNeeded = (int) majority;
      latch = new CountDownLatch(votesNeeded);
      if (votesNeeded == 0) {
         decision = true;
      }
   }

   /**
    * if we can connect to a node
    *
    * @return
    */
   @Override
   public Vote connected() {
      return new BooleanVote(true);
   }

   /**
    * if we cant connect to the node
    *
    * @return
    */
   @Override
   public Vote notConnected() {
      return new BooleanVote(false);
   }

   /**
    * live nodes | remaining nodes |  majority   | votes needed
    * 1      |       0         |     0       |      0
    * 2      |       1         |     1       |      1
    * n      |    r = n-1      |   n/2 + 1   |   n/2 + 1 rounded
    * 3      |       2         |     2.5     |      2
    * 4      |       3         |      3      |      3
    * 5      |       4         |     3.5     |      3
    * 6      |       5         |      4      |      4
    *
    * @param vote the vote to make.
    */
   @Override
   public synchronized void vote(BooleanVote vote) {
      if (decision)
         return;
      if (vote.getVote()) {
         total++;
         latch.countDown();
         if (total >= votesNeeded) {
            decision = true;
         }
      }
   }

   @Override
   public void allVotesCast(Topology voteTopology) {

   }

   @Override
   public Boolean getDecision() {
      return decision;
   }

   @Override
   public SimpleString getName() {
      return null;
   }

   public void await(int latchTimeout, TimeUnit unit) throws InterruptedException {
      latch.await(latchTimeout, unit);
   }
}
