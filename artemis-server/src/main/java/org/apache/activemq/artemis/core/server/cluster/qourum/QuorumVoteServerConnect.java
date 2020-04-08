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
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

/**
 * A Quorum Vote for deciding if a replicated backup should become live.
 */
public class QuorumVoteServerConnect extends QuorumVote<ServerConnectVote, Boolean> {

   public static final SimpleString LIVE_FAILOVER_VOTE = new SimpleString("LiveFailoverQuorumVote");
   // this flag mark the end of the vote
   private final CountDownLatch voteCompleted;
   private final String targetNodeId;
   private final String liveConnector;
   private int votesNeeded;

   // Is this the live requesting to stay live, or a backup requesting to become live.
   private final boolean requestToStayLive;

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
   public QuorumVoteServerConnect(int size, String targetNodeId, boolean requestToStayLive, String liveConnector) {
      super(LIVE_FAILOVER_VOTE);
      this.targetNodeId = targetNodeId;
      this.liveConnector = liveConnector;
      double majority;
      if (size <= 2) {
         majority = ((double) size) / 2;
      } else {
         //even
         majority = ((double) size) / 2 + 1;
      }
      //votes needed could be say 2.5 so we add 1 in this case
      votesNeeded = (int) majority;
      voteCompleted = new CountDownLatch(1);
      if (votesNeeded == 0) {
         voteCompleted.countDown();
      }
      this.requestToStayLive = requestToStayLive;
   }

   public QuorumVoteServerConnect(int size, String targetNodeId) {
      this(size, targetNodeId, false, null);
   }
   /**
    * if we can connect to a node
    *
    * @return
    */
   @Override
   public Vote connected() {
      return new ServerConnectVote(targetNodeId, requestToStayLive, null);
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
   public synchronized void vote(ServerConnectVote vote) {
      if (voteCompleted.getCount() == 0) {
         ActiveMQServerLogger.LOGGER.ignoredQuorumVote(vote);
         return;
      }
      if (vote.getVote()) {
         if (!requestToStayLive) {
            acceptPositiveVote();
         } else if (liveConnector.equals(vote.getTransportConfiguration())) {
            acceptPositiveVote();
         } else {
            ActiveMQServerLogger.LOGGER.quorumBackupIsLive(vote.getTransportConfiguration());
         }
      }
   }

   private synchronized void acceptPositiveVote() {
      if (voteCompleted.getCount() == 0) {
         throw new IllegalStateException("Cannot accept any new positive vote if the vote is completed or the decision is already taken");
      }
      votesNeeded--;
      if (votesNeeded == 0) {
         voteCompleted.countDown();
      }
   }

   @Override
   public synchronized void allVotesCast(Topology voteTopology) {
      if (voteCompleted.getCount() > 0) {
         voteCompleted.countDown();
      }
   }

   @Override
   public synchronized Boolean getDecision() {
      return votesNeeded == 0;
   }

   public void await(int latchTimeout, TimeUnit unit) throws InterruptedException {
      ActiveMQServerLogger.LOGGER.waitingForQuorumVoteResults(latchTimeout, unit.toString().toLowerCase());
      if (voteCompleted.await(latchTimeout, unit))
         ActiveMQServerLogger.LOGGER.receivedAllQuorumVotes();
      else
         ActiveMQServerLogger.LOGGER.timeoutWaitingForQuorumVoteResponses();
   }

   public boolean isRequestToStayLive() {
      return requestToStayLive;
   }
}
