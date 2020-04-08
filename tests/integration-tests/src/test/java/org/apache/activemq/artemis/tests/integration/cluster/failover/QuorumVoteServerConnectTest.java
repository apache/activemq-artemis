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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.artemis.core.server.cluster.qourum.QuorumVoteServerConnect;
import org.apache.activemq.artemis.core.server.cluster.qourum.ServerConnectVote;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class QuorumVoteServerConnectTest extends ActiveMQTestBase {

   private final int size;
   private final int trueVotes;

   @Parameterized.Parameters(name = "size={0} trueVotes={1}")
   public static Collection primeNumbers() {
      return Arrays.asList(new Object[][]{{1, 0}, {2, 1}, {3, 2}, {4, 3}, {5, 3}, {6, 4}, {7, 4}, {8, 5}, {9, 5}, {10, 6}});
   }

   public QuorumVoteServerConnectTest(int size, int trueVotes) {

      this.size = size;
      this.trueVotes = trueVotes;
   }

   @Test
   public void testVoteOnRequestToStay() {
      Assume.assumeThat(trueVotes, Matchers.greaterThan(0));
      Assume.assumeThat(size, Matchers.greaterThan(trueVotes));
      final String liveConnector = "live";
      final String backupConnector = "backup";
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo", true, liveConnector);
      quorum.vote(new ServerConnectVote("foo", true, backupConnector));
      Assert.assertFalse(quorum.getDecision());
      for (int i = 0; i < trueVotes - 1; i++) {
         quorum.vote(new ServerConnectVote("foo", true, liveConnector));
         Assert.assertFalse(quorum.getDecision());
      }
      quorum.vote(new ServerConnectVote("foo", true, liveConnector));
      Assert.assertTrue(quorum.getDecision());
   }

   @Test
   public void testAllVoteCastFreezeNotRequestToStayDecision() {
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo");
      Assert.assertFalse(quorum.isRequestToStayLive());
      final boolean decisionBeforeVoteCompleted = quorum.getDecision();
      quorum.allVotesCast(null);
      for (int i = 0; i < trueVotes; i++) {
         quorum.vote(new ServerConnectVote("foo", true, null));
      }
      Assert.assertEquals(decisionBeforeVoteCompleted, quorum.getDecision());
   }

   @Test
   public void testAllVoteCastFreezeRequestToStayDecision() {
      final String liveConnector = "live";
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo", true, liveConnector);
      Assert.assertTrue(quorum.isRequestToStayLive());
      final boolean decisionBeforeVoteCompleted = quorum.getDecision();
      quorum.allVotesCast(null);
      for (int i = 0; i < trueVotes; i++) {
         quorum.vote(new ServerConnectVote("foo", true, liveConnector));
      }
      Assert.assertEquals(decisionBeforeVoteCompleted, quorum.getDecision());
   }

   @Test
   public void testAllVoteCastUnblockAwait() throws InterruptedException {
      Assume.assumeThat(trueVotes, Matchers.greaterThan(0));
      Assume.assumeThat(size, Matchers.greaterThan(trueVotes));
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo");
      Assert.assertFalse(quorum.getDecision());
      CountDownLatch taskStarted = new CountDownLatch(1);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
         final Future<InterruptedException> waitingTaskResult = executor.submit(() -> {
            taskStarted.countDown();
            try {
               quorum.await(1, TimeUnit.DAYS);
               return null;
            } catch (InterruptedException e) {
               return e;
            }
         });
         // realistic expectation of the max time to start a Thread
         Assert.assertTrue(taskStarted.await(10, TimeUnit.SECONDS));
         Assert.assertFalse(waitingTaskResult.isDone());
         quorum.allVotesCast(null);
         try {
            Assert.assertNull(waitingTaskResult.get(5, TimeUnit.SECONDS));
         } catch (TimeoutException ex) {
            Assert.fail("allVoteCast hasn't unblocked the waiting task");
         } catch (ExecutionException ex) {
            Assert.fail("This shouldn't really happen: the wait task shouldn't throw any exception: " + ex);
         }
         Assert.assertTrue(waitingTaskResult.isDone());
         Assert.assertFalse(quorum.getDecision());
      } finally {
         executor.shutdownNow();
      }
   }

   @Test
   public void testRequestToStayQuorumUnblockAwait() throws InterruptedException {
      Assume.assumeThat(trueVotes, Matchers.greaterThan(0));
      Assume.assumeThat(size, Matchers.greaterThan(trueVotes));
      final String liveConnector = "live";
      final String backupConnector = "backup";
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo", true, liveConnector);
      Assert.assertFalse(quorum.getDecision());
      CountDownLatch taskStarted = new CountDownLatch(1);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
         final Future<InterruptedException> waitingTaskResult = executor.submit(() -> {
            taskStarted.countDown();
            try {
               quorum.await(1, TimeUnit.DAYS);
               return null;
            } catch (InterruptedException e) {
               return e;
            }
         });
         // realistic expectation of the max time to start a Thread
         Assert.assertTrue(taskStarted.await(10, TimeUnit.SECONDS));
         quorum.vote(new ServerConnectVote("foo", true, backupConnector));
         Assert.assertFalse(waitingTaskResult.isDone());
         Assert.assertFalse(quorum.getDecision());
         for (int i = 0; i < trueVotes - 1; i++) {
            quorum.vote(new ServerConnectVote("foo", true, liveConnector));
            Assert.assertFalse(waitingTaskResult.isDone());
            Assert.assertFalse(quorum.getDecision());
         }
         quorum.vote(new ServerConnectVote("foo", true, liveConnector));
         Assert.assertTrue(quorum.getDecision());
         try {
            Assert.assertNull(waitingTaskResult.get(5, TimeUnit.SECONDS));
         } catch (TimeoutException ex) {
            Assert.fail("allVoteCast hasn't unblocked the waiting task");
         } catch (ExecutionException ex) {
            Assert.fail("This shouldn't really happen: the wait task shouldn't throw any exception: " + ex);
         }
         Assert.assertTrue(waitingTaskResult.isDone());
         Assert.assertTrue(quorum.getDecision());
      } finally {
         executor.shutdownNow();
      }
   }

   @Test
   public void testNotRequestToStayQuorumUnblockAwait() throws InterruptedException {
      Assume.assumeThat(trueVotes, Matchers.greaterThan(0));
      Assume.assumeThat(size, Matchers.greaterThan(trueVotes));
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo");
      Assert.assertFalse(quorum.getDecision());
      CountDownLatch taskStarted = new CountDownLatch(1);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
         final Future<InterruptedException> waitingTaskResult = executor.submit(() -> {
            taskStarted.countDown();
            try {
               quorum.await(1, TimeUnit.DAYS);
               return null;
            } catch (InterruptedException e) {
               return e;
            }
         });
         // realistic expectation of the max time to start a Thread
         Assert.assertTrue(taskStarted.await(10, TimeUnit.SECONDS));
         quorum.vote(new ServerConnectVote("foo", false, null));
         Assert.assertFalse(waitingTaskResult.isDone());
         Assert.assertFalse(quorum.getDecision());
         for (int i = 0; i < trueVotes - 1; i++) {
            quorum.vote(new ServerConnectVote("foo", true, null));
            Assert.assertFalse(waitingTaskResult.isDone());
            Assert.assertFalse(quorum.getDecision());
         }
         quorum.vote(new ServerConnectVote("foo", true, null));
         Assert.assertTrue(quorum.getDecision());
         try {
            Assert.assertNull(waitingTaskResult.get(5, TimeUnit.SECONDS));
         } catch (TimeoutException ex) {
            Assert.fail("allVoteCast hasn't unblocked the waiting task");
         } catch (ExecutionException ex) {
            Assert.fail("This shouldn't really happen: the wait task shouldn't throw any exception: " + ex);
         }
         Assert.assertTrue(waitingTaskResult.isDone());
         Assert.assertTrue(quorum.getDecision());
      } finally {
         executor.shutdownNow();
      }
   }

   @Test
   public void testSuccessfulVote() {
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes - 1; i++) {
         quorum.vote(new ServerConnectVote("foo", true, null));
      }

      if (size > 1) {
         assertFalse(quorum.getDecision());
      }
      quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes; i++) {
         quorum.vote(new ServerConnectVote("foo", true, null));
      }
      assertTrue(quorum.getDecision());
   }

   @Test
   public void testUnSuccessfulVote() {
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes - 1; i++) {
         quorum.vote(new ServerConnectVote("foo", true, null));
      }

      if (size > 1) {
         assertFalse(quorum.getDecision());
      }
      quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes - 1; i++) {
         quorum.vote(new ServerConnectVote("foo", true, null));
      }
      if (size == 1) {
         assertTrue(quorum.getDecision());
      } else {
         assertFalse(quorum.getDecision());
      }
   }
}
