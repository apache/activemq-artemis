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

import org.apache.activemq.artemis.core.server.cluster.qourum.QuorumVoteServerConnect;
import org.apache.activemq.artemis.core.server.cluster.qourum.ServerConnectVote;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
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
   public void testSuccessfulVote() {
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes - 1; i++) {
         quorum.vote(new ServerConnectVote("foo", true));
      }

      if (size > 1) {
         assertFalse(quorum.getDecision());
      }
      quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes; i++) {
         quorum.vote(new ServerConnectVote("foo", true));
      }
      assertTrue(quorum.getDecision());
   }

   @Test
   public void testUnSuccessfulVote() {
      QuorumVoteServerConnect quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes - 1; i++) {
         quorum.vote(new ServerConnectVote("foo", true));
      }

      if (size > 1) {
         assertFalse(quorum.getDecision());
      }
      quorum = new QuorumVoteServerConnect(size, "foo");
      for (int i = 0; i < trueVotes - 1; i++) {
         quorum.vote(new ServerConnectVote("foo", true));
      }
      if (size == 1) {
         assertTrue(quorum.getDecision());
      } else {
         assertFalse(quorum.getDecision());
      }
   }
}
