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
package org.apache.activemq.artemis.spi.core.security.jaas.kubernetes.model;

import static org.apache.activemq.artemis.spi.core.security.jaas.KubernetesLoginModuleTest.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.hamcrest.Matchers;
import org.junit.Test;

public class TokenReviewTest {

   @Test
   public void testEmpty() {
      String json = "{}";
      TokenReview tr = TokenReview.fromJsonString(json);

      assertFalse(tr.isAuthenticated());
      assertNull(tr.getUser());
      assertNull(tr.getUsername());
   }

   @Test
   public void testSimple() {
      String json = "{\"status\": {\"authenticated\": true, \"user\": {\"username\": \"" + USERNAME + "\"}}}";

      TokenReview tr = TokenReview.fromJsonString(json);

      assertNotNull(tr);
      assertTrue(tr.isAuthenticated());
      assertThat(tr.getUsername(), is(USERNAME));
      assertNotNull(tr.getUser());
      assertThat(tr.getUser().getUsername(), is(USERNAME));
      assertThat(tr.getAudiences(), Matchers.empty());
      assertNull(tr.getUser().getExtra());
   }

   @Test
   public void testCompleteObject() {
      String json = "{\"status\": {"
            + "\"authenticated\": true, "
            + "\"user\": {"
            + "  \"username\": \"" + USERNAME + "\","
            + "  \"uid\": \"kermit-uid\","
            + "  \"groups\": ["
            + "    \"group-1\","
            + "    \"group-2\""
            + "  ],"
            + "  \"extra\": {"
            + "    \"authentication.kubernetes.io/pod-name\": ["
            + "      \"pod-1\","
            + "      \"pod-2\""
            + "    ],"
            + "    \"authentication.kubernetes.io/pod-uid\": ["
            + "      \"pod-uid-1\","
            + "      \"pod-uid-2\""
            + "    ]"
            + "  }"
            + "},"
            + "\"audiences\": ["
            + "  \"audience-1\","
            + "  \"audience-2\""
            + "]}}";

      TokenReview tr = TokenReview.fromJsonString(json);

      assertNotNull(tr);
      assertTrue(tr.isAuthenticated());
      assertThat(tr.getUsername(), is(USERNAME));
      assertNotNull(tr.getUser());
      assertThat(tr.getUser().getUsername(), is(USERNAME));
      assertThat(tr.getAudiences(), containsInAnyOrder("audience-1", "audience-2"));
      assertThat(tr.getUser().getGroups(), containsInAnyOrder("group-1", "group-2"));
      assertThat(tr.getUser().getUid(), is("kermit-uid"));

      assertNotNull(tr.getUser().getExtra());
      assertThat(tr.getUser().getExtra().getPodNames(), containsInAnyOrder("pod-1", "pod-2"));
      assertThat(tr.getUser().getExtra().getPodUids(), containsInAnyOrder("pod-uid-1", "pod-uid-2"));

   }

}
