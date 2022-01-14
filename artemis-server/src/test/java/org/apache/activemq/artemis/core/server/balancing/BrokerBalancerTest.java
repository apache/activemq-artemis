/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.targets.LocalTarget;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetResult;
import org.apache.activemq.artemis.core.server.balancing.transformer.KeyTransformer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BrokerBalancerTest {

   Target localTarget;
   BrokerBalancer underTest;

   @Before
   public void setUp() {
      ActiveMQServer mockServer = mock(ActiveMQServer.class);
      Mockito.when(mockServer.getNodeID()).thenReturn(SimpleString.toSimpleString("UUID"));
      localTarget = new LocalTarget(null, mockServer);
   }

   @Test
   public void getTarget() {
      Pool pool = null;
      Policy policy = null;
      underTest  = new BrokerBalancer("test", TargetKey.CLIENT_ID, "^.{3}",
                                      localTarget, "^FOO.*", null, pool, policy, null);
      assertEquals( localTarget, underTest.getTarget("FOO_EE").getTarget());
      assertEquals(TargetResult.REFUSED_USE_ANOTHER_RESULT, underTest.getTarget("BAR_EE"));
   }

   @Test
   public void getLocalTargetWithTransformer() throws Exception {
      Pool pool = null;
      Policy policy = null;
      KeyTransformer keyTransformer = new KeyTransformer() {
         @Override
         public String transform(String key) {
            return key.substring("TRANSFORM_TO".length() + 1);
         }
      };
      underTest  = new BrokerBalancer("test", TargetKey.CLIENT_ID, "^.{3}",
                                      localTarget, "^FOO.*", null, pool, policy, keyTransformer);
      assertEquals( localTarget, underTest.getTarget("TRANSFORM_TO_FOO_EE").getTarget());
   }

}