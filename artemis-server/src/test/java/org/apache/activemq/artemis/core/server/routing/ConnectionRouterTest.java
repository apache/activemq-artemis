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

package org.apache.activemq.artemis.core.server.routing;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.routing.policies.AbstractPolicy;
import org.apache.activemq.artemis.core.server.routing.policies.Policy;
import org.apache.activemq.artemis.core.server.routing.targets.LocalTarget;
import org.apache.activemq.artemis.core.server.routing.targets.Target;
import org.apache.activemq.artemis.core.server.routing.targets.TargetResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionRouterTest {

   Target localTarget;
   ConnectionRouter underTest;

   @Before
   public void setUp() {
      ActiveMQServer mockServer = mock(ActiveMQServer.class);
      Mockito.when(mockServer.getNodeID()).thenReturn(SimpleString.toSimpleString("UUID"));
      localTarget = new LocalTarget(null, mockServer);
   }

   @Test
   public void getTarget() {
      Policy policy = null;
      underTest  = new ConnectionRouter("test", KeyType.CLIENT_ID, "^.{3}",
                                      localTarget, "^FOO.*", null, null, policy);
      assertEquals( localTarget, underTest.getTarget("FOO_EE").getTarget());
      assertEquals(TargetResult.REFUSED_USE_ANOTHER_RESULT, underTest.getTarget("BAR_EE"));
   }

   @Test
   public void getLocalTargetWithTransformer() throws Exception {
      Policy policy = new AbstractPolicy("TEST") {
         @Override
         public String transformKey(String key) {
            return key.substring("TRANSFORM_TO".length() + 1);
         }
      };

      underTest  = new ConnectionRouter("test", KeyType.CLIENT_ID, "^.{3}",
                                      localTarget, "^FOO.*", null, null, policy);
      assertEquals( localTarget, underTest.getTarget("TRANSFORM_TO_FOO_EE").getTarget());
   }

}