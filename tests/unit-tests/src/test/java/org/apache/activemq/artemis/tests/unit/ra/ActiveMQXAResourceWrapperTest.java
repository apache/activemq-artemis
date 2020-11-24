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

package org.apache.activemq.artemis.tests.unit.ra;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.service.extensions.xa.recovery.ActiveMQXAResourceWrapper;
import org.apache.activemq.artemis.service.extensions.xa.recovery.XARecoveryConfig;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

import javax.transaction.xa.XAResource;

public class ActiveMQXAResourceWrapperTest extends ActiveMQTestBase {

   @Test
   public void testMe() throws Exception {
      ActiveMQServer server = createServer(false, true);
      ActiveMQResourceAdapter ra = null;
      ClientSession cs = null;

      try {
         server.getConfiguration().setSecurityEnabled(false);
         server.start();

         ra = new ActiveMQResourceAdapter();

         ra.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
         ra.setConnectionTTL(4000L);
         ra.start(new BootstrapContext());


         TestActiveMQXAResourceWrapper wrapper = new TestActiveMQXAResourceWrapper(ra.getRecoveryManager().getResources().toArray(new XARecoveryConfig[]{}));

         XAResource res = wrapper.connect();
         if (!(res instanceof ClientSession)) {
            fail("Unexpected XAResource type");
         }
         cs = (ClientSession) res;
         assertEquals(4000L, cs.getSessionFactory().getServerLocator().getConnectionTTL());

      } finally {
         if (cs != null)
            cs.close();
         if (ra != null)
            ra.stop();
         server.stop();
      }
   }

   class TestActiveMQXAResourceWrapper extends ActiveMQXAResourceWrapper {

      TestActiveMQXAResourceWrapper(XARecoveryConfig[] configs) {
         super(configs);
      }

      @Override
      protected XAResource connect() throws Exception {
         return super.connect();
      }
   }
}
