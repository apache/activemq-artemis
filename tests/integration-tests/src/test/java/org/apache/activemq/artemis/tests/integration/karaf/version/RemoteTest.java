/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.karaf.version;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.karaf.ContainerBaseTest;
import org.junit.Test;

public class RemoteTest extends ContainerBaseTest {

   @Test
   public void testValidateRemote() throws Exception {
      setupContainer(ProbeRemoteServer.class, "probe1", configureArtemisFeatures(false, "1.5.1", "artemis-core"));

      executeRemoteTest();

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
      factory.createConnection().close();

      container.stop();
   }

}
