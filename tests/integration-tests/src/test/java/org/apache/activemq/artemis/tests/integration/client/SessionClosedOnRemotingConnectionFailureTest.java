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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SessionClosedOnRemotingConnectionFailureTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private ClientSessionFactory sf;

   @Test
   public void testSessionClosedOnRemotingConnectionFailure() throws Exception {
      ClientSession session = addClientSession(sf.createSession());

      session.createQueue("fooaddress", "fooqueue");

      ClientProducer prod = session.createProducer("fooaddress");

      ClientConsumer cons = session.createConsumer("fooqueue");

      session.start();

      prod.send(session.createMessage(false));

      Assert.assertNotNull(cons.receive());

      // Now fail the underlying connection

      RemotingConnection connection = ((ClientSessionInternal) session).getConnection();

      connection.fail(new ActiveMQNotConnectedException());

      Assert.assertTrue(session.isClosed());

      Assert.assertTrue(prod.isClosed());

      Assert.assertTrue(cons.isClosed());

      // Now try and use the producer

      try {
         prod.send(session.createMessage(false));

         Assert.fail("Should throw exception");
      } catch (ActiveMQObjectClosedException oce) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      try {
         cons.receive();

         Assert.fail("Should throw exception");
      } catch (ActiveMQObjectClosedException oce) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      session.close();
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Configuration config = createDefaultInVMConfig();
      server = createServer(false, config);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
   }
}
