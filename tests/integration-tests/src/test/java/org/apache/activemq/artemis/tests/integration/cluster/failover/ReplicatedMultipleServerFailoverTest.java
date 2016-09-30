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

import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.junit.Test;

public class ReplicatedMultipleServerFailoverTest extends MultipleServerFailoverTestBase {

   @Test
   public void testStartLiveFirst() throws Exception {
      for (TestableServer liveServer : liveServers) {
         liveServer.start();
      }
      for (TestableServer backupServer : backupServers) {
         backupServer.start();
      }
      waitForTopology(liveServers.get(0).getServer(), liveServers.size(), backupServers.size());
      sendCrashReceive();
   }

   @Test
   public void testStartBackupFirst() throws Exception {
      for (TestableServer backupServer : backupServers) {
         backupServer.start();
      }
      for (TestableServer liveServer : liveServers) {
         liveServer.start();
      }
      waitForTopology(liveServers.get(0).getServer(), liveServers.size(), liveServers.size());
      sendCrashReceive();
   }

   protected void sendCrashReceive() throws Exception {
      ServerLocator[] locators = new ServerLocator[liveServers.size()];
      try {
         for (int i = 0; i < locators.length; i++) {
            locators[i] = getServerLocator(i);
         }

         ClientSessionFactory[] factories = new ClientSessionFactory[liveServers.size()];
         for (int i = 0; i < factories.length; i++) {
            factories[i] = createSessionFactory(locators[i]);
         }

         ClientSession[] sessions = new ClientSession[liveServers.size()];
         for (int i = 0; i < factories.length; i++) {
            sessions[i] = createSession(factories[i], true, true);
            sessions[i].createQueue(ADDRESS, ADDRESS, null, true);
         }

         //make sure bindings are ready before sending messages
         for (int i = 0; i < liveServers.size(); i++) {
            this.waitForBindings(liveServers.get(i).getServer(), ADDRESS.toString(), true, 1, 0, 2000);
            this.waitForBindings(liveServers.get(i).getServer(), ADDRESS.toString(), false, 1, 0, 2000);
         }

         ClientProducer producer = sessions[0].createProducer(ADDRESS);

         for (int i = 0; i < liveServers.size() * 100; i++) {
            ClientMessage message = sessions[0].createMessage(true);

            setBody(i, message);

            message.putIntProperty("counter", i);

            producer.send(message);
         }

         producer.close();

         for (TestableServer liveServer : liveServers) {
            waitForDistribution(ADDRESS, liveServer.getServer(), 100);
         }

         for (TestableServer liveServer : liveServers) {
            liveServer.crash();
         }
         ClientConsumer[] consumers = new ClientConsumer[liveServers.size()];
         for (int i = 0; i < factories.length; i++) {
            consumers[i] = sessions[i].createConsumer(ADDRESS);
            sessions[i].start();
         }

         for (int i = 0; i < 100; i++) {
            for (ClientConsumer consumer : consumers) {
               ClientMessage message = consumer.receive(1000);
               assertNotNull("expecting durable msg " + i, message);
               message.acknowledge();
            }

         }
      } finally {
         for (ServerLocator locator : locators) {
            if (locator != null) {
               try {
                  locator.close();
               } catch (Exception e) {
                  //ignore
               }
            }
         }
      }
   }

   @Override
   public int getLiveServerCount() {
      return 2;
   }

   @Override
   public int getBackupServerCount() {
      return 2;
   }

   @Override
   public boolean isNetty() {
      return false;
   }

   @Override
   public boolean isSharedStore() {
      return false;
   }

   @Override
   public String getNodeGroupName() {
      return "nodeGroup";
   }
}
