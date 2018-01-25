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
package org.apache.activemq.artemis.rest.integration;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.MessageServiceManager;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.jboss.resteasy.test.TestPortProvider;

public class EmbeddedRestActiveMQ {

   private TJWSEmbeddedJaxrsServer tjws = new TJWSEmbeddedJaxrsServer();
   private EmbeddedActiveMQ embeddedActiveMQ;
   private MessageServiceManager manager = new MessageServiceManager(null);

   public EmbeddedRestActiveMQ(ConnectionFactoryOptions jmsOptions) {
      int port = TestPortProvider.getPort();
      tjws.setPort(port);
      tjws.setRootResourcePath("");
      tjws.setSecurityDomain(null);
      manager = new MessageServiceManager(jmsOptions);
      initEmbeddedActiveMQ();
   }

   protected void initEmbeddedActiveMQ() {
      embeddedActiveMQ = new EmbeddedActiveMQ();
   }

   public MessageServiceManager getManager() {
      return manager;
   }

   public void start() throws Exception {
      embeddedActiveMQ.start();
      tjws.start();
      manager.start();
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getTopicManager().getDestination());
   }

   public void stop() throws Exception {
      try {
         tjws.stop();
      } catch (Exception e) {
      }
      try {
         manager.stop();
      } catch (Exception e) {
      }
      embeddedActiveMQ.stop();
   }

   public EmbeddedActiveMQ getEmbeddedActiveMQ() {
      return embeddedActiveMQ;
   }

   public void setEmbeddedActiveMQ(EmbeddedActiveMQ embeddedActiveMQ) {
      this.embeddedActiveMQ = embeddedActiveMQ;
   }
}
