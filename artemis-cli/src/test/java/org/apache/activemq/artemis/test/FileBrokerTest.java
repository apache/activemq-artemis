/**
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
package org.apache.activemq.artemis.test;

import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.dto.ServerDTO;
import org.apache.activemq.artemis.integration.FileBroker;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.junit.Assert;
import org.junit.Test;

public class FileBrokerTest
{
   @Test
   public void startWithJMS() throws Exception
   {
      ServerDTO serverDTO = new ServerDTO();
      serverDTO.configuration = "broker.xml";
      FileBroker broker = null;
      try
      {
         broker = new FileBroker(serverDTO, new ActiveMQSecurityManagerImpl());
         broker.start();
         JMSServerManagerImpl jmsServerManager = (JMSServerManagerImpl) broker.getComponents().get("jms");
         Assert.assertNotNull(jmsServerManager);
         Assert.assertTrue(jmsServerManager.isStarted());
         //this tells us the jms server is activated
         Assert.assertTrue(jmsServerManager.getJMSStorageManager().isStarted());
         ActiveMQServerImpl activeMQServer = (ActiveMQServerImpl) broker.getComponents().get("core");
         Assert.assertNotNull(activeMQServer);
         Assert.assertTrue(activeMQServer.isStarted());
         Assert.assertTrue(broker.isStarted());
      }
      finally
      {
         if (broker != null)
         {
            broker.stop();
         }
      }
   }

   @Test
   public void startWithoutJMS() throws Exception
   {
      ServerDTO serverDTO = new ServerDTO();
      serverDTO.configuration = "broker-nojms.xml";
      FileBroker broker = null;
      try
      {
         broker = new FileBroker(serverDTO, new ActiveMQSecurityManagerImpl());
         broker.start();
         JMSServerManagerImpl jmsServerManager = (JMSServerManagerImpl) broker.getComponents().get("jms");
         Assert.assertNull(jmsServerManager);
         ActiveMQServerImpl activeMQServer = (ActiveMQServerImpl) broker.getComponents().get("core");
         Assert.assertNotNull(activeMQServer);
         Assert.assertTrue(activeMQServer.isStarted());
         Assert.assertTrue(broker.isStarted());
      }
      finally
      {
         assert broker != null;
         broker.stop();
      }
   }
}
