/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.cluster.bridge;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.tests.util.InVMNodeManagerServer;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;

/**
 * A BridgeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *         <p/>
 *         Created 21 Nov 2008 10:32:23
 */
public abstract class BridgeTestBase extends UnitTestCase
{

   @Override
   @After
   public void tearDown() throws Exception
   {
      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
   }

   protected HornetQServer createHornetQServer(final int id, final boolean netty, final Map<String, Object> params) throws Exception
   {
      return createHornetQServer(id, params, netty, null);
   }

   protected HornetQServer createHornetQServer(final int id, final boolean netty, final Map<String, Object> params, NodeManager nodeManager) throws Exception
   {
      return createHornetQServer(id, params, netty, nodeManager);
   }

   protected HornetQServer createHornetQServer(final int id,
                                               final Map<String, Object> params,
                                               final boolean netty,
                                               final NodeManager nodeManager) throws Exception
   {
      Configuration serviceConf = createBasicConfig();
      serviceConf.setSecurityEnabled(false);
      serviceConf.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      serviceConf.setJournalType(getDefaultJournalType());
      serviceConf.setBindingsDirectory(getBindingsDir(id, false));
      serviceConf.setJournalMinFiles(2);
      serviceConf.setJournalDirectory(getJournalDir(id, false));
      serviceConf.setPagingDirectory(getPageDir(id, false));
      serviceConf.setLargeMessagesDirectory(getLargeMessagesDir(id, false));
      // these tests don't need any big storage so limiting the size of the journal files to speed up the test
      serviceConf.setJournalFileSize(100 * 1024);

      if (netty)
      {
         params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + id);
         serviceConf.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      }
      else
      {
         params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, id);
         serviceConf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params));
      }
      HornetQServer service;
      if (nodeManager == null)
      {
         service = HornetQServers.newHornetQServer(serviceConf, true);
      }
      else
      {
         service = new InVMNodeManagerServer(serviceConf, nodeManager);
      }
      return addServer(service);
   }

   protected HornetQServer createBackupHornetQServer(final int id,
                                                     final Map<String, Object> params,
                                                     final boolean netty,
                                                     final int liveId,
                                                     final NodeManager nodeManager) throws Exception
   {
      Configuration serviceConf = createBasicConfig();
      serviceConf.setSecurityEnabled(false);
      serviceConf.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      serviceConf.setJournalType(getDefaultJournalType());
      serviceConf.setBindingsDirectory(getBindingsDir(liveId, false));
      serviceConf.setJournalMinFiles(2);
      serviceConf.setJournalDirectory(getJournalDir(liveId, false));
      serviceConf.setPagingDirectory(getPageDir(liveId, false));
      serviceConf.setLargeMessagesDirectory(getLargeMessagesDir(liveId, false));
      // these tests don't need any big storage so limiting the size of the journal files to speed up the test
      serviceConf.setJournalFileSize(100 * 1024);

      if (netty)
      {
         params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + id);
         serviceConf.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      }
      else
      {
         params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, id);
         serviceConf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params));
      }
      HornetQServer service;
      if (nodeManager == null)
      {
         service = HornetQServers.newHornetQServer(serviceConf, true);
      }
      else
      {
         service = new InVMNodeManagerServer(serviceConf, nodeManager);
      }
      return addServer(service);
   }


   protected void waitForServerStart(HornetQServer server) throws Exception
   {
      if (!server.waitForActivation(5000L, TimeUnit.MILLISECONDS))
         throw new IllegalStateException("Timed out waiting for server starting = " + server);
   }
}
