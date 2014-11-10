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
package org.hornetq.tests.integration.cluster.reattach;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;

/**
 * A NettyMultiThreadRandomReattachTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 18 Feb 2009 08:01:20
 *
 *
 */
public class NettyMultiThreadRandomReattachTest extends MultiThreadRandomReattachTest
{
   @Override
   protected void start() throws Exception
   {
      Configuration liveConf = createDefaultConfig()
         .setJMXManagementEnabled(false)
         .setSecurityEnabled(false)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));
      liveServer = createServer(false, liveConf);
      liveServer.start();
      waitForServer(liveServer);
   }

   @Override
   protected ServerLocator createLocator() throws Exception
   {
      ServerLocator locator = createNettyNonHALocator();
      locator.setReconnectAttempts(-1);
      locator.setConfirmationWindowSize(1024 * 1024);
      locator.setAckBatchSize(0);
      return  locator;
   }

}
