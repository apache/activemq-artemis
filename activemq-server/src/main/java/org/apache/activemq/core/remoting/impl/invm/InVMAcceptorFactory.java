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
package org.apache.activemq6.core.remoting.impl.invm;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq6.core.server.cluster.ClusterConnection;
import org.apache.activemq6.spi.core.protocol.ProtocolManager;
import org.apache.activemq6.spi.core.remoting.Acceptor;
import org.apache.activemq6.spi.core.remoting.AcceptorFactory;
import org.apache.activemq6.spi.core.remoting.BufferHandler;
import org.apache.activemq6.spi.core.remoting.ConnectionLifeCycleListener;

/**
 * A InVMAcceptorFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMAcceptorFactory implements AcceptorFactory
{
   public Acceptor createAcceptor(final String name,
                                  final ClusterConnection clusterConnection,
                                  final Map<String, Object> configuration,
                                  final BufferHandler handler,
                                  final ConnectionLifeCycleListener listener,
                                  final Executor threadPool,
                                  final ScheduledExecutorService scheduledThreadPool,
                                  final Map<String, ProtocolManager> protocolHandler)
   {
      return new InVMAcceptor(clusterConnection, configuration, handler, listener, threadPool);
   }

   public Set<String> getAllowableProperties()
   {
      return TransportConstants.ALLOWABLE_ACCEPTOR_KEYS;
   }

}
