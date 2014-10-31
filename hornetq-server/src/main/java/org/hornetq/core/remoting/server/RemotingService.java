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
package org.hornetq.core.remoting.server;

import java.util.Set;

import org.hornetq.api.core.Interceptor;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.security.HornetQPrincipal;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface RemotingService
{
   /**
    * Remove a connection from the connections held by the remoting service.
    * <strong>This method must be used only from the management API.
    * RemotingConnections are removed from the remoting service when their connectionTTL is hit.</strong>
    *
    * @param remotingConnectionID the ID of the RemotingConnection to removed
    * @return the removed RemotingConnection
    */
   RemotingConnection removeConnection(Object remotingConnectionID);

   Set<RemotingConnection> getConnections();

   void addIncomingInterceptor(Interceptor interceptor);

   void addOutgoingInterceptor(Interceptor interceptor);

   boolean removeIncomingInterceptor(Interceptor interceptor);

   boolean removeOutgoingInterceptor(Interceptor interceptor);

   void stop(boolean criticalError) throws Exception;

   void start() throws Exception;

   boolean isStarted();

   /**
    * Allow acceptors to use this as their default security Principal if applicable.
    * <p>
    * Used by AS7 integration code.
    *
    * @param principal
    */
   void allowInvmSecurityOverride(HornetQPrincipal principal);

   /**
    * Freezes and then disconnects all connections except the given one and tells the client where else
    * it might connect (only applicable if server is in a cluster and uses scaleDown-on-failover=true).
    *
    * @param scaleDownNodeID
    * @param remotingConnection
    */
   void freeze(String scaleDownNodeID, CoreRemotingConnection remotingConnection);

   /**
    * Returns the acceptor identified by its {@code name} or {@code null} if it does not exists.
    *
    * @param name the name of the acceptor
    */
   Acceptor getAcceptor(String name);

}
