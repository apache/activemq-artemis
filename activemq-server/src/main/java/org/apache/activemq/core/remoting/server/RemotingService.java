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
package org.apache.activemq.core.remoting.server;

import java.util.Set;

import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.core.security.ActiveMQPrincipal;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.spi.core.remoting.Acceptor;
import org.apache.activemq.utils.ReusableLatch;

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

   ReusableLatch getConnectionCountLatch();

   void addIncomingInterceptor(Interceptor interceptor);

   void addOutgoingInterceptor(Interceptor interceptor);

   boolean removeIncomingInterceptor(Interceptor interceptor);

   boolean removeOutgoingInterceptor(Interceptor interceptor);

   void stop(boolean criticalError) throws Exception;

   void start() throws Exception;

   void startAcceptors() throws Exception;

   boolean isStarted();

   /**
    * Allow acceptors to use this as their default security Principal if applicable.
    * <p>
    * Used by AS7 integration code.
    *
    * @param principal
    */
   void allowInvmSecurityOverride(ActiveMQPrincipal principal);

   /**
    * Pauses the acceptors so that no more connections can be made to the server
    */
   void pauseAcceptors();

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
