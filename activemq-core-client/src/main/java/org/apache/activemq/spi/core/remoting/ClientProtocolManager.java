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

package org.apache.activemq.spi.core.remoting;

import java.util.List;
import java.util.concurrent.locks.Lock;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.spi.core.protocol.RemotingConnection;

/**
 * @author Clebert Suconic
 */

public interface ClientProtocolManager
{

   /// Life Cycle Methods:

   RemotingConnection connect(Connection transportConnection, long callTimeout, long callFailoverTimeout, List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors, TopologyResponseHandler topologyResponseHandler);

   RemotingConnection getCurrentConnection();

   Lock lockSessionCreation();

   boolean waitOnLatch(long milliseconds) throws InterruptedException;

   /**
    * This is to be called when a connection failed and we want to interrupt any communication.
    * This used to be called exitLoop at some point o the code.. with a method named causeExit from ClientSessionFactoryImpl
    */
   void stop();

   boolean isAlive();

   /// Sending methods

   void addChannelHandlers(ChannelPipeline pipeline);

   void sendSubscribeTopology(boolean isServer);

   void ping(long connectionTTL);

   SessionContext createSessionContext(final String name,
                                       final String username,
                                       final String password,
                                       final boolean xa,
                                       final boolean autoCommitSends,
                                       final boolean autoCommitAcks,
                                       final boolean preAcknowledge,
                                       int minLargeMessageSize,
                                       int confirmationWindowSize) throws HornetQException;

   boolean cleanupBeforeFailover(HornetQException cause);

   boolean checkForFailover(String liveNodeID) throws HornetQException;

   void setSessionFactory(ClientSessionFactory factory);

   ClientSessionFactory getSessionFactory();

   String getName();

}
