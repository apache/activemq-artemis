/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.hornetq.client;

import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQClientProtocolManager;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.apache.activemq.artemis.core.version.Version;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.SessionContext;

public class HornetQClientProtocolManager extends ActiveMQClientProtocolManager {

   private static final int VERSION_PLAYED = 123;

   @Override
   protected void sendHandshake(Connection transportConnection) {
   }

   @Override
   protected SessionContext newSessionContext(String name,
                                              int confirmationWindowSize,
                                              Channel sessionChannel,
                                              CreateSessionResponseMessage response) {
      // these objects won't be null, otherwise it would keep retrying on the previous loop
      return new HornetQClientSessionContext(name, connection, sessionChannel, response.getServerVersion(), confirmationWindowSize);
   }

   @Override
   protected Packet newCreateSessionPacket(Version clientVersion,
                                           String name,
                                           String username,
                                           String password,
                                           boolean xa,
                                           boolean autoCommitSends,
                                           boolean autoCommitAcks,
                                           boolean preAcknowledge,
                                           int minLargeMessageSize,
                                           int confirmationWindowSize,
                                           long sessionChannelID) {
      return new CreateSessionMessage(name, sessionChannelID, VERSION_PLAYED, username, password, minLargeMessageSize, xa, autoCommitSends, autoCommitAcks, preAcknowledge, confirmationWindowSize, null);
   }

   @Override
   public void sendSubscribeTopology(final boolean isServer) {
      getChannel0().send(new SubscribeClusterTopologyUpdatesMessageV2(isServer, VERSION_PLAYED));
   }

}
