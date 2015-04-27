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
package org.apache.activemq.core.protocol.openwire.amq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;

public class AMQMapTransportConnectionStateRegister implements
      AMQTransportConnectionStateRegister
{

   private Map<ConnectionId, AMQTransportConnectionState> connectionStates = new ConcurrentHashMap<ConnectionId, AMQTransportConnectionState>();

   public AMQTransportConnectionState registerConnectionState(
         ConnectionId connectionId, AMQTransportConnectionState state)
   {
      AMQTransportConnectionState rc = connectionStates
            .put(connectionId, state);
      return rc;
   }

   public AMQTransportConnectionState unregisterConnectionState(
         ConnectionId connectionId)
   {
      AMQTransportConnectionState rc = connectionStates.remove(connectionId);
      if (rc.getReferenceCounter().get() > 1)
      {
         rc.decrementReference();
         connectionStates.put(connectionId, rc);
      }
      return rc;
   }

   public List<AMQTransportConnectionState> listConnectionStates()
   {

      List<AMQTransportConnectionState> rc = new ArrayList<AMQTransportConnectionState>();
      rc.addAll(connectionStates.values());
      return rc;
   }

   public AMQTransportConnectionState lookupConnectionState(String connectionId)
   {
      return connectionStates.get(new ConnectionId(connectionId));
   }

   public AMQTransportConnectionState lookupConnectionState(ConsumerId id)
   {
      AMQTransportConnectionState cs = lookupConnectionState(id
            .getConnectionId());
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a consumer from a connection that had not been registered: "
                     + id.getParentId().getParentId());
      }
      return cs;
   }

   public AMQTransportConnectionState lookupConnectionState(ProducerId id)
   {
      AMQTransportConnectionState cs = lookupConnectionState(id
            .getConnectionId());
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a producer from a connection that had not been registered: "
                     + id.getParentId().getParentId());
      }
      return cs;
   }

   public AMQTransportConnectionState lookupConnectionState(SessionId id)
   {
      AMQTransportConnectionState cs = lookupConnectionState(id
            .getConnectionId());
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a session from a connection that had not been registered: "
                     + id.getParentId());
      }
      return cs;
   }

   public AMQTransportConnectionState lookupConnectionState(
         ConnectionId connectionId)
   {
      AMQTransportConnectionState cs = connectionStates.get(connectionId);
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a connection that had not been registered: "
                     + connectionId);
      }
      return cs;
   }

   public boolean doesHandleMultipleConnectionStates()
   {
      return true;
   }

   public boolean isEmpty()
   {
      return connectionStates.isEmpty();
   }

   public void clear()
   {
      connectionStates.clear();

   }

   public void intialize(AMQTransportConnectionStateRegister other)
   {
      connectionStates.clear();
      connectionStates.putAll(other.mapStates());

   }

   public Map<ConnectionId, AMQTransportConnectionState> mapStates()
   {
      HashMap<ConnectionId, AMQTransportConnectionState> map = new HashMap<ConnectionId, AMQTransportConnectionState>(
            connectionStates);
      return map;
   }

}
