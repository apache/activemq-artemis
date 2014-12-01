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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;

/**
 * We just copy this structure from amq, but what's the purpose
 * and can it be removed ?
 *
 * @author howard
 *
 */
public class AMQSingleTransportConnectionStateRegister implements
      AMQTransportConnectionStateRegister
{

   private AMQTransportConnectionState connectionState;
   private ConnectionId connectionId;

   public AMQTransportConnectionState registerConnectionState(
         ConnectionId connectionId, AMQTransportConnectionState state)
   {
      AMQTransportConnectionState rc = connectionState;
      connectionState = state;
      this.connectionId = connectionId;
      return rc;
   }

   public synchronized AMQTransportConnectionState unregisterConnectionState(
         ConnectionId connectionId)
   {
      AMQTransportConnectionState rc = null;

      if (connectionId != null && connectionState != null
            && this.connectionId != null)
      {
         if (this.connectionId.equals(connectionId))
         {
            rc = connectionState;
            connectionState = null;
            connectionId = null;
         }
      }
      return rc;
   }

   public synchronized List<AMQTransportConnectionState> listConnectionStates()
   {
      List<AMQTransportConnectionState> rc = new ArrayList<AMQTransportConnectionState>();
      if (connectionState != null)
      {
         rc.add(connectionState);
      }
      return rc;
   }

   public synchronized AMQTransportConnectionState lookupConnectionState(
         String connectionId)
   {
      AMQTransportConnectionState cs = connectionState;
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a connectionId for a connection that had not been registered: "
                     + connectionId);
      }
      return cs;
   }

   public synchronized AMQTransportConnectionState lookupConnectionState(
         ConsumerId id)
   {
      AMQTransportConnectionState cs = connectionState;
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a consumer from a connection that had not been registered: "
                     + id.getParentId().getParentId());
      }
      return cs;
   }

   public synchronized AMQTransportConnectionState lookupConnectionState(
         ProducerId id)
   {
      AMQTransportConnectionState cs = connectionState;
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a producer from a connection that had not been registered: "
                     + id.getParentId().getParentId());
      }
      return cs;
   }

   public synchronized AMQTransportConnectionState lookupConnectionState(
         SessionId id)
   {
      AMQTransportConnectionState cs = connectionState;
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot lookup a session from a connection that had not been registered: "
                     + id.getParentId());
      }
      return cs;
   }

   public synchronized AMQTransportConnectionState lookupConnectionState(
         ConnectionId connectionId)
   {
      AMQTransportConnectionState cs = connectionState;
      return cs;
   }

   public synchronized boolean doesHandleMultipleConnectionStates()
   {
      return false;
   }

   public synchronized boolean isEmpty()
   {
      return connectionState == null;
   }

   public void intialize(AMQTransportConnectionStateRegister other)
   {

      if (other.isEmpty())
      {
         clear();
      }
      else
      {
         Map map = other.mapStates();
         Iterator i = map.entrySet().iterator();
         Map.Entry<ConnectionId, AMQTransportConnectionState> entry = (Entry<ConnectionId, AMQTransportConnectionState>) i
               .next();
         connectionId = entry.getKey();
         connectionState = entry.getValue();
      }

   }

   public Map<ConnectionId, AMQTransportConnectionState> mapStates()
   {
      Map<ConnectionId, AMQTransportConnectionState> map = new HashMap<ConnectionId, AMQTransportConnectionState>();
      if (!isEmpty())
      {
         map.put(connectionId, connectionState);
      }
      return map;
   }

   public void clear()
   {
      connectionState = null;
      connectionId = null;

   }

}
