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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.core.protocol.openwire.OpenWireConnection;

/**
 * @See org.apache.activemq.broker.TransportConnectionState
 * @author howard
 *
 */
public class AMQTransportConnectionState extends ConnectionState
{

   private AMQConnectionContext context;
   private OpenWireConnection connection;
   private AtomicInteger referenceCounter = new AtomicInteger();
   private final Object connectionMutex = new Object();

   public AMQTransportConnectionState(ConnectionInfo info,
         OpenWireConnection transportConnection)
   {
      super(info);
      connection = transportConnection;
   }

   public AMQConnectionContext getContext()
   {
      return context;
   }

   public OpenWireConnection getConnection()
   {
      return connection;
   }

   public void setContext(AMQConnectionContext context)
   {
      this.context = context;
   }

   public void setConnection(OpenWireConnection connection)
   {
      this.connection = connection;
   }

   public int incrementReference()
   {
      return referenceCounter.incrementAndGet();
   }

   public int decrementReference()
   {
      return referenceCounter.decrementAndGet();
   }

   public AtomicInteger getReferenceCounter()
   {
      return referenceCounter;
   }

   public void setReferenceCounter(AtomicInteger referenceCounter)
   {
      this.referenceCounter = referenceCounter;
   }

   public Object getConnectionMutex()
   {
      return connectionMutex;
   }

}
