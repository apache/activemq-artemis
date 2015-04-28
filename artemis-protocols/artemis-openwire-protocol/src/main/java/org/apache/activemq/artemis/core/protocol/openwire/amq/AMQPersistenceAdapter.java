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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import java.io.IOException;
import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ProducerId;

public class AMQPersistenceAdapter
{

   /**
    * Returns a set of all the {@link org.apache.activemq.command.ActiveMQDestination}
    * objects that the persistence store is aware exist.
    *
    * @return active destinations
    */
   Set<ActiveMQDestination> getDestinations()
   {
      return null;
   }

   /**
    * Factory method to create a new queue message store with the given destination name
    * @param destination
    * @return the message store
    * @throws IOException
    */
   AMQMessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException
   {
      return null;
   }

   /**
    * return the last stored producer sequenceId for this producer Id
    * used to suppress duplicate sends on failover reconnect at the transport
    * when a reconnect occurs
    * @param id the producerId to find a sequenceId for
    * @return the last stored sequence id or -1 if no suppression needed
    */
   public long getLastProducerSequenceId(ProducerId id)
   {
      return 0;
   }
}
