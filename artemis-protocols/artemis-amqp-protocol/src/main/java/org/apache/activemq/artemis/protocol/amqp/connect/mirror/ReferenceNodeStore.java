/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.util.HashMap;

import io.netty.util.collection.LongObjectHashMap;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.utils.collections.NodeStore;
import org.apache.activemq.artemis.utils.collections.LinkedListImpl;

import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_BROKER_ID_EXTRA_PROPERTY;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_ID_EXTRA_PROPERTY;

public class ReferenceNodeStore implements NodeStore<MessageReference> {

   private final String serverID;

   public ReferenceNodeStore(ActiveMQServer server) {
      this.serverID = server.getNodeID().toString();
   }

   HashMap<String, LongObjectHashMap<LinkedListImpl.Node<MessageReference>>> lists;

   String lruListID;
   LongObjectHashMap<LinkedListImpl.Node<MessageReference>> lruMap;

   @Override
   public void storeNode(MessageReference element, LinkedListImpl.Node<MessageReference> node) {
      String list = getServerID(element);
      long id = getID(element);
      storeNode(list, id, node);
   }

   private void storeNode(String serverID, long id, LinkedListImpl.Node<MessageReference> node) {
      LongObjectHashMap<LinkedListImpl.Node<MessageReference>> nodesMap = getMap(serverID);
      if (nodesMap != null) {
         synchronized (nodesMap) {
            nodesMap.put(id, node);
         }
      }
   }

   @Override
   public void removeNode(MessageReference element, LinkedListImpl.Node<MessageReference> node) {
      long id = getID(element);
      String serverID = getServerID(element);
      LongObjectHashMap<LinkedListImpl.Node<MessageReference>> nodeMap = getMap(serverID);
      if (nodeMap != null) {
         synchronized (nodeMap) {
            nodeMap.remove(id);
         }
      }
   }


   @Override
   public LinkedListImpl.Node<MessageReference> getNode(String serverID, long id) {
      LongObjectHashMap<LinkedListImpl.Node<MessageReference>> nodeMap = getMap(serverID);

      assert nodeMap != null;

      synchronized (nodeMap) {
         return nodeMap.get(id);
      }
   }

   /** notice getMap should always return an instance. It should never return null. */
   private synchronized LongObjectHashMap<LinkedListImpl.Node<MessageReference>> getMap(String serverID) {
      if (serverID == null) {
         serverID = this.serverID; // returning for the localList in case it's null
      }

      if (lruListID != null && lruListID.equals(serverID)) {
         return lruMap;
      }

      if (lists == null) {
         lists = new HashMap<>();
      }
      LongObjectHashMap<LinkedListImpl.Node<MessageReference>> theList = lists.get(serverID);
      if (theList == null) {
         theList = new LongObjectHashMap<>();
         lists.put(serverID, theList);
      }

      lruMap = theList; // cached result
      lruListID = serverID;

      return theList;
   }

   public String getServerID(MessageReference element) {
      Object nodeID = element.getMessage().getBrokerProperty(INTERNAL_BROKER_ID_EXTRA_PROPERTY);
      if (nodeID != null) {
         return nodeID.toString();
      } else {
         // it is important to return null here, as the MirrorSource is expecting it to be null
         // in the case the nodeID being from the originating server.
         // don't be tempted to return this.serverID here.
         return null;
      }
   }

   public long getID(MessageReference element) {
      Long id = (Long) element.getMessage().getBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY);
      if (id == null) {
         return element.getMessageID();
      } else {
         return id;
      }
   }

   @Override
   public synchronized void clear() {
      lists.forEach((k, v) -> v.clear());
      lists.clear();
      lruListID = null;
      lruMap = null;
   }

   @Override
   public int size() {
      int size = 0;
      for (LongObjectHashMap mapValue : lists.values()) {
         size += mapValue.size();
      }
      return size;
   }

}
