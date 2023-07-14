/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.util.HashMap;

import io.netty.util.collection.LongObjectHashMap;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.utils.collections.NodeStore;
import org.apache.activemq.artemis.utils.collections.LinkedListImpl;

public class ReferenceNodeStore implements NodeStore<MessageReference> {

   private final ReferenceNodeStoreFactory factory;

   public ReferenceNodeStore(ReferenceNodeStoreFactory factory) {
      this.factory = factory;
   }

   // This is where the messages are stored by server id...
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
         serverID = factory.getDefaultNodeID();
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
      return factory.getServerID(element);
   }

   public String getServerID(Message message) {
      return factory.getServerID(message);
   }

   public long getID(MessageReference element) {
      return factory.getID(element);
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
