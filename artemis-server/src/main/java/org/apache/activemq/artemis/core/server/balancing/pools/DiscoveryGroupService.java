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

package org.apache.activemq.artemis.core.server.balancing.pools;

import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.cluster.DiscoveryListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DiscoveryGroupService extends DiscoveryService implements DiscoveryListener {
   private final DiscoveryGroup discoveryGroup;

   private final Map<String, Entry> entries = new ConcurrentHashMap<>();

   public DiscoveryGroupService(DiscoveryGroup discoveryGroup) {
      this.discoveryGroup = discoveryGroup;
   }

   @Override
   public void start() throws Exception {
      discoveryGroup.registerListener(this);

      discoveryGroup.start();
   }

   @Override
   public void stop() throws Exception {
      discoveryGroup.unregisterListener(this);

      discoveryGroup.stop();

      entries.clear();
   }

   @Override
   public boolean isStarted() {
      return discoveryGroup.isStarted();
   }

   @Override
   public void connectorsChanged(List<DiscoveryEntry> newEntries) {
      Map<String, Entry> oldEntries = new HashMap<>(entries);

      for (DiscoveryEntry newEntry : newEntries) {
         Entry oldEntry = oldEntries.remove(newEntry.getNodeID());

         if (oldEntry == null) {
            Entry addingEntry = new Entry(newEntry.getNodeID(), newEntry.getConnector());

            entries.put(addingEntry.getNodeID(), addingEntry);

            fireEntryAddedEvent(addingEntry);
         } else if (!newEntry.getConnector().equals(oldEntry.getConnector())) {
            Entry updatingEntry = new Entry(newEntry.getNodeID(), newEntry.getConnector());

            entries.put(updatingEntry.getNodeID(), updatingEntry);

            fireEntryUpdatedEvent(oldEntry, updatingEntry);
         }
      }

      oldEntries.forEach((nodeID, entry) -> {
         entries.remove(nodeID);

         fireEntryRemovedEvent(entry);
      });
   }
}
