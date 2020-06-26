/*
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

package org.apache.activemq.artemis.cli.commands.check;

import java.util.ArrayList;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.management.NodeInfo;

@Command(name = "node", description = "Check a node")
public class NodeCheck extends CheckAbstract {

   @Option(name = "--up", description = "Check that the node is started, it is executed by default if there are no other checks")
   private boolean up;

   @Option(name = "--diskUsage", description = "Disk usage percentage to check or -1 to use the max-disk-usage")
   private Integer diskUsage;

   @Option(name = "--memoryUsage", description = "Memory usage percentage to check")
   private Integer memoryUsage;

   @Option(name = "--live", description = "Check that the node has a live")
   private boolean live;

   @Option(name = "--backup", description = "Check that the node has a backup")
   private boolean backup;

   @Option(name = "--peers", description = "Number of peers to check")
   private Integer peers;

   public boolean isUp() {
      return up;
   }

   public void setUp(boolean up) {
      this.up = up;
   }

   public Integer getDiskUsage() {
      return diskUsage;
   }

   public void setDiskUsage(Integer diskUsage) {
      this.diskUsage = diskUsage;
   }

   public Integer getMemoryUsage() {
      return memoryUsage;
   }

   public void setMemoryUsage(Integer memoryUsage) {
      this.memoryUsage = memoryUsage;
   }

   public boolean isLive() {
      return live;
   }

   public void setLive(boolean live) {
      this.live = live;
   }

   public boolean isBackup() {
      return backup;
   }

   public void setBackup(boolean backup) {
      this.backup = backup;
   }

   public Integer getPeers() {
      return peers;
   }

   public void setPeers(Integer peers) {
      this.peers = peers;
   }

   @Override
   protected CheckTask[] getCheckTasks() {
      ArrayList<CheckTask> checkTasks = new ArrayList<>();

      if (live) {
         checkTasks.add(new CheckTask("the node has a live", this::checkNodeLive));
      }

      if (backup) {
         checkTasks.add(new CheckTask("the node has a backup", this::checkNodeBackup));
      }

      if (peers != null) {
         if (peers > 0) {
            checkTasks.add(new CheckTask(String.format("there are %d peers", peers), this::checkNodePeers));
         } else {
            throw new IllegalArgumentException("Invalid peers number to check: " + peers);
         }
      }

      if (diskUsage != null) {
         if (diskUsage == -1) {
            checkTasks.add(new CheckTask("the disk usage is less then the max-disk-usage", this::checkNodeDiskUsage));
         } else if (diskUsage > 0 && diskUsage < 100) {
            checkTasks.add(new CheckTask("the disk usage is less then " + diskUsage, this::checkNodeDiskUsage));
         } else {
            throw new IllegalArgumentException("Invalid disk usage percentage: " + diskUsage);
         }
      }

      if (memoryUsage != null) {
         if (memoryUsage > 0 && memoryUsage < 100) {
            checkTasks.add(new CheckTask("the memory usage is less then " + memoryUsage, this::checkNodeMemoryUsage));
         } else {
            throw new IllegalArgumentException("Invalid memory usage percentage: " + memoryUsage);
         }
      }

      if (up || checkTasks.size() == 0) {
         checkTasks.add(0, new CheckTask("the node is started", this::checkNodeUp));
      }

      return checkTasks.toArray(new CheckTask[checkTasks.size()]);
   }

   private void checkNodeUp(final CheckContext context) throws Exception {
      if (!context.getManagementProxy().invokeOperation(Boolean.class, "broker", "isStarted")) {
         throw new CheckException("The node isn't started.");
      }
   }

   private void checkNodeLive(final CheckContext context) throws Exception {
      String nodeId = getName();

      if (nodeId == null) {
         nodeId = context.getNodeId();
      }

      NodeInfo node = context.getTopology().get(nodeId);

      if (node == null || node.getLive() == null) {
         throw new CheckException("No live found for the node " + nodeId);
      }
   }

   private void checkNodeBackup(final CheckContext context) throws Exception {
      String nodeId = getName();

      if (nodeId == null) {
         nodeId = context.getNodeId();
      }

      NodeInfo node = context.getTopology().get(nodeId);

      if (node == null || node.getBackup() == null) {
         throw new CheckException("No backup found for the node " + nodeId);
      }
   }

   private void checkNodePeers(final CheckContext context) throws Exception {
      int topologyPeers = context.getTopology().values().stream().
         mapToInt(node -> (node.getLive() != null ? 1 : 0) +
            (node.getBackup() != null ? 1 : 0)).sum();

      if (topologyPeers < peers) {
         throw new CheckException("Insufficient peers: " + peers);
      }
   }

   private void checkNodeDiskUsage(final CheckContext context) throws Exception {
      int thresholdValue;

      if (diskUsage == -1) {
         thresholdValue = context.getManagementProxy().invokeOperation(
            int.class, "broker", "getMaxDiskUsage");
      } else {
         thresholdValue = diskUsage;
      }

      checkNodeUsage(context, "getDiskStoreUsage", thresholdValue);
   }

   private void checkNodeMemoryUsage(final CheckContext context) throws Exception {
      checkNodeUsage(context, "getAddressMemoryUsagePercentage", memoryUsage);
   }

   private void checkNodeUsage(final CheckContext context, final String name, final int thresholdValue) throws Exception {
      int usageValue = context.getManagementProxy().invokeOperation(int.class, "broker", name);

      if (usageValue > thresholdValue) {
         throw new CheckException("The " + (name.startsWith("get") ? name.substring(3) : name) +
                                     " " + usageValue + " is less than " + thresholdValue);
      }
   }
}
