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
package org.hornetq.core.server.cluster.ha;


import org.hornetq.core.config.BackupStrategy;

/**
 * a template for creating policy. this makes it easier in configuration and in embedded code to create a certain type
 * of policy. for instance:
 *
 * <ha-policy template="COLOCATED_REPLICATED"/>
 *
 * or in code
 *
 * HAPolicy policy = HAPolicyTemplate.COLOCATED_REPLICATED.getHaPolicy()
 */
public enum HAPolicyTemplate
{
   NONE(createNonePolicy()),
   REPLICATED(createReplicatedPolicy()),
   SHARED_STORE(createSharedStorePolicy()),
   BACKUP_REPLICATED(createBackupReplicatedPolicy()),
   BACKUP_SHARED_STORE(createBackupSharedStorePolicy()),
   COLOCATED_REPLICATED(createColocatedReplicatedPolicy()),
   COLOCATED_SHARED_STORE(createColocatedSharedStorePolicy());

   private final HAPolicy haPolicy;

   public HAPolicy getHaPolicy()
   {
      return haPolicy;
   }

   HAPolicyTemplate(HAPolicy haPolicy)
   {
      this.haPolicy = haPolicy;
   }

   private static HAPolicy createNonePolicy()
   {
      HAPolicy policy = new HAPolicy();
      policy.setPolicyType(HAPolicy.POLICY_TYPE.NONE);
      return policy;
   }

   private static HAPolicy createReplicatedPolicy()
   {
      HAPolicy policy = new HAPolicy();
      policy.setPolicyType(HAPolicy.POLICY_TYPE.REPLICATED);
      return policy;
   }

   private static HAPolicy createSharedStorePolicy()
   {
      HAPolicy policy = new HAPolicy();
      policy.setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      return policy;
   }

   private static HAPolicy createBackupReplicatedPolicy()
   {
      HAPolicy policy = new HAPolicy();
      policy.setMaxBackups(0);
      policy.setRequestBackup(false);
      policy.setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_REPLICATED);
      policy.setBackupStrategy(BackupStrategy.FULL);
      policy.setRestartBackup(true);
      return policy;
   }

   private static HAPolicy createBackupSharedStorePolicy()
   {
      HAPolicy policy = new HAPolicy();
      policy.setMaxBackups(0);
      policy.setRequestBackup(false);
      policy.setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      policy.setBackupStrategy(BackupStrategy.FULL);
      policy.setRestartBackup(true);
      return policy;
   }

   private static HAPolicy createColocatedSharedStorePolicy()
   {
      HAPolicy policy = new HAPolicy();
      policy.setBackupPortOffset(100);
      policy.setBackupRequestRetries(-1);
      policy.setBackupRequestRetryInterval(5000);
      policy.setMaxBackups(2);
      policy.setRequestBackup(true);
      policy.setPolicyType(HAPolicy.POLICY_TYPE.COLOCATED_SHARED_STORE);
      policy.setBackupStrategy(BackupStrategy.SCALE_DOWN);
      return policy;
   }

   private static HAPolicy createColocatedReplicatedPolicy()
   {
      HAPolicy policy = new HAPolicy();
      policy.setBackupPortOffset(100);
      policy.setBackupRequestRetries(-1);
      policy.setBackupRequestRetryInterval(5000);
      policy.setMaxBackups(2);
      policy.setRequestBackup(true);
      policy.setPolicyType(HAPolicy.POLICY_TYPE.COLOCATED_REPLICATED);
      policy.setBackupStrategy(BackupStrategy.SCALE_DOWN);
      return policy;
   }
}
