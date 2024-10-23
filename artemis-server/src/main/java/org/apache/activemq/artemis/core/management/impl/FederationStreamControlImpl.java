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
package org.apache.activemq.artemis.core.management.impl;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;

import org.apache.activemq.artemis.api.core.management.FederationStreamControl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.federation.FederationStream;
import org.apache.activemq.artemis.logs.AuditLogger;

public class FederationStreamControlImpl extends AbstractControl implements FederationStreamControl {

   private final FederationStream federationStream;

   public FederationStreamControlImpl(final FederationStream federationStream,
                                      final StorageManager storageManager) throws Exception {
      super(FederationStreamControl.class, storageManager);
      this.federationStream = federationStream;
   }

   @Override
   public String getUser() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getUser(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConfig().getConnectionConfiguration().getUsername();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getName() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getName(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getName().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getStaticConnectors() throws Exception {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getStaticConnectors(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().getConfig().getStaticConnectors().toArray(new String[0]);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getDiscoveryGroupName() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getDiscoveryGroupName(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().getConfig().getDiscoveryGroupName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getRetryInterval() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getRetryInterval(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().getConfig().getRetryInterval();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public double getRetryIntervalMultiplier() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getRetryIntervalMultiplier(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().getConfig().getRetryIntervalMultiplier();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMaxRetryInterval() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getMaxRetryInterval(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().getConfig().getMaxRetryInterval();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getReconnectAttempts() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getReconnectAttempts(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().getConfig().getReconnectAttempts();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isSharedConnection() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.isSharedConnection(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().isSharedConnection();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPull() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.isPull(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().isPull();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isHA() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.isHA(this.federationStream);
      }
      clearIO();
      try {
         return federationStream.getConnection().getConfig().isHA();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(FederationStreamControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(FederationStreamControl.class);
   }

}
