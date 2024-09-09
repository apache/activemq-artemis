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

import org.apache.activemq.artemis.api.core.management.FederationRemoteConsumerControl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.federation.FederatedConsumerKey;
import org.apache.activemq.artemis.core.server.federation.FederatedQueueConsumer;
import org.apache.activemq.artemis.logs.AuditLogger;

public class FederationRemoteConsumerControlImpl extends AbstractControl implements FederationRemoteConsumerControl {

   private final FederatedConsumerKey federatedConsumerKey;
   private final FederatedQueueConsumer federatedConsumer;

   public FederationRemoteConsumerControlImpl(final FederatedQueueConsumer federatedConsumer,
                                              final StorageManager storageManager) throws Exception {
      super(FederationRemoteConsumerControl.class, storageManager);
      this.federatedConsumerKey = federatedConsumer.getKey();
      this.federatedConsumer = federatedConsumer;
   }

   @Override
   public String getAddress() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getAddress(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumerKey.getAddress().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getPriority() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getPriority(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumerKey.getPriority();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getRoutingType() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getRoutingType(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumerKey.getRoutingType().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getFilterString() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getFilterString(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumerKey.getFilterString().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getQueueName() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getQueueName(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumerKey.getQueueName().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getQueueFilterString() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getFilterString(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumerKey.getQueueFilterString().toString();
      } finally {
         blockOnIO();
      }
   }

   /*
   @Override
   public String getFqqn() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getFqqn(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumerKey.getFqqn().toString();
      } finally {
         blockOnIO();
      }
   }
*/

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(FederationRemoteConsumerControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(FederationRemoteConsumerControl.class);
   }

   @Override
   public long getFederatedMessageCount() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getMessageCount(federatedConsumer);
      }
      clearIO();
      try {
         return federatedConsumer.federatedMessageCount();
      } finally {
         blockOnIO();
      }
   }

}
