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

import java.util.Map;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.Divert;

public class DivertControlImpl extends AbstractControl implements DivertControl {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Divert divert;

   private final DivertConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // DivertControlMBean implementation ---------------------------

   public DivertControlImpl(final Divert divert,
                            final StorageManager storageManager,
                            final DivertConfiguration configuration) throws Exception {
      super(DivertControl.class, storageManager);
      this.divert = divert;
      this.configuration = configuration;
   }

   @Override
   public String getAddress() {
      clearIO();
      try {
         return configuration.getAddress();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getFilter() {
      clearIO();
      try {
         return configuration.getFilterString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getForwardingAddress() {
      clearIO();
      try {
         return configuration.getForwardingAddress();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getRoutingName() {
      clearIO();
      try {
         return divert.getRoutingName().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTransformerClassName() {
      clearIO();
      try {
         return configuration.getTransformerConfiguration() == null ? null : configuration.getTransformerConfiguration().getClassName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTransformerPropertiesAsJSON() {
      return JsonUtil.toJsonObject(getTransformerProperties()).toString();
   }

   @Override
   public Map<String, String> getTransformerProperties() {
      clearIO();
      try {
         return configuration.getTransformerConfiguration() == null ? null : configuration.getTransformerConfiguration().getProperties();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getRoutingType() {
      clearIO();
      try {
         return configuration.getRoutingType().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getUniqueName() {
      clearIO();
      try {
         return divert.getUniqueName().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isExclusive() {
      clearIO();
      try {
         return divert.isExclusive();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(DivertControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(DivertControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
