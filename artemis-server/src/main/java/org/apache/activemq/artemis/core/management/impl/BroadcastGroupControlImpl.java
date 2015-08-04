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

import javax.management.MBeanOperationInfo;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.management.BroadcastGroupControl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.cluster.BroadcastGroup;
import org.apache.activemq.artemis.utils.json.JSONArray;

public class BroadcastGroupControlImpl extends AbstractControl implements BroadcastGroupControl {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final BroadcastGroup broadcastGroup;

   private final BroadcastGroupConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BroadcastGroupControlImpl(final BroadcastGroup broadcastGroup,
                                    final StorageManager storageManager,
                                    final BroadcastGroupConfiguration configuration) throws Exception {
      super(BroadcastGroupControl.class, storageManager);
      this.broadcastGroup = broadcastGroup;
      this.configuration = configuration;
   }

   // BroadcastGroupControlMBean implementation ---------------------

   public String getName() {
      clearIO();
      try {
         return configuration.getName();
      }
      finally {
         blockOnIO();
      }
   }

   public long getBroadcastPeriod() {
      clearIO();
      try {
         return configuration.getBroadcastPeriod();
      }
      finally {
         blockOnIO();
      }
   }

   public Object[] getConnectorPairs() {
      clearIO();
      try {
         Object[] ret = new Object[configuration.getConnectorInfos().size()];

         int i = 0;
         for (String connector : configuration.getConnectorInfos()) {
            ret[i++] = connector;
         }

         return ret;
      }
      finally {
         blockOnIO();
      }
   }

   public String getConnectorPairsAsJSON() throws Exception {
      clearIO();
      try {
         JSONArray array = new JSONArray();

         for (String connector : configuration.getConnectorInfos()) {
            array.put(connector);
         }
         return array.toString();
      }
      finally {
         blockOnIO();
      }
   }

   //todo ghoward we should deal with this properly
   public String getGroupAddress() throws Exception {
      clearIO();
      try {
         if (configuration.getEndpointFactory() instanceof UDPBroadcastEndpointFactory) {
            return ((UDPBroadcastEndpointFactory) configuration.getEndpointFactory()).getGroupAddress();
         }
         throw new Exception("Invalid request because this is not a UDP Broadcast configuration.");
      }
      finally {
         blockOnIO();
      }
   }

   public int getGroupPort() throws Exception {
      clearIO();
      try {
         if (configuration.getEndpointFactory() instanceof UDPBroadcastEndpointFactory) {
            return ((UDPBroadcastEndpointFactory) configuration.getEndpointFactory()).getGroupPort();
         }
         throw new Exception("Invalid request because this is not a UDP Broadcast configuration.");
      }
      finally {
         blockOnIO();
      }
   }

   public int getLocalBindPort() throws Exception {
      clearIO();
      try {
         if (configuration.getEndpointFactory() instanceof UDPBroadcastEndpointFactory) {
            return ((UDPBroadcastEndpointFactory) configuration.getEndpointFactory()).getLocalBindPort();
         }
         throw new Exception("Invalid request because this is not a UDP Broadcast configuration.");
      }
      finally {
         blockOnIO();
      }
   }

   // MessagingComponentControlMBean implementation -----------------

   public boolean isStarted() {
      clearIO();
      try {
         return broadcastGroup.isStarted();
      }
      finally {
         blockOnIO();
      }
   }

   public void start() throws Exception {
      clearIO();
      try {
         broadcastGroup.start();
      }
      finally {
         blockOnIO();
      }
   }

   public void stop() throws Exception {
      clearIO();
      try {
         broadcastGroup.stop();
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(BroadcastGroupControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
