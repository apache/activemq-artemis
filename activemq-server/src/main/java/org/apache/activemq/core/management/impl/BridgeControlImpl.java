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
package org.apache.activemq.core.management.impl;

import javax.management.MBeanOperationInfo;

import org.apache.activemq.api.core.management.BridgeControl;
import org.apache.activemq.core.config.BridgeConfiguration;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.server.cluster.Bridge;

/**
 * A BridgeControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:09:04
 */
public class BridgeControlImpl extends AbstractControl implements BridgeControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Bridge bridge;

   private final BridgeConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BridgeControlImpl(final Bridge bridge,
                            final StorageManager storageManager,
                            final BridgeConfiguration configuration) throws Exception
   {
      super(BridgeControl.class, storageManager);
      this.bridge = bridge;
      this.configuration = configuration;
   }

   // BridgeControlMBean implementation ---------------------------

   public String[] getStaticConnectors() throws Exception
   {
      clearIO();
      try
      {
         return configuration.getStaticConnectors().toArray(new String[0]);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getForwardingAddress()
   {
      clearIO();
      try
      {
         return configuration.getForwardingAddress();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getQueueName()
   {
      clearIO();
      try
      {
         return configuration.getQueueName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getDiscoveryGroupName()
   {
      clearIO();
      try
      {
         return configuration.getDiscoveryGroupName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getFilterString()
   {
      clearIO();
      try
      {
         return configuration.getFilterString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getReconnectAttempts()
   {
      clearIO();
      try
      {
         return configuration.getReconnectAttempts();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getName()
   {
      clearIO();
      try
      {
         return configuration.getName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public long getRetryInterval()
   {
      clearIO();
      try
      {
         return configuration.getRetryInterval();
      }
      finally
      {
         blockOnIO();
      }
   }

   public double getRetryIntervalMultiplier()
   {
      clearIO();
      try
      {
         return configuration.getRetryIntervalMultiplier();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getTransformerClassName()
   {
      clearIO();
      try
      {
         return configuration.getTransformerClassName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isStarted()
   {
      clearIO();
      try
      {
         return bridge.isStarted();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isUseDuplicateDetection()
   {
      clearIO();
      try
      {
         return configuration.isUseDuplicateDetection();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isHA()
   {
      clearIO();
      try
      {
         return configuration.isHA();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void start() throws Exception
   {
      clearIO();
      try
      {
         bridge.start();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void stop() throws Exception
   {
      clearIO();
      try
      {
         bridge.stop();
         bridge.flushExecutor();
      }
      finally
      {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(BridgeControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
