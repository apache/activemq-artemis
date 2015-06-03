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
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.management.ClusterConnectionControl;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.utils.json.JSONArray;

public class ClusterConnectionControlImpl extends AbstractControl implements ClusterConnectionControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClusterConnection clusterConnection;

   private final ClusterConnectionConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClusterConnectionControlImpl(final ClusterConnection clusterConnection,
                                       final StorageManager storageManager,
                                       final ClusterConnectionConfiguration configuration) throws Exception
   {
      super(ClusterConnectionControl.class, storageManager);
      this.clusterConnection = clusterConnection;
      this.configuration = configuration;
   }

   // ClusterConnectionControlMBean implementation ---------------------------

   public String getAddress()
   {
      clearIO();
      try
      {
         return configuration.getAddress();
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

   public int getMaxHops()
   {
      clearIO();
      try
      {
         return configuration.getMaxHops();
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

   public String getNodeID()
   {
      clearIO();
      try
      {
         return clusterConnection.getNodeID();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getStaticConnectors()
   {
      clearIO();
      try
      {
         if (configuration.getStaticConnectors() == null)
         {
            return null;
         }
         else
         {
            return configuration.getStaticConnectors().toArray(new String[0]);
         }
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getStaticConnectorsAsJSON() throws Exception
   {
      clearIO();
      try
      {
         List<String> connectors = configuration.getStaticConnectors();

         if (connectors == null)
         {
            return null;
         }

         JSONArray array = new JSONArray();

         for (String connector : connectors)
         {
            array.put(connector);
         }
         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isDuplicateDetection()
   {
      clearIO();
      try
      {
         return configuration.isDuplicateDetection();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isForwardWhenNoConsumers()
   {
      clearIO();
      try
      {
         return configuration.isForwardWhenNoConsumers();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getTopology()
   {
      clearIO();
      try
      {
         return clusterConnection.getTopology().describe();
      }
      finally
      {
         blockOnIO();
      }
   }

   public Map<String, String> getNodes() throws Exception
   {
      clearIO();
      try
      {
         return clusterConnection.getNodes();
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
         return clusterConnection.isStarted();
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
         clusterConnection.start();
         clusterConnection.flushExecutor();
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
         clusterConnection.stop();
         clusterConnection.flushExecutor();
      }
      finally
      {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(ClusterConnectionControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
