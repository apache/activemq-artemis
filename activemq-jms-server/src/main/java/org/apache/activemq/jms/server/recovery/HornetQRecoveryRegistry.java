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
package org.apache.activemq.jms.server.recovery;

import javax.transaction.xa.XAResource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.jms.server.HornetQJMSServerLogger;
import org.jboss.tm.XAResourceRecovery;

/**
 * <p>This class is used by the Resource Adapter to register RecoveryDiscovery, which is based on the {@link XARecoveryConfig}</p>
 * <p>Each outbound or inboud connection will pass the configuration here through by calling the method {@link HornetQRecoveryRegistry#register(XARecoveryConfig)}</p>
 * <p>Later the {@link RecoveryDiscovery} will call {@link HornetQRecoveryRegistry#nodeUp(String, Pair, String, String)}
 * so we will keep a track of nodes on the cluster
 * or nodes where this server is connected to. </p>
 *
 * @author clebertsuconic
 */
public class HornetQRecoveryRegistry implements XAResourceRecovery
{

   private static final HornetQRecoveryRegistry theInstance = new HornetQRecoveryRegistry();

   private final ConcurrentHashMap<XARecoveryConfig, RecoveryDiscovery> configSet = new ConcurrentHashMap<XARecoveryConfig, RecoveryDiscovery>();

   /**
    * The list by server id and resource adapter wrapper, what will actually be calling recovery.
    * This will be returned by getXAResources
    */
   private final ConcurrentHashMap<String, HornetQXAResourceWrapper> recoveries = new ConcurrentHashMap<String, HornetQXAResourceWrapper>();

   /**
    * In case of failures, we retry on the next getXAResources
    */
   private final Set<RecoveryDiscovery> failedDiscoverySet = new HashSet<RecoveryDiscovery>();

   private HornetQRecoveryRegistry()
   {
   }

   /**
    * This will be called periodically by the Transaction Manager
    */
   public XAResource[] getXAResources()
   {
      try
      {
         checkFailures();

         HornetQXAResourceWrapper[] resourceArray = new HornetQXAResourceWrapper[recoveries.size()];
         resourceArray = recoveries.values().toArray(resourceArray);

         if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQJMSServerLogger.LOGGER.debug("\n=======================================================================================");
            HornetQJMSServerLogger.LOGGER.debug("Returning the following list on getXAREsources:");
            for (Map.Entry<String, HornetQXAResourceWrapper> entry : recoveries.entrySet())
            {
               HornetQJMSServerLogger.LOGGER.debug("server-id=" + entry.getKey() + ", value=" + entry.getValue());
            }
            HornetQJMSServerLogger.LOGGER.debug("=======================================================================================\n");
         }

         return resourceArray;
      }
      catch (Throwable e)
      {
         HornetQJMSServerLogger.LOGGER.warn(e.getMessage(), e);
         return new XAResource[]{};
      }
   }

   public static HornetQRecoveryRegistry getInstance()
   {
      return theInstance;
   }

   /**
    * This will be called by then resource adapters, to register a new discovery
    *
    * @param resourceConfig
    */
   public void register(final XARecoveryConfig resourceConfig)
   {
      RecoveryDiscovery newInstance = new RecoveryDiscovery(resourceConfig);
      RecoveryDiscovery discoveryRecord = configSet.putIfAbsent(resourceConfig, newInstance);
      if (discoveryRecord == null)
      {
         discoveryRecord = newInstance;
         discoveryRecord.start(false);
      }
      // you could have a configuration shared with multiple MDBs or RAs
      discoveryRecord.incrementUsage();
   }

   /**
    * Reference counts and deactivate a configuration
    * Notice: this won't remove the servers since a server may have previous XIDs
    *
    * @param resourceConfig
    */
   public void unRegister(final XARecoveryConfig resourceConfig)
   {
      RecoveryDiscovery discoveryRecord = configSet.get(resourceConfig);
      if (discoveryRecord != null && discoveryRecord.decrementUsage() == 0)
      {
         discoveryRecord = configSet.remove(resourceConfig);
         if (discoveryRecord != null)
         {
            discoveryRecord.stop();
         }
      }
   }

   /**
    * We need to make sure that all resources are closed, we don't actually do this when a resourceConfig is closed but
    * maybe we should.
    */
   public void stop()
   {
      for (RecoveryDiscovery recoveryDiscovery : configSet.values())
      {
         recoveryDiscovery.stop();
      }
      for (HornetQXAResourceWrapper hornetQXAResourceWrapper : recoveries.values())
      {
         hornetQXAResourceWrapper.close();
      }
      recoveries.clear();
      configSet.clear();
   }

   /**
    * in case of a failure the Discovery will register itslef to retry
    *
    * @param failedDiscovery
    */
   public void failedDiscovery(RecoveryDiscovery failedDiscovery)
   {
      HornetQJMSServerLogger.LOGGER.debug("RecoveryDiscovery being set to restart:" + failedDiscovery);
      synchronized (failedDiscoverySet)
      {
         failedDiscoverySet.add(failedDiscovery);
      }
   }

   /**
    * @param nodeID
    * @param networkConfiguration
    * @param username
    * @param password
    */
   public void nodeUp(String nodeID,
                      Pair<TransportConfiguration, TransportConfiguration> networkConfiguration,
                      String username,
                      String password)
   {

      if (recoveries.get(nodeID) == null)
      {
         if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQJMSServerLogger.LOGGER.debug(nodeID + " being registered towards " + networkConfiguration);
         }
         XARecoveryConfig config = new XARecoveryConfig(true,
                                                        extractTransportConfiguration(networkConfiguration),
                                                        username,
                                                        password);

         HornetQXAResourceWrapper wrapper = new HornetQXAResourceWrapper(config);
         recoveries.putIfAbsent(nodeID, wrapper);
      }
   }

   public void nodeDown(String nodeID)
   {
   }

   /**
    * this will go through the list of retries
    */
   private void checkFailures()
   {
      final HashSet<RecoveryDiscovery> failures = new HashSet<RecoveryDiscovery>();

      // it will transfer all the discoveries to a new collection
      synchronized (failedDiscoverySet)
      {
         failures.addAll(failedDiscoverySet);
         failedDiscoverySet.clear();
      }

      if (failures.size() > 0)
      {
         // This shouldn't happen on a regular scenario, however when this retry happens this needs
         // to be done on a new thread
         Thread t = new Thread("HornetQ Recovery Discovery Reinitialization")
         {
            @Override
            public void run()
            {
               for (RecoveryDiscovery discovery : failures)
               {
                  try
                  {
                     HornetQJMSServerLogger.LOGGER.debug("Retrying discovery " + discovery);
                     discovery.start(true);
                  }
                  catch (Throwable e)
                  {
                     HornetQJMSServerLogger.LOGGER.warn(e.getMessage(), e);
                  }
               }
            }
         };

         t.start();
      }
   }

   /**
    * @param networkConfiguration
    * @return
    */
   private TransportConfiguration[] extractTransportConfiguration(Pair<TransportConfiguration, TransportConfiguration> networkConfiguration)
   {
      if (networkConfiguration.getB() != null)
      {
         return new TransportConfiguration[]{networkConfiguration.getA(), networkConfiguration.getB()};
      }
      return new TransportConfiguration[]{networkConfiguration.getA()};
   }

}
