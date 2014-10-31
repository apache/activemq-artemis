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
package org.hornetq.jms.server.recovery;

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.jms.server.HornetQJMSServerLogger;

/**
 * <p>This class will have a simple Connection Factory and will listen
 * for topology updates. </p>
 * <p>This Discovery is instantiated by {@link HornetQRecoveryRegistry}
 *
 * @author clebertsuconic
 */
public class RecoveryDiscovery implements SessionFailureListener
{

   private ServerLocator locator;
   private ClientSessionFactoryInternal sessionFactory;
   private final XARecoveryConfig config;
   private final AtomicInteger usage = new AtomicInteger(0);
   private boolean started = false;


   public RecoveryDiscovery(XARecoveryConfig config)
   {
      this.config = config;
   }

   public synchronized void start(boolean retry)
   {
      if (!started)
      {
         HornetQJMSServerLogger.LOGGER.debug("Starting RecoveryDiscovery on " + config);
         started = true;

         locator = config.createServerLocator();
         locator.disableFinalizeCheck();
         locator.addClusterTopologyListener(new InternalListener(config));
         try
         {
            sessionFactory = (ClientSessionFactoryInternal) locator.createSessionFactory();
            // We are using the SessionFactoryInternal here directly as we don't have information to connect with an user and password
            // on the session as all we want here is to get the topology
            // in case of failure we will retry
            sessionFactory.addFailureListener(this);

            HornetQJMSServerLogger.LOGGER.debug("RecoveryDiscovery started fine on " + config);
         }
         catch (Exception startupError)
         {
            if (!retry)
            {
               HornetQJMSServerLogger.LOGGER.xaRecoveryStartError(config);
            }
            stop();
            HornetQRecoveryRegistry.getInstance().failedDiscovery(this);
         }

      }
   }

   public synchronized void stop()
   {
      internalStop();
   }

   /**
    * we may have several connection factories referencing the same connection recovery entry.
    * Because of that we need to make a count of the number of the instances that are referencing it,
    * so we will remove it as soon as we are done
    */
   public int incrementUsage()
   {
      return usage.decrementAndGet();
   }

   public int decrementUsage()
   {
      return usage.incrementAndGet();
   }


   @Override
   protected void finalize()
   {
      // I don't think it's a good thing to synchronize a method on a finalize,
      // hence the internalStop (no sync) call here
      internalStop();
   }

   protected void internalStop()
   {
      if (started)
      {
         started = false;
         try
         {
            if (sessionFactory != null)
            {
               sessionFactory.close();
            }
         }
         catch (Exception ignored)
         {
            HornetQJMSServerLogger.LOGGER.debug(ignored, ignored);
         }

         try
         {
            locator.close();
         }
         catch (Exception ignored)
         {
            HornetQJMSServerLogger.LOGGER.debug(ignored, ignored);
         }

         sessionFactory = null;
         locator = null;
      }
   }


   static final class InternalListener implements ClusterTopologyListener
   {
      private final XARecoveryConfig config;

      public InternalListener(final XARecoveryConfig config)
      {
         this.config = config;
      }

      @Override
      public void nodeUP(TopologyMember topologyMember, boolean last)
      {
         // There is a case where the backup announce itself,
         // we need to ignore a case where getLive is null
         if (topologyMember.getLive() != null)
         {
            Pair<TransportConfiguration, TransportConfiguration> connector =
               new Pair<TransportConfiguration, TransportConfiguration>(topologyMember.getLive(),
                                                                        topologyMember.getBackup());
            HornetQRecoveryRegistry.getInstance().nodeUp(topologyMember.getNodeId(), connector,
                                                         config.getUsername(), config.getPassword());
         }
      }

      @Override
      public void nodeDown(long eventUID, String nodeID)
      {
         // I'm not putting any node down, since it may have previous transactions hanging, however at some point we may
         //change it have some sort of timeout for removal
      }

   }


   @Override
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      if (exception.getType() == HornetQExceptionType.DISCONNECTED)
      {
         HornetQJMSServerLogger.LOGGER.warn("being disconnected for server shutdown", exception);
      }
      else
      {
         HornetQJMSServerLogger.LOGGER.warn("Notified of connection failure in xa discovery, we will retry on the next recovery",
                                            exception);
      }
      internalStop();
      HornetQRecoveryRegistry.getInstance().failedDiscovery(this);
   }

   @Override
   public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
   {
      connectionFailed(me, failedOver);
   }

   @Override
   public void beforeReconnect(HornetQException exception)
   {
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "RecoveryDiscovery [config=" + config + ", started=" + started + "]";
   }

   @Override
   public int hashCode()
   {
      return config.hashCode();
   }

   @Override
   public boolean equals(Object o)
   {
      if (o == null || (!(o instanceof RecoveryDiscovery)))
      {
         return false;
      }
      RecoveryDiscovery discovery = (RecoveryDiscovery) o;

      return config.equals(discovery.config);
   }

}
