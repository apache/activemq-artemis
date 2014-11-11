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
package org.apache.activemq6.ra;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;

import org.apache.activemq6.utils.ConcurrentHashSet;


/**
 * The connection manager used in non-managed environments.
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAConnectionManager implements ConnectionManager
{
   /** Serial version UID */
   static final long serialVersionUID = 4409118162975011014L;
   /** Trace enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * Constructor
    */
   public HornetQRAConnectionManager()
   {
      if (HornetQRAConnectionManager.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor()");
      }
   }

   ConcurrentHashSet<ManagedConnection> connections = new ConcurrentHashSet<>();

   /**
    * Allocates a connection
    * @param mcf The managed connection factory
    * @param cxRequestInfo The connection request information
    * @return The connection
    * @exception ResourceException Thrown if there is a problem obtaining the connection
    */
   public Object allocateConnection(final ManagedConnectionFactory mcf, final ConnectionRequestInfo cxRequestInfo) throws ResourceException
   {
      if (HornetQRAConnectionManager.trace)
      {
         HornetQRALogger.LOGGER.trace("allocateConnection(" + mcf + ", " + cxRequestInfo + ")");
      }

      ManagedConnection mc = mcf.createManagedConnection(null, cxRequestInfo);
      Object c = mc.getConnection(null, cxRequestInfo);

      if (HornetQRAConnectionManager.trace)
      {
         HornetQRALogger.LOGGER.trace("Allocated connection: " + c + ", with managed connection: " + mc);
      }

      connections.add(mc);
      return c;
   }

   public void stop()
   {
      for (ManagedConnection conn : connections)
      {
         try
         {
            conn.destroy();
         }
         catch (Throwable e)
         {

         }
      }
   }
}
