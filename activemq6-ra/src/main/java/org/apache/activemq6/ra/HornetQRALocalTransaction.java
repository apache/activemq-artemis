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

import javax.jms.JMSException;
import javax.resource.ResourceException;
import javax.resource.spi.LocalTransaction;


/**
 * JMS Local transaction
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRALocalTransaction implements LocalTransaction
{
   /** Trace enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /** The managed connection */
   private final HornetQRAManagedConnection mc;

   /**
    * Constructor
    * @param mc The managed connection
    */
   public HornetQRALocalTransaction(final HornetQRAManagedConnection mc)
   {
      if (HornetQRALocalTransaction.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + mc + ")");
      }

      this.mc = mc;
   }

   /**
    * Begin
    * @exception ResourceException Thrown if the operation fails
    */
   public void begin() throws ResourceException
   {
      if (HornetQRALocalTransaction.trace)
      {
         HornetQRALogger.LOGGER.trace("begin()");
      }

     // mc.setInManagedTx(true);
   }

   /**
    * Commit
    * @exception ResourceException Thrown if the operation fails
    */
   public void commit() throws ResourceException
   {
      if (HornetQRALocalTransaction.trace)
      {
         HornetQRALogger.LOGGER.trace("commit()");
      }

      mc.lock();
      try
      {
         if (mc.getSession().getTransacted())
         {
            mc.getSession().commit();
         }
      }
      catch (JMSException e)
      {
         throw new ResourceException("Could not commit LocalTransaction", e);
      }
      finally
      {
         //mc.setInManagedTx(false);
         mc.unlock();
      }
   }

   /**
    * Rollback
    * @exception ResourceException Thrown if the operation fails
    */
   public void rollback() throws ResourceException
   {
      if (HornetQRALocalTransaction.trace)
      {
         HornetQRALogger.LOGGER.trace("rollback()");
      }

      mc.lock();
      try
      {
         if (mc.getSession().getTransacted())
         {
            mc.getSession().rollback();
         }
      }
      catch (JMSException ex)
      {
         throw new ResourceException("Could not rollback LocalTransaction", ex);
      }
      finally
      {
         //mc.setInManagedTx(false);
         mc.unlock();
      }
   }
}
