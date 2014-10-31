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

import java.util.Arrays;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.jms.server.HornetQJMSServerLogger;

/**
 * XAResourceWrapper.
 *
 * Mainly from org.jboss.server.XAResourceWrapper from the JBoss AS server module
 *
 * The reason why we don't use that class directly is that it assumes on failure of connection
 * the RM_FAIL or RM_ERR is thrown, but in HornetQ we throw XA_RETRY since we want the recovery manager to be able
 * to retry on failure without having to manually retry
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *
 * @version $Revision: 45341 $
 */
public class HornetQXAResourceWrapper implements XAResource, SessionFailureListener
{
   /** The state lock */
   private static final Object lock = new Object();

   private ServerLocator serverLocator;

   private ClientSessionFactory csf;

   private ClientSession delegate;

   private XARecoveryConfig[] xaRecoveryConfigs;

   public HornetQXAResourceWrapper(XARecoveryConfig... xaRecoveryConfigs)
   {
      this.xaRecoveryConfigs = xaRecoveryConfigs;

      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("Recovery configured with " + Arrays.toString(xaRecoveryConfigs) +
            ", instance=" +
            System.identityHashCode(this));
      }
   }

   public Xid[] recover(final int flag) throws XAException
   {
      XAResource xaResource = getDelegate(false);

      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("looking for recover at " + xaResource + " configuration " + Arrays.toString(this.xaRecoveryConfigs));
      }

      try
      {
         Xid[] xids = xaResource.recover(flag);

         if (HornetQJMSServerLogger.LOGGER.isDebugEnabled() && xids != null && xids.length > 0)
         {
            HornetQJMSServerLogger.LOGGER.debug("Recovering these following IDs " + Arrays.toString(xids) + " at " + this);
         }

         return xids;
      }
      catch (XAException e)
      {
         HornetQJMSServerLogger.LOGGER.xaRecoverError(e);
         throw check(e);
      }
   }

   public void commit(final Xid xid, final boolean onePhase) throws XAException
   {
      XAResource xaResource = getDelegate(true);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("Commit " + xaResource + " xid " + " onePhase=" + onePhase);
      }
      try
      {
         xaResource.commit(xid, onePhase);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void rollback(final Xid xid) throws XAException
   {
      XAResource xaResource = getDelegate(true);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("Rollback " + xaResource + " xid ");
      }
      try
      {
         xaResource.rollback(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void forget(final Xid xid) throws XAException
   {
      XAResource xaResource = getDelegate(false);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("Forget " + xaResource + " xid ");
      }

      try
      {
         xaResource.forget(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public boolean isSameRM(XAResource xaRes) throws XAException
   {
      if (xaRes instanceof HornetQXAResourceWrapper)
      {
         xaRes = ((HornetQXAResourceWrapper)xaRes).getDelegate(false);
      }

      XAResource xaResource = getDelegate(false);
      try
      {
         return xaResource.isSameRM(xaRes);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public int prepare(final Xid xid) throws XAException
   {
      XAResource xaResource = getDelegate(true);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("prepare " + xaResource + " xid ");
      }
      try
      {
         return xaResource.prepare(xid);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void start(final Xid xid, final int flags) throws XAException
   {
      XAResource xaResource = getDelegate(false);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("start " + xaResource + " xid ");
      }
      try
      {
         xaResource.start(xid, flags);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void end(final Xid xid, final int flags) throws XAException
   {
      XAResource xaResource = getDelegate(false);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("end " + xaResource + " xid ");
      }
      try
      {
         xaResource.end(xid, flags);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public int getTransactionTimeout() throws XAException
   {
      XAResource xaResource = getDelegate(false);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("getTransactionTimeout " + xaResource + " xid ");
      }
      try
      {
         return xaResource.getTransactionTimeout();
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public boolean setTransactionTimeout(final int seconds) throws XAException
   {
      XAResource xaResource = getDelegate(false);
      if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQJMSServerLogger.LOGGER.debug("setTransactionTimeout " + xaResource + " xid ");
      }
      try
      {
         return xaResource.setTransactionTimeout(seconds);
      }
      catch (XAException e)
      {
         throw check(e);
      }
   }

   public void connectionFailed(final HornetQException me, boolean failedOver)
   {
      if (me.getType() == HornetQExceptionType.DISCONNECTED)
      {
         if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQJMSServerLogger.LOGGER.debug("being disconnected for server shutdown", me);
         }
      }
      else
      {
         HornetQJMSServerLogger.LOGGER.xaRecoverConnectionError(me, csf);
      }
      close();
   }

   @Override
   public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
   {
      connectionFailed(me, failedOver);
   }

   public void beforeReconnect(final HornetQException me)
   {
   }

   /**
    * Get the connectionFactory XAResource
    *
    * @return the connectionFactory
    * @throws XAException for any problem
    */
   private XAResource getDelegate(boolean retry) throws XAException
   {
      XAResource result = null;
      Exception error = null;
      try
      {
         result = connect();
      }
      catch (Exception e)
      {
         error = e;
      }

      if (result == null)
      {
         // we should always throw a retry for certain methods comit etc, if not the tx is marked as a heuristic and
         // all chaos is let loose
         if (retry)
         {
            XAException xae = new XAException("Connection unavailable for xa recovery");
            xae.errorCode = XAException.XA_RETRY;
            if (error != null)
            {
               xae.initCause(error);
            }
            if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQJMSServerLogger.LOGGER.debug("Cannot get connectionFactory XAResource", xae);
            }
            throw xae;
         }
         else
         {
            XAException xae = new XAException("Error trying to connect to any providers for xa recovery");
            xae.errorCode = XAException.XAER_RMERR;
            if (error != null)
            {
               xae.initCause(error);
            }
            if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQJMSServerLogger.LOGGER.debug("Cannot get connectionFactory XAResource", xae);
            }
            throw xae;
         }

      }

      return result;
   }

   /**
    * Connect to the server if not already done so
    *
    * @return the connectionFactory XAResource
    * @throws Exception for any problem
    */
   protected XAResource connect() throws Exception
   {
      // Do we already have a valid connectionFactory?
      synchronized (HornetQXAResourceWrapper.lock)
      {
         if (delegate != null)
         {
            return delegate;
         }
      }

      for (XARecoveryConfig xaRecoveryConfig : xaRecoveryConfigs)
      {

         if (xaRecoveryConfig == null)
         {
            continue;
         }
         if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQJMSServerLogger.LOGGER.debug("Trying to connect recovery on " + xaRecoveryConfig + " of " + Arrays.toString(xaRecoveryConfigs));
         }

         ClientSession cs = null;

         try
         {
            // setting ha=false because otherwise the connector would go towards any server, causing Heuristic exceptions
            // we really need to control what server it's connected to

            // Manual configuration may still use discovery, so we will keep this
            if (xaRecoveryConfig.getDiscoveryConfiguration() != null)
            {
               serverLocator = HornetQClient.createServerLocator(false, xaRecoveryConfig.getDiscoveryConfiguration());
            }
            else
            {
               serverLocator = HornetQClient.createServerLocator(false, xaRecoveryConfig.getTransportConfig());
            }
            serverLocator.disableFinalizeCheck();
            csf = serverLocator.createSessionFactory();
            if (xaRecoveryConfig.getUsername() == null)
            {
               cs = csf.createSession(true, false, false);
            }
            else
            {
               cs = csf.createSession(xaRecoveryConfig.getUsername(),
                  xaRecoveryConfig.getPassword(),
                  true,
                  false,
                  false,
                  false,
                  1);
            }
         }
         catch (Throwable e)
         {
            HornetQJMSServerLogger.LOGGER.xaRecoverAutoConnectionError(e, xaRecoveryConfig);
            if (HornetQJMSServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQJMSServerLogger.LOGGER.debug(e.getMessage(), e);
            }

            try
            {
               if (cs != null) cs.close();
               if (serverLocator != null) serverLocator.close();
            }
            catch (Throwable ignored)
            {
               if (HornetQJMSServerLogger.LOGGER.isTraceEnabled())
               {
                  HornetQJMSServerLogger.LOGGER.trace(e.getMessage(), ignored);
               }
            }
            continue;
         }

         cs.addFailureListener(this);

         synchronized (HornetQXAResourceWrapper.lock)
         {
            delegate = cs;
         }

         return delegate;
      }
      HornetQJMSServerLogger.LOGGER.recoveryConnectFailed(Arrays.toString(xaRecoveryConfigs));
      throw new HornetQNotConnectedException();
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "HornetQXAResourceWrapper [serverLocator=" + serverLocator +
         ", csf=" +
         csf +
         ", delegate=" +
         delegate +
         ", xaRecoveryConfigs=" +
         Arrays.toString(xaRecoveryConfigs) +
         ", instance=" +
         System.identityHashCode(this) +
         "]";
   }

   /**
    * Close the connection
    */
   public void close()
   {
      ServerLocator oldServerLocator = null;
      ClientSessionFactory oldCSF = null;
      ClientSession oldDelegate = null;
      synchronized (HornetQXAResourceWrapper.lock)
      {
         oldCSF = csf;
         csf = null;
         oldDelegate = delegate;
         delegate = null;
         oldServerLocator = serverLocator;
         serverLocator = null;
      }

      if (oldDelegate != null)
      {
         try
         {
            oldDelegate.close();
         }
         catch (Throwable ignorable)
         {
            HornetQJMSServerLogger.LOGGER.debug(ignorable.getMessage(), ignorable);
         }
      }

      if (oldCSF != null)
      {
         try
         {
            oldCSF.close();
         }
         catch (Throwable ignorable)
         {
            HornetQJMSServerLogger.LOGGER.debug(ignorable.getMessage(), ignorable);
         }
      }

      if (oldServerLocator != null)
      {
         try
         {
            oldServerLocator.close();
         }
         catch (Throwable ignorable)
         {
            HornetQJMSServerLogger.LOGGER.debug(ignorable.getMessage(), ignorable);
         }
      }
   }

   /**
    * Check whether an XAException is fatal. If it is an RM problem
    * we close the connection so the next call will reconnect.
    *
    * @param e the xa exception
    * @return never
    * @throws XAException always
    */
   protected XAException check(final XAException e) throws XAException
   {
      HornetQJMSServerLogger.LOGGER.xaRecoveryError(e);


      // If any exception happened, we close the connection so we may start fresh
      close();
      throw e;
   }

   @Override
   protected void finalize() throws Throwable
   {
      close();
   }
}
