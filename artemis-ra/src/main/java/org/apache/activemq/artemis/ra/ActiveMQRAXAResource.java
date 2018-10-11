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
package org.apache.activemq.artemis.ra;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.client.impl.ActiveMQXAResource;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;

/**
 * ActiveMQXAResource.
 */
public class ActiveMQRAXAResource implements ActiveMQXAResource {

   /**
    * The managed connection
    */
   private final ActiveMQRAManagedConnection managedConnection;

   /**
    * The resource
    */
   private final XAResource xaResource;

   /**
    * Create a new ActiveMQXAResource.
    *
    * @param managedConnection the managed connection
    * @param xaResource        the xa resource
    */
   public ActiveMQRAXAResource(final ActiveMQRAManagedConnection managedConnection, final XAResource xaResource) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + managedConnection + ", " + xaResource + ")");
      }

      this.managedConnection = managedConnection;
      this.xaResource = xaResource;
   }

   /**
    * Start
    *
    * @param xid   A global transaction identifier
    * @param flags One of TMNOFLAGS, TMJOIN, or TMRESUME
    * @throws XAException An error has occurred
    */
   @Override
   public void start(final Xid xid, final int flags) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("start(" + xid + ", " + flags + ")");
      }

      managedConnection.lock();

      ClientSessionInternal sessionInternal = (ClientSessionInternal) xaResource;
      try {
         try {
            //this resets any tx stuff, we assume here that the tm and jca layer are well behaved when it comes to this
            sessionInternal.resetIfNeeded();
         } catch (ActiveMQException e) {
            ActiveMQRALogger.LOGGER.problemResettingXASession(e);

            XAException xaException = new XAException(XAException.XAER_RMFAIL);
            xaException.initCause(e);
            throw xaException;
         }

         xaResource.start(xid, flags);
      } finally {
         managedConnection.setInManagedTx(true);
         managedConnection.unlock();
      }
   }

   /**
    * End
    *
    * @param xid   A global transaction identifier
    * @param flags One of TMSUCCESS, TMFAIL, or TMSUSPEND.
    * @throws XAException An error has occurred
    */
   @Override
   public void end(final Xid xid, final int flags) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("end(" + xid + ", " + flags + ")");
      }

      managedConnection.lock();
      try {
         xaResource.end(xid, flags);
      } finally {
         managedConnection.setInManagedTx(false);
         managedConnection.unlock();
      }
   }

   /**
    * Prepare
    *
    * @param xid A global transaction identifier
    * @return XA_RDONLY or XA_OK
    * @throws XAException An error has occurred
    */
   @Override
   public int prepare(final Xid xid) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("prepare(" + xid + ")");
      }

      return xaResource.prepare(xid);
   }

   /**
    * Commit
    *
    * @param xid      A global transaction identifier
    * @param onePhase If true, the resource manager should use a one-phase commit protocol to commit the work done on behalf of xid.
    * @throws XAException An error has occurred
    */
   @Override
   public void commit(final Xid xid, final boolean onePhase) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("commit(" + xid + ", " + onePhase + ")");
      }

      xaResource.commit(xid, onePhase);
   }

   /**
    * Rollback
    *
    * @param xid A global transaction identifier
    * @throws XAException An error has occurred
    */
   @Override
   public void rollback(final Xid xid) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("rollback(" + xid + ")");
      }

      xaResource.rollback(xid);
   }

   /**
    * Forget
    *
    * @param xid A global transaction identifier
    * @throws XAException An error has occurred
    */
   @Override
   public void forget(final Xid xid) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("forget(" + xid + ")");
      }

      managedConnection.lock();
      try {
         xaResource.forget(xid);
      } finally {
         managedConnection.setInManagedTx(true);
         managedConnection.setInManagedTx(false);
         managedConnection.unlock();
      }
   }

   /**
    * IsSameRM
    *
    * @param xaRes An XAResource object whose resource manager instance is to be compared with the resource manager instance of the target object.
    * @return True if its the same RM instance; otherwise false.
    * @throws XAException An error has occurred
    */
   @Override
   public boolean isSameRM(final XAResource xaRes) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("isSameRM(" + xaRes + ")");
      }

      return xaResource.isSameRM(xaRes);
   }

   /**
    * Recover
    *
    * @param flag One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS
    * @return Zero or more XIDs
    * @throws XAException An error has occurred
    */
   @Override
   public Xid[] recover(final int flag) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("recover(" + flag + ")");
      }

      return xaResource.recover(flag);
   }

   /**
    * Get the transaction timeout in seconds
    *
    * @return The transaction timeout
    * @throws XAException An error has occurred
    */
   @Override
   public int getTransactionTimeout() throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getTransactionTimeout()");
      }

      return xaResource.getTransactionTimeout();
   }

   /**
    * Set the transaction timeout
    *
    * @param seconds The number of seconds
    * @return True if the transaction timeout value is set successfully; otherwise false.
    * @throws XAException An error has occurred
    */
   @Override
   public boolean setTransactionTimeout(final int seconds) throws XAException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setTransactionTimeout(" + seconds + ")");
      }

      return xaResource.setTransactionTimeout(seconds);
   }

   @Override
   public XAResource getResource() {
      return xaResource;
   }
}
