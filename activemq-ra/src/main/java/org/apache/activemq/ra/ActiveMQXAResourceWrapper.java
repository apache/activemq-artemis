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

package org.apache.activemq.ra;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.core.client.impl.ActiveMQXAResource;
import org.apache.activemq.utils.VersionLoader;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 *
 * Wraps XAResource with org.jboss.tm.XAResourceWrapper.  This adds extra meta-data to to the XAResource used by
 * Transaction Manager for recovery scenarios.
 */

public class ActiveMQXAResourceWrapper implements org.jboss.tm.XAResourceWrapper, org.jboss.jca.core.spi.transaction.xa.XAResourceWrapper, ActiveMQXAResource
{
   private final XAResource xaResource;

   // The EIS Name
   private final String productName;

   // The EIS Version
   private final String productVersion;

   // A composite of NodeID + JNDIName that allows adminstrator looking at an XAResource to determine it's origin.
   private final String jndiNameNodeId;

   /**
    * Creates a new XAResourceWrapper.  PRODUCT_NAME, productVersion and jndiName are useful for log output in the
    * Transaction Manager.  For ActiveMQ only the resourceManagerID is required to allow Transaction Manager to recover
    * from relevant recovery scenarios.
    *
    * @param xaResource
    * @param jndiName
    */
   public ActiveMQXAResourceWrapper(XAResource xaResource, String jndiName, String nodeId)
   {
      this.xaResource = xaResource;
      this.productName = ActiveMQResourceAdapter.PRODUCT_NAME;
      this.productVersion = VersionLoader.getVersion().getFullVersion();
      this.jndiNameNodeId = jndiName + " NodeId:" + nodeId;
   }

   @Override
   public XAResource getResource()
   {
      return xaResource;
   }

   @Override
   public String getProductName()
   {
      return productName;
   }

   @Override
   public String getProductVersion()
   {
      return productVersion;
   }

   @Override
   public String getJndiName()
   {
      return jndiNameNodeId;
   }

   @Override
   public void commit(Xid xid, boolean b) throws XAException
   {
      getResource().commit(xid, b);
   }

   @Override
   public void end(Xid xid, int i) throws XAException
   {
      getResource().end(xid, i);
   }

   @Override
   public void forget(Xid xid) throws XAException
   {
      getResource().forget(xid);
   }

   @Override
   public int getTransactionTimeout() throws XAException
   {
      return getResource().getTransactionTimeout();
   }

   @Override
   public boolean isSameRM(XAResource xaResource) throws XAException
   {
      return getResource().isSameRM(xaResource);
   }

   @Override
   public int prepare(Xid xid) throws XAException
   {
      return getResource().prepare(xid);
   }

   @Override
   public Xid[] recover(int i) throws XAException
   {
      return getResource().recover(i);
   }

   @Override
   public void rollback(Xid xid) throws XAException
   {
      getResource().rollback(xid);
   }

   @Override
   public boolean setTransactionTimeout(int i) throws XAException
   {
      return getResource().setTransactionTimeout(i);
   }

   @Override
   public void start(Xid xid, int i) throws XAException
   {
      getResource().start(xid, i);
   }

}
