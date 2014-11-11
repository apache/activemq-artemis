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
package org.apache.activemq6.tests.unit.core.postoffice.impl;

import org.apache.activemq6.core.server.impl.RefsOperation;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.filter.Filter;
import org.apache.activemq6.core.postoffice.Binding;
import org.apache.activemq6.core.postoffice.BindingType;
import org.apache.activemq6.core.postoffice.Bindings;
import org.apache.activemq6.core.postoffice.impl.BindingsImpl;
import org.apache.activemq6.core.server.Bindable;
import org.apache.activemq6.core.server.Queue;
import org.apache.activemq6.core.server.RoutingContext;
import org.apache.activemq6.core.server.ServerMessage;
import org.apache.activemq6.core.server.impl.RoutingContextImpl;
import org.apache.activemq6.core.server.impl.ServerMessageImpl;
import org.apache.activemq6.core.transaction.Transaction;
import org.apache.activemq6.core.transaction.TransactionOperation;
import org.apache.activemq6.tests.util.UnitTestCase;

/**
 * A BindingImplTest
 *
 * @author clebert
 *
 * Created Mar 12, 2009 9:14:46 PM
 *
 *
 */
public class BindingsImplTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testRemoveWhileRouting() throws Exception
   {
      // It would require many iterations before getting a failure
      for (int i = 0; i < 500; i++)
      {
         internalTest(true);
      }
   }

   @Test
   public void testRemoveWhileRedistributing() throws Exception
   {
      // It would require many iterations before getting a failure
      for (int i = 0; i < 500; i++)
      {
         internalTest(false);
      }
   }

   private void internalTest(final boolean route) throws Exception
   {
      final FakeBinding fake = new FakeBinding(new SimpleString("a"));

      final Bindings bind = new BindingsImpl(null, null, null);
      bind.addBinding(fake);
      bind.addBinding(new FakeBinding(new SimpleString("a")));
      bind.addBinding(new FakeBinding(new SimpleString("a")));

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               bind.removeBinding(fake);
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      Queue queue = new FakeQueue(new SimpleString("a"));
      t.start();

      for (int i = 0; i < 100; i++)
      {
         if (route)
         {
            bind.route(new ServerMessageImpl(i, 100), new RoutingContextImpl(new FakeTransaction()));
         }
         else
         {
            bind.redistribute(new ServerMessageImpl(i, 100), queue, new RoutingContextImpl(new FakeTransaction()));
         }
      }
   }

   private final class FakeTransaction implements Transaction
   {

      public void addOperation(final TransactionOperation sync)
      {

      }

      public boolean hasTimedOut(long currentTime, int defaultTimeout)
      {
         return false;
      }

      public void commit() throws Exception
      {

      }

      public void commit(final boolean onePhase) throws Exception
      {

      }

      public long getCreateTime()
      {

         return 0;
      }

      public long getID()
      {

         return 0;
      }

      public Object getProperty(final int index)
      {

         return null;
      }

      @Override
      public boolean isContainsPersistent()
      {
         return false;
      }

      public State getState()
      {

         return null;
      }

      public Xid getXid()
      {
         return null;
      }

      public void markAsRollbackOnly(final HornetQException exception)
      {

      }

      public void prepare() throws Exception
      {

      }

      public void putProperty(final int index, final Object property)
      {

      }

      public void removeOperation(final TransactionOperation sync)
      {

      }

      public void resume()
      {

      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.transaction.Transaction#rollback()
       */
      public void rollback() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.transaction.Transaction#setState(org.hornetq.core.transaction.Transaction.State)
       */
      public void setState(final State state)
      {

      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.transaction.Transaction#suspend()
       */
      public void suspend()
      {

      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.transaction.Transaction#getDistinctQueues()
       */
      public Set<Queue> getDistinctQueues()
      {
         return Collections.emptySet();
      }

      public void setContainsPersistent()
      {


      }

      public void setTimeout(int timeout)
      {

      }

      public List<TransactionOperation> getAllOperations()
      {
         return null;
      }

      public void setWaitBeforeCommit(boolean waitBeforeCommit)
      {
      }

      @Override
      public RefsOperation createRefsOperation(Queue queue)
      {
         // TODO Auto-generated method stub
         return null;
      }
   }

   private final class FakeFilter implements Filter
   {

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.filter.Filter#getFilterString()
       */
      public SimpleString getFilterString()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.filter.Filter#match(org.hornetq.core.server.ServerMessage)
       */
      public boolean match(final ServerMessage message)
      {
         return false;
      }

   }

   private final class FakeBinding implements Binding
   {

      public void close() throws Exception
      {


      }

      @Override
      public void unproposed(SimpleString groupID)
      {

      }

      final SimpleString name;

      FakeBinding(final SimpleString name)
      {
         this.name = name;
      }

      public SimpleString getAddress()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#getBindable()
       */
      public Bindable getBindable()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#getClusterName()
       */
      public SimpleString getClusterName()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#getDistance()
       */
      public int getDistance()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#getFilter()
       */
      public Filter getFilter()
      {
         return new FakeFilter();
      }

      public long getID()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#getRoutingName()
       */
      public SimpleString getRoutingName()
      {
         return name;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#getType()
       */
      public BindingType getType()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#getUniqueName()
       */
      public SimpleString getUniqueName()
      {
         return null;
      }

      public boolean isExclusive()
      {
         return false;
      }

      public boolean isHighAcceptPriority(final ServerMessage message)
      {
         return false;
      }

      public void route(final ServerMessage message, final RoutingContext context) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.postoffice.Binding#toManagementString()
       */
      @Override
      public String toManagementString()
      {
         return null;
      }

      @Override
      public boolean isConnected()
      {
         return true;
      }

      @Override
      public void routeWithAck(ServerMessage message, RoutingContext context)
      {

      }


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
