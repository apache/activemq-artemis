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
package org.hornetq.tests.integration.ra;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 6, 2010
 */
public abstract class HornetQRATestBase extends JMSTestBase
{
   protected ServerLocator locator;

   protected static final String MDBQUEUE = "mdbQueue";
   protected static final String DLQ = "dlqQueue";
   protected static final String MDBQUEUEPREFIXED = "jms.queue.mdbQueue";
   protected static final SimpleString MDBQUEUEPREFIXEDSIMPLE = new SimpleString("jms.queue.mdbQueue");

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = createInVMNonHALocator();
      createQueue(true, MDBQUEUE);
      createQueue(DLQ);
      setupDLQ(1);
   }

   protected void setupDLQ(int maxDeliveries)
   {
      AddressSettings settings = new AddressSettings();
      settings.setDeadLetterAddress(SimpleString.toSimpleString("jms.queue." + DLQ));
      settings.setMaxDeliveryAttempts(maxDeliveries);
      server.getAddressSettingsRepository().addMatch("#", settings);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      locator.close();

      super.tearDown();
   }

   protected HornetQActivation lookupActivation(HornetQResourceAdapter qResourceAdapter)
   {
      Map<ActivationSpec, HornetQActivation> activations = qResourceAdapter.getActivations();
      assertEquals(1, activations.size());

      return activations.values().iterator().next();
   }

   protected HornetQResourceAdapter newResourceAdapter()
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      // We don't have a TM on these tests.. This would cause the lookup to take at least 10 seconds if we didn't set to ""
      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.setTransactionManagerLocatorMethod("");
      qResourceAdapter.setConnectorClassName(UnitTestCase.INVM_CONNECTOR_FACTORY);
      return qResourceAdapter;
   }


   protected class DummyMessageEndpointFactory implements MessageEndpointFactory
   {
      private DummyMessageEndpoint endpoint;

      private final boolean isDeliveryTransacted;

      public DummyMessageEndpointFactory(DummyMessageEndpoint endpoint, boolean deliveryTransacted)
      {
         this.endpoint = endpoint;
         isDeliveryTransacted = deliveryTransacted;
      }

      public MessageEndpoint createEndpoint(XAResource xaResource) throws UnavailableException
      {
         if (xaResource != null)
         {
            endpoint.setXAResource(xaResource);
         }
         return endpoint;
      }

      public boolean isDeliveryTransacted(Method method) throws NoSuchMethodException
      {
         return isDeliveryTransacted;
      }
   }

   protected class DummyMessageEndpoint implements MessageEndpoint, MessageListener
   {
      public CountDownLatch latch;

      public HornetQMessage lastMessage;

      public boolean released = false;

      public XAResource xaResource;

      volatile boolean inAfterDelivery = false;

      public DummyMessageEndpoint(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException
      {

      }

      public void afterDelivery() throws ResourceException
      {
         if (latch != null)
         {
            latch.countDown();
         }
      }

      public void release()
      {
         released = true;
      }

      public void onMessage(Message message)
      {
         lastMessage = (HornetQMessage) message;
         System.err.println(message);
      }

      public void reset(CountDownLatch latch)
      {
         this.latch = latch;
         lastMessage = null;
      }

      public void setXAResource(XAResource xaResource)
      {
         this.xaResource = xaResource;
      }
   }

   public class MyBootstrapContext implements BootstrapContext
   {
      WorkManager workManager = new DummyWorkManager();

      public Timer createTimer() throws UnavailableException
      {
         return null;
      }

      public WorkManager getWorkManager()
      {
         return workManager;
      }

      public XATerminator getXATerminator()
      {
         return null;
      }

      class DummyWorkManager implements WorkManager
      {
         public void doWork(Work work) throws WorkException
         {
         }

         public void doWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
         }

         public long startWork(Work work) throws WorkException
         {
            return 0;
         }

         public long startWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
            return 0;
         }

         public void scheduleWork(Work work) throws WorkException
         {
            work.run();
         }

         public void scheduleWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
         }
      }
   }
}
