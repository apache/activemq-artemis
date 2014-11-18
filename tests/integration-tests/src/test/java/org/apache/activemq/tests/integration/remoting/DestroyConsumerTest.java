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
package org.apache.activemq.tests.integration.remoting;

import org.junit.Test;

import org.apache.activemq.tests.util.ServiceTestBase;

public class DestroyConsumerTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testFoo()
   {
   }

   // public void testDestroyConsumer() throws Exception
   // {
   // ActiveMQServer server = createService(false, false, createDefaultConfig(), new HashMap<String, AddressSettings>());
   // server.start();
   //
   // SimpleString queue = new SimpleString("add1");
   //
   // ClientSessionFactory factory = createInVMFactory();
   //
   // ClientSession session = factory.createSession(false, false, false, false);
   //
   // session.createQueue(queue, queue, null, false, false);
   //
   // ClientConsumer consumer = session.createConsumer(queue);
   //
   // session.start();
   //
   // Binding binding = server.getServer().getPostOffice().getBindingsForAddress(queue).get(0);
   //
   // assertEquals(1, binding.getQueue().getConsumerCount());
   //
   // ClientSessionImpl impl = (ClientSessionImpl) session;
   //
   // // Simulating a CTRL-C what would close the Socket but not the ClientSession
   // impl.cleanUp();
   //
   //
   // assertEquals(0, binding.getQueue().getConsumerCount());
   //
   //
   //
   // }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
