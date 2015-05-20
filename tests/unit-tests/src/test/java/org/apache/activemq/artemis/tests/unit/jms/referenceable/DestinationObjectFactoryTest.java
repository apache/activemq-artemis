/**
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
package org.apache.activemq.artemis.tests.unit.jms.referenceable;

import org.apache.activemq.artemis.tests.util.ServiceTestBase;
import org.junit.Test;

import javax.naming.Reference;

import org.junit.Assert;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.referenceable.DestinationObjectFactory;
import org.apache.activemq.artemis.tests.util.RandomUtil;

public class DestinationObjectFactoryTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testReference() throws Exception
   {
      ActiveMQDestination queue = (ActiveMQDestination) ActiveMQJMSClient.createQueue(RandomUtil.randomString());
      Reference reference = queue.getReference();

      DestinationObjectFactory factory = new DestinationObjectFactory();
      Object object = factory.getObjectInstance(reference, null, null, null);
      Assert.assertNotNull(object);
      Assert.assertTrue(object instanceof ActiveMQDestination);
      Assert.assertEquals(queue, object);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
