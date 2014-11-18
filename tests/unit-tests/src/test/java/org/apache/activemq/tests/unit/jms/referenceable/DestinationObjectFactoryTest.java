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
package org.apache.activemq.tests.unit.jms.referenceable;

import org.junit.Test;

import javax.naming.Reference;

import org.junit.Assert;

import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.jms.client.ActiveMQDestination;
import org.apache.activemq.jms.referenceable.DestinationObjectFactory;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class DestinationObjectFactoryTest extends UnitTestCase
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
