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
package org.apache.activemq.tests.unit.jms;

import org.junit.Test;

import javax.jms.JMSRuntimeException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.junit.Assert;

import org.apache.activemq.jms.client.HornetQDestination;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class HornetQDestinationTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testEquals() throws Exception
   {
      String destinationName = RandomUtil.randomString();
      String address = HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      HornetQDestination sameDestination = (HornetQDestination) HornetQDestination.fromAddress(address);
      HornetQDestination differentDestination = (HornetQDestination) HornetQDestination.fromAddress(address + RandomUtil.randomString());

      Assert.assertFalse(destination.equals(null));
      Assert.assertTrue(destination.equals(destination));
      Assert.assertTrue(destination.equals(sameDestination));
      Assert.assertFalse(destination.equals(differentDestination));
   }

   @Test
   public void testFromAddressWithQueueAddressPrefix() throws Exception
   {
      String destinationName = RandomUtil.randomString();
      String address = HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof Queue);
      Assert.assertEquals(destinationName, ((Queue)destination).getQueueName());
   }

   @Test
   public void testFromAddressWithTopicAddressPrefix() throws Exception
   {
      String destinationName = RandomUtil.randomString();
      String address = HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof Topic);
      Assert.assertEquals(destinationName, ((Topic)destination).getTopicName());
   }

   @Test
   public void testFromAddressWithInvalidPrefix() throws Exception
   {
      String invalidPrefix = "junk";
      String destinationName = RandomUtil.randomString();
      String address = invalidPrefix + destinationName;
      try
      {
         HornetQDestination.fromAddress(address);
         Assert.fail("IllegalArgumentException");
      }
      catch (JMSRuntimeException e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
