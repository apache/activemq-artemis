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
package org.apache.activemq.artemis.tests.unit.jms;

import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

import javax.jms.JMSRuntimeException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.junit.Assert;

import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.tests.util.RandomUtil;

public class ActiveMQDestinationTest extends ActiveMQTestBase {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testEquals() throws Exception {
      String destinationName = RandomUtil.randomString();
      String address = ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      ActiveMQDestination destination = (ActiveMQDestination) ActiveMQDestination.fromAddress(address);
      ActiveMQDestination sameDestination = (ActiveMQDestination) ActiveMQDestination.fromAddress(address);
      ActiveMQDestination differentDestination = (ActiveMQDestination) ActiveMQDestination.fromAddress(address + RandomUtil.randomString());

      Assert.assertFalse(destination.equals(null));
      Assert.assertTrue(destination.equals(destination));
      Assert.assertTrue(destination.equals(sameDestination));
      Assert.assertFalse(destination.equals(differentDestination));
   }

   @Test
   public void testFromAddressWithQueueAddressPrefix() throws Exception {
      String destinationName = RandomUtil.randomString();
      String address = ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      ActiveMQDestination destination = (ActiveMQDestination) ActiveMQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof Queue);
      Assert.assertEquals(destinationName, ((Queue) destination).getQueueName());
   }

   @Test
   public void testFromAddressWithTopicAddressPrefix() throws Exception {
      String destinationName = RandomUtil.randomString();
      String address = ActiveMQDestination.JMS_TOPIC_ADDRESS_PREFIX + destinationName;
      ActiveMQDestination destination = (ActiveMQDestination) ActiveMQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof Topic);
      Assert.assertEquals(destinationName, ((Topic) destination).getTopicName());
   }

   @Test
   public void testFromAddressWithInvalidPrefix() throws Exception {
      String invalidPrefix = "junk";
      String destinationName = RandomUtil.randomString();
      String address = invalidPrefix + destinationName;
      try {
         ActiveMQDestination.fromAddress(address);
         Assert.fail("IllegalArgumentException");
      }
      catch (JMSRuntimeException e) {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
