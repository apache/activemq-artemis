/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker;

import org.apache.activemq.util.ThreadTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class TopicSubscriptionTest extends QueueSubscriptionTest {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      durable = true;
      topic = true;
   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();
      ThreadTracker.result();
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testManyProducersManyConsumers() throws Exception {
      consumerCount = 40;
      producerCount = 20;
      messageCount = 100;
      messageSize = 1;
      prefetchCount = 10;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * producerCount * consumerCount);
      assertDestinationMemoryUsageGoesToZero();
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testOneProducerTwoConsumersLargeMessagesOnePrefetch() throws Exception {
      consumerCount = 2;
      producerCount = 1;
      messageCount = 10;
      messageSize = 1024 * 1024 * 1; // 1 MB
      prefetchCount = 1;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * consumerCount * producerCount);
      assertDestinationMemoryUsageGoesToZero();
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testOneProducerTwoConsumersSmallMessagesOnePrefetch() throws Exception {
      consumerCount = 2;
      producerCount = 1;
      prefetchCount = 1;
      messageSize = 1024;
      messageCount = 1000;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * consumerCount * producerCount);
      assertDestinationMemoryUsageGoesToZero();
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testOneProducerTwoConsumersSmallMessagesLargePrefetch() throws Exception {
      consumerCount = 2;
      producerCount = 1;
      messageCount = 1000;
      messageSize = 1024;
      prefetchCount = messageCount * 2;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * consumerCount * producerCount);
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testOneProducerTwoConsumersLargeMessagesLargePrefetch() throws Exception {
      consumerCount = 2;
      producerCount = 1;
      messageCount = 10;
      messageSize = 1024 * 1024 * 1; // 1 MB
      prefetchCount = messageCount * 2;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * consumerCount * producerCount);
      assertDestinationMemoryUsageGoesToZero();
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testOneProducerManyConsumersFewMessages() throws Exception {
      consumerCount = 50;
      producerCount = 1;
      messageCount = 10;
      messageSize = 1; // 1 byte
      prefetchCount = 10;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * consumerCount * producerCount);
      assertDestinationMemoryUsageGoesToZero();
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testOneProducerManyConsumersManyMessages() throws Exception {
      consumerCount = 50;
      producerCount = 1;
      messageCount = 100;
      messageSize = 1; // 1 byte
      prefetchCount = 10;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * consumerCount * producerCount);
      assertDestinationMemoryUsageGoesToZero();
   }

   @Override
   @Test(timeout = 60 * 1000)
   public void testManyProducersOneConsumer() throws Exception {
      consumerCount = 1;
      producerCount = 20;
      messageCount = 100;
      messageSize = 1; // 1 byte
      prefetchCount = 10;

      doMultipleClientsTest();

      assertTotalMessagesReceived(messageCount * producerCount * consumerCount);
      assertDestinationMemoryUsageGoesToZero();
   }

}
