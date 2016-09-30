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
package org.apache.activemq.perf;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.leveldb.LevelDBStoreFactory;

/**
 *
 */
public class SimpleDurableTopicTest extends SimpleTopicTest {

   protected long initialConsumerDelay = 0;

   @Override
   protected void setUp() throws Exception {
      numberOfDestinations = 1;
      numberOfConsumers = 1;
      numberofProducers = Integer.parseInt(System.getProperty("SimpleDurableTopicTest.numberofProducers", "20"), 20);
      sampleCount = Integer.parseInt(System.getProperty("SimpleDurableTopicTest.sampleCount", "1000"), 10);
      playloadSize = 1024;
      super.setUp();
   }

   @Override
   protected void configureBroker(BrokerService answer, String uri) throws Exception {
      LevelDBStoreFactory persistenceFactory = new LevelDBStoreFactory();
      answer.setPersistenceFactory(persistenceFactory);
      //answer.setDeleteAllMessagesOnStartup(true);
      answer.addConnector(uri);
      answer.setUseShutdownHook(false);
   }

   @Override
   protected PerfProducer createProducer(ConnectionFactory fac,
                                         Destination dest,
                                         int number,
                                         byte payload[]) throws JMSException {
      PerfProducer pp = new PerfProducer(fac, dest, payload);
      pp.setDeliveryMode(DeliveryMode.PERSISTENT);
      return pp;
   }

   @Override
   protected PerfConsumer createConsumer(ConnectionFactory fac, Destination dest, int number) throws JMSException {
      PerfConsumer result = new PerfConsumer(fac, dest, "subs:" + number);
      result.setInitialDelay(this.initialConsumerDelay);
      return result;
   }

   @Override
   protected ActiveMQConnectionFactory createConnectionFactory(String uri) throws Exception {
      ActiveMQConnectionFactory result = super.createConnectionFactory(uri);
      //result.setSendAcksAsync(false);
      return result;
   }

}
