/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class SyncSendTest extends ActiveMQTestBase {

   private static long totalRecordTime = -1;
   private static final int RECORDS = 300;
   private static final int MEASURE_RECORDS = 100;
   private static final int WRAMP_UP = 100;

   @Parameters(name = "storage={0}, protocol={1}")
   public static Collection getParameters() {
      Object[] storages = new Object[]{"libaio", "nio", "null"};
      Object[] protocols = new Object[]{"core", "openwire", "amqp"};

      ArrayList<Object[]> objects = new ArrayList<>();
      for (Object s : storages) {
         if (s.equals("libaio") && !LibaioContext.isLoaded()) {
            continue;
         }
         for (Object p : protocols) {
            objects.add(new Object[]{s, p});
         }
      }

      return objects;
   }

   private final String storage;
   private final String protocol;

   public SyncSendTest(String storage, String protocol) {
      this.storage = storage;
      this.protocol = protocol;
   }

   @Override
   protected ConfigurationImpl createBasicConfig(final int serverID) {
      ConfigurationImpl config = super.createBasicConfig(serverID);
      config.setJournalDatasync(true).setJournalSyncNonTransactional(true).setJournalSyncTransactional(true);
      return config;
   }

   ActiveMQServer server;

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();

      if (storage.equals("null")) {
         server = createServer(false, true);
      } else {
         server = createServer(true, true);
      }

      if (storage.equals("libaio")) {
         server.getConfiguration().setJournalType(JournalType.ASYNCIO);
      } else {
         server.getConfiguration().setJournalType(JournalType.NIO);
      }

      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setAutoCreateAddresses(false).setAutoCreateQueues(false));

      server.getConfiguration().setJournalSyncTransactional(true).setJournalSyncNonTransactional(true).setJournalDatasync(true);
      server.start();
   }

   private long getTimePerSync() throws Exception {

      if (storage.equals("null")) {
         return 0;
      }
      if (totalRecordTime < 0) {
         File measureFile = File.createTempFile("junit", null, temporaryFolder);

         System.out.println("File::" + measureFile);

         RandomAccessFile rfile = new RandomAccessFile(measureFile, "rw");
         FileChannel channel = rfile.getChannel();

         channel.position(0);

         ByteBuffer buffer = ByteBuffer.allocate(10);
         buffer.put(new byte[10]);
         buffer.position(0);

         assertEquals(10, channel.write(buffer));
         channel.force(true);

         long time = System.nanoTime();

         for (int i = 0; i < MEASURE_RECORDS + WRAMP_UP; i++) {
            if (i == WRAMP_UP) {
               time = System.nanoTime();
            }
            channel.position(0);
            buffer.position(0);
            buffer.putInt(i);
            buffer.position(0);
            assertEquals(10, channel.write(buffer));
            channel.force(false);
         }

         long timeEnd = System.nanoTime();

         totalRecordTime = ((timeEnd - time) / MEASURE_RECORDS) * RECORDS;

         System.out.println("total time = " + totalRecordTime);

      }
      return totalRecordTime;

   }

   @TestTemplate
   public void testSendConsumeAudoACK() throws Exception {

      long recordTime = getTimePerSync();

      server.createQueue(QueueConfiguration.of("queue").setRoutingType(RoutingType.ANYCAST).setAutoCreateAddress(true));

      ConnectionFactory factory = newCF();

      Connection connection = factory.createConnection();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue;
         queue = session.createQueue("queue");
         MessageProducer producer = session.createProducer(queue);

         long start = System.nanoTime();

         for (int i = 0; i < (RECORDS + WRAMP_UP); i++) {
            if (i == WRAMP_UP) {
               start = System.nanoTime(); // wramp up
            }
            producer.send(session.createMessage());
         }

         long end = System.nanoTime();

         System.out.println("end - start = " + (end - start) + " milliseconds = " + TimeUnit.NANOSECONDS.toMillis(end - start));
         System.out.println("RECORD TIME = " + recordTime + " milliseconds = " + TimeUnit.NANOSECONDS.toMillis(recordTime));

         if ((end - start) < recordTime * 0.7) {
            fail("Messages are being sent too fast! Faster than the disk would be able to sync!");
         }

         connection.start();
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < (RECORDS + WRAMP_UP); i++) {
            if (i == WRAMP_UP) {
               start = System.nanoTime(); // wramp up
            }
            Message msg = consumer.receive(5000);
            assertNotNull(msg);
         }

         end = System.nanoTime();

         System.out.println("end - start = " + (end - start) + " milliseconds = " + TimeUnit.NANOSECONDS.toMillis(end - start));
         System.out.println("RECORD TIME = " + recordTime + " milliseconds = " + TimeUnit.NANOSECONDS.toMillis(recordTime));

         // There's no way to sync on ack for AMQP
         if (!protocol.equals("amqp") && (end - start) < recordTime * 0.7) {
            fail("Messages are being acked too fast! Faster than the disk would be able to sync!");
         }

         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(SimpleString.of("queue"));
         Wait.assertEquals(0, serverQueue::getMessageCount);
         Wait.assertEquals(0, serverQueue::getDeliveringCount);
      } finally {
         connection.close();
      }
   }

   // this will set ack as synchronous, to make sure we make proper measures against the sync on disk
   private ConnectionFactory newCF() {
      if (protocol.equals("core")) {
         ConnectionFactory factory = new ActiveMQConnectionFactory();
         ((ActiveMQConnectionFactory) factory).setBlockOnAcknowledge(true);
         return factory;
      } else if (protocol.equals("amqp")) {
         final JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:61616");
         factory.setForceAsyncAcks(true);
         return factory;
      } else {
         org.apache.activemq.ActiveMQConnectionFactory cf = new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616?wireFormat.cacheEnabled=true");
         cf.setSendAcksAsync(false);
         return cf;
      }

   }

}
