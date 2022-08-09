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

package org.apache.activemq.artemis.tests.soak.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.soak.TestParameters.testProperty;

/** It is recommended to set the following System properties before running this test:
 *
 * export TEST_HORIZONTAL_DESTINATIONS=500
 * export TEST_HORIZONTAL_MESSAGES=500
 * export TEST_HORIZONTAL_COMMIT_INTERVAL=100
 * export TEST_HORIZONTAL_SIZE=60000
 *
 * #You may choose to use zip files to save some time on producing if you want to run this test over and over when debugging
 * export TEST_HORIZONTAL_ZIP_LOCATION=a folder
 * */
@RunWith(Parameterized.class)
public class HorizontalPagingTest extends SoakTestBase {

   private static final String TEST_NAME = "HORIZONTAL";

   private final String protocol;
   private static final String ZIP_LOCATION = testProperty(TEST_NAME, "ZIP_LOCATION", null);
   private static final int SERVER_START_TIMEOUT = testProperty(TEST_NAME, "SERVER_START_TIMEOUT", 300_000);
   private static final int TIMEOUT_MINUTES = testProperty(TEST_NAME, "TIMEOUT_MINUTES", 120);
   private static final String PROTOCOL_LIST = testProperty(TEST_NAME, "PROTOCOL_LIST", "OPENWIRE,CORE,AMQP");
   private static final int PRINT_INTERVAL = testProperty(TEST_NAME, "PRINT_INTERVAL", 100);

   private final int DESTINATIONS;
   private final int MESSAGES;
   private final int COMMIT_INTERVAL;
   // if 0 will use AUTO_ACK
   private final int RECEIVE_COMMIT_INTERVAL;
   private final int MESSAGE_SIZE;
   private final int PARALLEL_SENDS;


   private static final Logger logger = Logger.getLogger(HorizontalPagingTest.class);

   public static final String SERVER_NAME_0 = "horizontalPaging";

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection<Object[]> parameters() {
      String[] protocols = PROTOCOL_LIST.split(",");

      ArrayList<Object[]> parameters = new ArrayList<>();
      for (String str : protocols) {
         logger.info("Adding " + str + " to the list for the test");
         parameters.add(new Object[]{str});
      }

      return parameters;
   }

   public HorizontalPagingTest(String protocol) {
      this.protocol = protocol;
      DESTINATIONS = testProperty(TEST_NAME, protocol + "_DESTINATIONS", 10);
      MESSAGES = testProperty(TEST_NAME, protocol + "_MESSAGES", 100);
      COMMIT_INTERVAL = testProperty(TEST_NAME, protocol + "_COMMIT_INTERVAL", 10);
      // if 0 will use AUTO_ACK
      RECEIVE_COMMIT_INTERVAL = testProperty(TEST_NAME, protocol + "_RECEIVE_COMMIT_INTERVAL", 1);
      MESSAGE_SIZE = testProperty(TEST_NAME, protocol + "_MESSAGE_SIZE", 60_000);
      PARALLEL_SENDS = testProperty(TEST_NAME, protocol + "_PARALLEL_SENDS", 2);
   }

   Process serverProcess;

   boolean unzipped = false;

   private String getZipName() {
      return "data-" + protocol + "-" + DESTINATIONS + "-" + MESSAGES + "-" + MESSAGE_SIZE + ".zip";
   }

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);

      boolean useZip = ZIP_LOCATION != null;
      String zipName = getZipName();
      File zipFile = useZip ? new File(ZIP_LOCATION + "/" + zipName) : null;

      if (ZIP_LOCATION  != null && zipFile.exists()) {
         unzipped = true;
         System.out.println("Invoking unzip");
         ProcessBuilder zipBuilder = new ProcessBuilder("unzip", zipFile.getAbsolutePath()).directory(new File(getServerLocation(SERVER_NAME_0)));

         Process process = zipBuilder.start();
         SpawnedVMSupport.startLogger("zip", process);
         System.out.println("Zip finished with " + process.waitFor());
      }

      serverProcess = startServer(SERVER_NAME_0, 0, SERVER_START_TIMEOUT);
   }


   @Test
   public void testHorizontal() throws Exception {
      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      AtomicInteger errors = new AtomicInteger(0);

      ExecutorService service = Executors.newFixedThreadPool(DESTINATIONS);
      runAfter(service::shutdownNow);

      if (!unzipped) {
         Connection connection = factory.createConnection();
         runAfter(connection::close);

         String text;
         {
            StringBuffer buffer = new StringBuffer();
            while (buffer.length() < MESSAGE_SIZE) {
               buffer.append("a big string...");
            }

            text = buffer.toString();
         }

         ReusableLatch latchDone = new ReusableLatch(0);


         for (int i = 0; i < DESTINATIONS; i++) {
            latchDone.countUp();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("queue_" + i);
            service.execute(() -> {
               try {
                  logger.info("*******************************************************************************************************************************\ndestination " + queue.getQueueName());
                  MessageProducer producer = session.createProducer(queue);
                  for (int m = 0; m < MESSAGES; m++) {
                     producer.send(session.createTextMessage(text));
                     if (m > 0 && m % COMMIT_INTERVAL == 0) {
                        logger.info("Sent " + m + " " + protocol + " messages on queue " + queue.getQueueName());
                        session.commit();
                     }
                  }

                  session.commit();
                  session.close();
               } catch (Throwable e) {
                  logger.warn(e.getMessage(), e);
                  errors.incrementAndGet();
               } finally {
                  latchDone.countDown();
               }
            });

            if ((i + 1) % PARALLEL_SENDS == 0) {
               latchDone.await();
            }
         }
         latchDone.await();

         connection.close();


         killServer(serverProcess);
      }


      if (ZIP_LOCATION != null && !unzipped) {
         String fileName = getZipName();
         logger.info("Zipping data folder for " + protocol + " as " + fileName);
         ProcessBuilder zipBuilder = new ProcessBuilder("zip", "-r", ZIP_LOCATION + "/" + getZipName(), "data").directory(new File(getServerLocation(SERVER_NAME_0)));
         Process process = zipBuilder.start();
         SpawnedVMSupport.startLogger("zip", process);
         System.out.println("Zip finished with " + process.waitFor());
      }

      serverProcess = startServer(SERVER_NAME_0, 0, SERVER_START_TIMEOUT);

      Connection connectionConsumer = factory.createConnection();

      runAfter(connectionConsumer::close);

      AtomicInteger completedFine = new AtomicInteger(0);

      for (int i = 0; i < DESTINATIONS; i++) {
         int destination = i;
         service.execute(() -> {
            try {
               Session sessionConsumer;

               if (RECEIVE_COMMIT_INTERVAL <= 0) {
                  sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
               } else {
                  sessionConsumer = connectionConsumer.createSession(true, Session.SESSION_TRANSACTED);
               }

               MessageConsumer messageConsumer = sessionConsumer.createConsumer(sessionConsumer.createQueue("queue_" + destination));
               for (int m = 0; m < MESSAGES; m++) {
                  TextMessage message = (TextMessage) messageConsumer.receive(50_000);
                  if (message == null) {
                     m--;
                     continue;
                  }

                  // The sending commit interval here will be used for printing
                  if (PRINT_INTERVAL > 0 && m % PRINT_INTERVAL == 0) {
                     logger.info("Destination " + destination + " received " + m + " " + protocol + " messages");
                  }

                  if (RECEIVE_COMMIT_INTERVAL > 0 && (m + 1) % RECEIVE_COMMIT_INTERVAL == 0) {
                     sessionConsumer.commit();
                  }
               }

               if (RECEIVE_COMMIT_INTERVAL > 0) {
                  sessionConsumer.commit();
               }

               completedFine.incrementAndGet();

            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            }
         });
      }

      connectionConsumer.start();

      service.shutdown();
      Assert.assertTrue("Test Timed Out", service.awaitTermination(TIMEOUT_MINUTES, TimeUnit.MINUTES));
      Assert.assertEquals(0, errors.get());
      Assert.assertEquals(DESTINATIONS, completedFine.get());

      connectionConsumer.close();
   }

}
