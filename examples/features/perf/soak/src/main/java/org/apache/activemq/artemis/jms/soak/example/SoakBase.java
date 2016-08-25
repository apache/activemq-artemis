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
package org.apache.activemq.artemis.jms.soak.example;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

public class SoakBase {

   private static final Logger log = Logger.getLogger(SoakBase.class.getName());

   private static final String DEFAULT_SOAK_PROPERTIES_FILE_NAME = "soak.properties";

   public static final int TO_MILLIS = 60 * 1000; // from minute to milliseconds

   public static byte[] randomByteArray(final int length) {
      byte[] bytes = new byte[length];

      Random random = new Random();

      for (int i = 0; i < length; i++) {
         bytes[i] = Integer.valueOf(random.nextInt()).byteValue();
      }

      return bytes;
   }

   protected static String getPerfFileName() {
      String fileName = System.getProperty("soak.props");
      if (fileName == null) {
         fileName = SoakBase.DEFAULT_SOAK_PROPERTIES_FILE_NAME;
      }
      return fileName;
   }

   protected static SoakParams getParams(final String fileName) throws Exception {
      Properties props = null;

      try (InputStream is = new FileInputStream(fileName)) {
         props = new Properties();

         props.load(is);
      }

      int durationInMinutes = Integer.valueOf(props.getProperty("duration-in-minutes"));
      int noOfWarmupMessages = Integer.valueOf(props.getProperty("num-warmup-messages"));
      int messageSize = Integer.valueOf(props.getProperty("message-size"));
      boolean durable = Boolean.valueOf(props.getProperty("durable"));
      boolean transacted = Boolean.valueOf(props.getProperty("transacted"));
      int batchSize = Integer.valueOf(props.getProperty("batch-size"));
      boolean drainQueue = Boolean.valueOf(props.getProperty("drain-queue"));
      String destinationLookup = props.getProperty("destination-lookup");
      String connectionFactoryLookup = props.getProperty("connection-factory-lookup");
      int throttleRate = Integer.valueOf(props.getProperty("throttle-rate"));
      boolean dupsOK = Boolean.valueOf(props.getProperty("dups-ok-acknowledge"));
      boolean disableMessageID = Boolean.valueOf(props.getProperty("disable-message-id"));
      boolean disableTimestamp = Boolean.valueOf(props.getProperty("disable-message-timestamp"));

      SoakBase.log.info("duration-in-minutes: " + durationInMinutes);
      SoakBase.log.info("num-warmup-messages: " + noOfWarmupMessages);
      SoakBase.log.info("message-size: " + messageSize);
      SoakBase.log.info("durable: " + durable);
      SoakBase.log.info("transacted: " + transacted);
      SoakBase.log.info("batch-size: " + batchSize);
      SoakBase.log.info("drain-queue: " + drainQueue);
      SoakBase.log.info("throttle-rate: " + throttleRate);
      SoakBase.log.info("connection-factory-lookup: " + connectionFactoryLookup);
      SoakBase.log.info("destination-lookup: " + destinationLookup);
      SoakBase.log.info("disable-message-id: " + disableMessageID);
      SoakBase.log.info("disable-message-timestamp: " + disableTimestamp);
      SoakBase.log.info("dups-ok-acknowledge: " + dupsOK);

      SoakParams soakParams = new SoakParams();
      soakParams.setDurationInMinutes(durationInMinutes);
      soakParams.setNoOfWarmupMessages(noOfWarmupMessages);
      soakParams.setMessageSize(messageSize);
      soakParams.setDurable(durable);
      soakParams.setSessionTransacted(transacted);
      soakParams.setBatchSize(batchSize);
      soakParams.setDrainQueue(drainQueue);
      soakParams.setConnectionFactoryLookup(connectionFactoryLookup);
      soakParams.setDestinationLookup(destinationLookup);
      soakParams.setThrottleRate(throttleRate);
      soakParams.setDisableMessageID(disableMessageID);
      soakParams.setDisableTimestamp(disableTimestamp);
      soakParams.setDupsOK(dupsOK);

      return soakParams;
   }
}
