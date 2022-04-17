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
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.Deflater;

import org.junit.Test;

public class JMSLargeMessageTest extends JMSClientTestSupport {

   // https://issues.apache.org/jira/projects/ARTEMIS/issues/ARTEMIS-3751
   @Test
   public void testSendLargeMessageWithCompressionWhenCompressedSizeIsLowerThanLargeMessageSize() throws Exception {
      Connection connection = createCoreConnection();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination queue = session.createQueue("testQueue");

      MessageProducer producer = session.createProducer(queue);
      MessageConsumer consumer = session.createConsumer(queue);

      connection.start();

      // What we want is to get a message witch size is larger than client's minLargeMessageSize (200 kibs here) while compressed the size must be lower than that
      // and at same time it must be greater than server minLargeMessageSize (which is 100 kibs by default)
      byte[] data = new byte[1024 * 300];

      // We don't want the data to be too random, 42 seems to be a good random seed
      new DeflateGenerator(new Random(42), 2.0, 2.0).generate(data, 2);

      assertCompressionSize(data, 100 * 1024, 200 * 1024);

      BytesMessage outMessage = session.createBytesMessage();
      outMessage.writeBytes(data);

      producer.send(outMessage);

      Message inMessage = consumer.receive(1000);

      assertEqualsByteArrays(data, inMessage.getBody(byte[].class));
   }

   @Override
   protected String getJmsConnectionURIOptions() {
      return "minLargeMessageSize=" + (200 * 1024) + "&compressLargeMessage=true";
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   private void assertCompressionSize(byte[] data, int min, int max) {
      byte[] output = new byte[data.length];

      Deflater deflater = new Deflater();
      deflater.setInput(data);
      deflater.finish();
      int compressed = deflater.deflate(output);
      deflater.end();

      assert compressed > min && compressed < max;
   }

   /**
    * Generate compressible data.
    * <p>
    * Based on "SDGen: Mimicking Datasets for Content Generation in Storage
    * Benchmarks" by Raúl Gracia-Tinedo et al. (https://www.usenix.org/node/188461)
    * and https://github.com/jibsen/lzdatagen
    */
   public static class DeflateGenerator {

      private static final int MIN_LENGTH = 3;
      private static final int MAX_LENGTH = 258;
      private static final int NUM_LENGTH = MAX_LENGTH - MIN_LENGTH;

      private static final int LENGTH_PER_CHUNK = 512;

      private final Random rnd;

      private final double dataExp;
      private final double lengthExp;

      public DeflateGenerator(Random rnd, double dataExp, double lengthExp) {
         this.rnd = rnd;
         this.dataExp = dataExp;
         this.lengthExp = lengthExp;
      }

      private void nextBytes(byte[] buffer, int size) {
         for (int i = 0; i < size; i++) {
            buffer[i] = ((byte) ((double) 256 * Math.pow(rnd.nextDouble(), dataExp)));
         }
      }

      private byte[] nextBytes(int size) {
         byte[] buffer = new byte[size];
         nextBytes(buffer, size);
         return buffer;
      }

      private void nextLengthFrequencies(int[] frequencies) {
         Arrays.fill(frequencies, 0);

         for (int i = 0; i < LENGTH_PER_CHUNK; i++) {
            int length = (int) ((double) frequencies.length * Math.pow(rnd.nextDouble(), lengthExp));

            frequencies[length]++;
         }
      }

      public void generate(byte[] result, double ratio) {
         ByteBuffer generated = generate(result.length, ratio);
         generated.get(result);
      }

      public ByteBuffer generate(int size, double ratio) {
         ByteBuffer result = ByteBuffer.allocate(size);

         byte[] buffer = new byte[MAX_LENGTH];
         int[] frequencies = new int[NUM_LENGTH];

         int length = 0;
         int i = 0;
         boolean repeat = false;

         while (i < size) {
            while (frequencies[length] == 0) {
               if (length == 0) {
                  nextBytes(buffer, MAX_LENGTH);
                  nextLengthFrequencies(frequencies);

                  length = NUM_LENGTH;
               }

               length--;
            }

            int len = length + MIN_LENGTH;
            frequencies[length]--;

            if (len > size - i) {
               len = size - i;
            }

            if (rnd.nextDouble() < 1.0 / ratio) {
               result.put(nextBytes(len));
               repeat = false;
            } else {
               if (repeat) {
                  result.put(nextBytes(1));
                  i++;
               }

               result.put(buffer, 0, len);

               repeat = true;
            }

            i += len;
         }

         return result.flip();
      }
   }

}
