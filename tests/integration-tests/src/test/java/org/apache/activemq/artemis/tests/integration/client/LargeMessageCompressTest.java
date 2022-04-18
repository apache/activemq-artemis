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
package org.apache.activemq.artemis.tests.integration.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;

import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.management.impl.QueueControlImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.openmbean.CompositeData;

/**
 * A LargeMessageCompressTest
 * <br>
 * Just extend the LargeMessageTest
 */
public class LargeMessageCompressTest extends LargeMessageTest {


   public LargeMessageCompressTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
      isCompressedTest = true;
   }

   @Override
   protected void validateLargeMessageComplete(ActiveMQServer server) throws Exception {
   }

   @Override
   protected boolean isNetty() {
      return false;
   }

   @Override
   protected ServerLocator createFactory(final boolean isNetty) throws Exception {
      return super.createFactory(isNetty).setCompressLargeMessage(true);
   }

   @Test
   public void testLargeMessageCompressionNotCompressedAndBrowsed() throws Exception {
      final int messageSize = (int) (3.5 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(new QueueConfiguration(ADDRESS).setAddress(ADDRESS).setDurable(false).setTemporary(true));

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessageStreaming(session, messageSize, true);

      clientFile.setType(Message.TEXT_TYPE);

      producer.send(clientFile);

      session.commit();

      session.close();

      QueueControlImpl queueControl = (QueueControlImpl) server.getManagementService().getResource("queue.SimpleAddress");

      CompositeData[] browse = queueControl.browse();

      Assert.assertNotNull(browse);

      Assert.assertEquals(browse.length, 1);

      Assert.assertEquals(browse[0].get("text"), "[compressed]");

      //clean up
      session = addClientSession(sf.createSession(false, false, false));

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      ClientMessage msg1 = consumer.receive(1000);
      Assert.assertNotNull(msg1);

      for (int i = 0; i < messageSize; i++) {
         byte b = msg1.getBodyBuffer().readByte();
         assertEquals("position = " + i, getSamplebyte(i), b);
      }

      msg1.acknowledge();
      session.commit();

      consumer.close();

      session.close();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testNoDirectByteBufLeaksOnLargeMessageCompression() throws Exception {
      Assume.assumeThat(PlatformDependent.usedDirectMemory(), not(equalTo(Long.valueOf(-1))));
      final int messageSize = (int) (3.5 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(new QueueConfiguration(ADDRESS).setAddress(ADDRESS).setDurable(false).setTemporary(true));

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessageStreaming(session, messageSize, true);

      producer.send(clientFile);

      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      final long usedDirectMemoryBeforeReceive = PlatformDependent.usedDirectMemory();
      ClientMessage msg1 = consumer.receive(1000);
      Assert.assertNotNull(msg1);
      final long usedDirectMemoryAfterReceive = PlatformDependent.usedDirectMemory();
      Assert.assertEquals("large message compression is leaking some Netty direct ByteBuff",
                          usedDirectMemoryBeforeReceive, usedDirectMemoryAfterReceive);
      msg1.acknowledge();
      session.commit();

      consumer.close();

      session.close();
   }

   @Test
   public void testLargeMessageCompression() throws Exception {
      final int messageSize = (int) (3.5 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(new QueueConfiguration(ADDRESS).setAddress(ADDRESS).setDurable(false).setTemporary(true));

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessageStreaming(session, messageSize, true);

      producer.send(clientFile);

      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      ClientMessage msg1 = consumer.receive(1000);
      Assert.assertNotNull(msg1);

      for (int i = 0; i < messageSize; i++) {
         byte b = msg1.getBodyBuffer().readByte();
         assertEquals("position = " + i, getSamplebyte(i), b);
      }

      msg1.acknowledge();
      session.commit();

      consumer.close();

      session.close();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testLargeMessageCompression2() throws Exception {
      final int messageSize = (int) (3.5 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(new QueueConfiguration(ADDRESS).setAddress(ADDRESS).setDurable(false).setTemporary(true));

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessageStreaming(session, messageSize, true);

      producer.send(clientFile);

      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      ClientMessage msg1 = consumer.receive(1000);
      Assert.assertNotNull(msg1);

      String testDir = getTestDir();
      File testFile = new File(testDir, "async_large_message");
      FileOutputStream output = new FileOutputStream(testFile);

      msg1.setOutputStream(output);

      msg1.waitOutputStreamCompletion(0);

      msg1.acknowledge();

      output.close();

      session.commit();

      consumer.close();

      session.close();

      //verify
      FileInputStream input = new FileInputStream(testFile);
      for (int i = 0; i < messageSize; i++) {
         byte b = (byte) input.read();
         assertEquals("position = " + i, getSamplebyte(i), b);
      }
      input.close();
      testFile.delete();
      validateNoFilesOnLargeDir();

   }

   @Test
   public void testLargeMessageCompression3() throws Exception {
      final int messageSize = (int) (3.5 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(new QueueConfiguration(ADDRESS).setAddress(ADDRESS).setDurable(false).setTemporary(true));

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessageStreaming(session, messageSize, true);

      producer.send(clientFile);

      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      ClientMessage msg1 = consumer.receive(1000);
      Assert.assertNotNull(msg1);

      String testDir = getTestDir();
      File testFile = new File(testDir, "async_large_message");
      FileOutputStream output = new FileOutputStream(testFile);

      msg1.saveToOutputStream(output);

      msg1.acknowledge();

      output.close();

      session.commit();

      consumer.close();

      session.close();

      //verify
      FileInputStream input = new FileInputStream(testFile);
      for (int i = 0; i < messageSize; i++) {
         byte b = (byte) input.read();
         assertEquals("position = " + i, getSamplebyte(i), b);
      }
      input.close();

      testFile.delete();
      validateNoFilesOnLargeDir();
   }

   // This test will send 1 Gig of spaces. There shouldn't be enough memory to uncompress the file in memory
   // but this will make sure we can work through compressed channels on saving it to stream
   @Test
   public void testHugeStreamingSpacesCompressed() throws Exception {
      final long messageSize = 1024L * 1024L;

      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      // big enough to hold the whole message compressed on a single message (about 1M on our tests)
      locator.setMinLargeMessageSize(100 * 1024 * 1024);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(new QueueConfiguration(ADDRESS));

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage clientMessage = session.createMessage(true);

      clientMessage.setBodyInputStream(new InputStream() {
         private long count;

         private boolean closed = false;

         @Override
         public void close() throws IOException {
            super.close();
            closed = true;
         }

         @Override
         public int read() throws IOException {
            if (closed) {
               throw new IOException("Stream was closed");
            }

            if (count++ < messageSize) {
               return ' ';
            } else {
               return -1;
            }
         }
      });

      producer.send(clientMessage);

      session.commit();

      // this is to make sure the message was sent as a regular message (not taking a file on server)
      validateNoFilesOnLargeDir();

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      ClientMessage msg1 = consumer.receive(1000);
      Assert.assertNotNull(msg1);

      final AtomicLong numberOfSpaces = new AtomicLong();

      msg1.saveToOutputStream(new OutputStream() {
         @Override
         public void write(int content) {
            if (content == ' ') {
               numberOfSpaces.incrementAndGet();
            }
         }
      });

      assertEquals(messageSize, numberOfSpaces.get());

      msg1.acknowledge();

      session.commit();

      session.close();
   }

   @Test
   public void testLargeMessageCompressionRestartAndCheckSize() throws Exception {
      final int messageSize = 1024 * 1024;

      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, false, false));

      session.createQueue(new QueueConfiguration(ADDRESS));

      ClientProducer producer = session.createProducer(ADDRESS);

      byte[] msgs = new byte[1024 * 1024];
      for (int i = 0; i < msgs.length; i++) {
         msgs[i] = RandomUtil.randomByte();
      }

      Message clientFile = createLargeClientMessage(session, msgs, true);

      producer.send(clientFile);

      session.commit();

      session.close();

      sf.close();

      locator.close();

      server.stop();

      server = createServer(true, isNetty());

      server.start();

      locator = createFactory(isNetty());

      sf = createSessionFactory(locator);

      session = sf.createSession();

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      ClientMessage msg1 = consumer.receive(1000);
      Assert.assertNotNull(msg1);

      assertEquals(messageSize, msg1.getBodySize());

      String testDir = getTestDir();
      File testFile = new File(testDir, "async_large_message");
      FileOutputStream output = new FileOutputStream(testFile);

      msg1.saveToOutputStream(output);

      msg1.acknowledge();

      session.commit();

      consumer.close();

      session.close();

      //verify
      FileInputStream input = new FileInputStream(testFile);
      for (int i = 0; i < messageSize; i++) {
         byte b = (byte) input.read();
         assertEquals("position = " + i, msgs[i], b);
      }
      input.close();

      testFile.delete();
      validateNoFilesOnLargeDir();
   }

   @Override
   @Test
   public void testSendServerMessage() throws Exception {
      // doesn't make sense as compressed
   }


   // https://issues.apache.org/jira/projects/ARTEMIS/issues/ARTEMIS-3751
   @Test
   public void testOverrideSize() throws Exception {
      ActiveMQServer server = createServer(true, true);

      server.start();

      ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616?minLargeMessageSize=" + (200 * 1024) + "&compressLargeMessage=true");
      Connection connection = cf.createConnection();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      connection.start();
      Destination queue = session.createQueue("testQueue");

      MessageProducer producer = session.createProducer(queue);
      MessageConsumer consumer = session.createConsumer(queue);

      // What we want is to get a message witch size is larger than client's minLargeMessageSize (200 kibs here) while compressed the size must be lower than that
      // and at same time it must be greater than server minLargeMessageSize (which is 100 kibs by default)
      byte[] data = new byte[1024 * 300];
      new DeflateGenerator(new Random(42), 2.0, 2.0).generate(data, 2);

      assertCompressionSize(data, 100 * 1024, 200 * 1024);

      BytesMessage outMessage = session.createBytesMessage();
      outMessage.writeBytes(data);

      producer.send(outMessage);

      javax.jms.Message inMessage = consumer.receive(1000);

      assertEqualsByteArrays(data, inMessage.getBody(byte[].class));
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
