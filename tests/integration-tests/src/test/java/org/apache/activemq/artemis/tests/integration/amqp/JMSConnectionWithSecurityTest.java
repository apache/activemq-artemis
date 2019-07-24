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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.amqp.testutil.FrameWriter;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.security.SaslInit;
import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

public class JMSConnectionWithSecurityTest extends JMSClientTestSupport {

   @Override
   protected boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected String getJmsConnectionURIOptions() {
      return "amqp.saslMechanisms=PLAIN";
   }

   @Test(timeout = 10000)
   public void testNoUserOrPassword() throws Exception {
      Connection connection = null;
      try {
         connection = createConnection("", "", null, false);
         connection.start();
         fail("Expected JMSException");
      } catch (JMSSecurityException ex) {
         IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with no user / password.");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 10000)
   public void testNoUserOrPasswordWithoutSaslRestrictions() throws Exception {
      Connection connection = null;
      JmsConnectionFactory factory = new JmsConnectionFactory(new URI("amqp://localhost:" + AMQP_PORT));
      try {
         connection = factory.createConnection();
         connection.start();
         fail("Expected Exception");
      } catch (JMSSecurityException ex) {
         IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with no user / password.");
      } catch (Exception ex) {
         fail("Expected JMSSecurityException");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   /**
     * $ PN_TRACE_FRM=1 ./target/bin/aac3_sender -b "localhost:34949/examples" --log-msgs dict -c 1
     * [0x9ea9d0]:  -> SASL
     * [0x9ea9d0]:  <- SASL
     * [0x9ea9d0]:0 <- @sasl-mechanisms(64) [sasl-server-mechanisms=@PN_SYMBOL[:PLAIN, :ANONYMOUS]]
     * [0x9ea9d0]:0 -> @sasl-init(65) [mechanism=:ANONYMOUS, initial-response=b"anonymous@nixos"]
     * [0x9ea9d0]:0 <- @sasl-outcome(68) [code=0]
     * [0x9ea9d0]:  -> AMQP
     * [0x9ea9d0]:0 -> @open(16) [container-id="204c1d45-9c47-402d-809f-7d17a4d97d6e", hostname="localhost", channel-max=32767]
     * [0x9ea9d0]:0 -> @begin(17) [next-outgoing-id=0, incoming-window=2147483647, outgoing-window=2147483647]
     * [0x9ea9d0]:0 -> @attach(18) [name="2b46ad5b-834b-454e-a2f7-2e5e0e324e21", handle=0, role=false, snd-settle-mode=2, rcv-settle-mode=0, source=@source(40) [durable=0, timeout=0, dynamic=false], target=@target(41) [address="examples", durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
     * [0x9ea9d0]:  <- AMQP
     * [0x9ea9d0]:0 <- @open(16) [container-id="localhost", max-frame-size=131072, channel-max=65535, idle-time-out=30000, offered-capabilities=@PN_SYMBOL[:"sole-connection-for-container", :"DELAYED_DELIVERY", :"SHARED-SUBS", :"ANONYMOUS-RELAY"], properties={:product="apache-activemq-artemis", :version="2.9.0"}]
     * [0x9ea9d0]:0 <- @close(24) [error=@error(29) [condition=:"amqp:internal-error", description="Unrecoverable error: NullPointerException"]]
     * [0x9ea9d0]:  <- EOS
     * [error]: Failed to connect to localhost:34949
     * [0x9ea9d0]:0 -> @close(24) []
     * [0x9ea9d0]:  -> EOS
     *
     * The numerical constants are arbitrary, important thing is to send open, begin and attach in one go (pipelined).
     */
   @Test
   public void testNoUserOrPasswordWithoutSaslRestrictionsWithPipelinedOpen() throws Exception {
      SaslInit saslInit = new SaslInit();
      saslInit.setMechanism(Symbol.valueOf("ANONYMOUS"));
      saslInit.setHostname("anonymous@nixos");

      Open open = new org.apache.qpid.proton.amqp.transport.Open();
      open.setContainerId("204c1d45-9c47-402d-809f-7d17a4d97d6e");
      open.setHostname("localhost");
      open.setChannelMax(UnsignedShort.valueOf((short) 32767));
      open.setMaxFrameSize(UnsignedInteger.valueOf(131072));

      Begin begin = new Begin();
      begin.setNextOutgoingId(UnsignedInteger.valueOf(0));
      begin.setOutgoingWindow(UnsignedInteger.valueOf(2147483647));
      begin.setIncomingWindow(UnsignedInteger.valueOf(2147483647));

      Attach attach = new Attach();
      attach.setName("2b46ad5b-834b-454e-a2f7-2e5e0e324e21");
      attach.setHandle(UnsignedInteger.valueOf(0));
      attach.setSndSettleMode(SenderSettleMode.MIXED);
      attach.setRcvSettleMode(ReceiverSettleMode.FIRST);

      Source source = new Source();
      source.setDurable(TerminusDurability.NONE);
      source.setTimeout(UnsignedInteger.valueOf(0));
      source.setDynamic(false);
      attach.setSource(source);

      Target target = new Target();
      target.setAddress("examples");
      target.setDurable(TerminusDurability.NONE);
      target.setTimeout(UnsignedInteger.valueOf(0));
      target.setDynamic(false);
      attach.setTarget(target);

      attach.setInitialDeliveryCount(UnsignedInteger.valueOf(0));
      attach.setMaxMessageSize(UnsignedLong.valueOf(0));


      DecoderImpl decoder = new DecoderImpl();
      EncoderImpl encoder = new EncoderImpl(decoder);
      AMQPDefinedTypes.registerAllTypes(decoder, encoder);

      ByteBuffer buf = ByteBuffer.allocate(10 * 1024); // plenty of space

      FrameWriter frameWriter;
      frameWriter = new FrameWriter(encoder, 131072, FrameWriter.SASL_FRAME_TYPE);
      frameWriter.writeHeader(AmqpHeader.SASL_HEADER);
      frameWriter.writeFrame(saslInit);
      frameWriter.readBytes(buf);

      buf.flip();

      InetSocketAddress hostAddress = new InetSocketAddress("localhost", AMQP_PORT);
      SocketChannel client = SocketChannel.open(hostAddress);

      client.write(buf);

      buf.clear();

      frameWriter = new FrameWriter(encoder, 131072, FrameWriter.AMQP_FRAME_TYPE);
      frameWriter.writeHeader(AmqpHeader.HEADER);
      frameWriter.writeFrame(open);
      frameWriter.writeFrame(begin);
      frameWriter.writeFrame(attach); // if this line is commented out, test passes

      frameWriter.readBytes(buf);

      buf.flip();
      client.write(buf);

      client.configureBlocking(false);
      buf.clear();

      StringBuilder response = new StringBuilder();
      Instant thence = Instant.now();
      while (Duration.between(thence, Instant.now()).toMillis() < 1000) {
         int read = client.read(buf);
         if (read == -1) {
            break;
         }
         if (read > 0) {
            buf.flip();
            response.append(StandardCharsets.US_ASCII.decode(buf));
         }
         buf.clear();
      }

      assertFalse("Does not contain amqp:internal-error", response.toString().contains("amqp:internal-error"));
      assertTrue("Does contain amqp:unauthorized-access", response.toString().contains("amqp:unauthorized-access"));

      client.close();
   }

   @Test(timeout = 10000)
   public void testUnknownUser() throws Exception {
      Connection connection = null;
      try {
         connection = createConnection("nosuchuser", "blah", null, false);
         connection.start();
         fail("Expected JMSException");
      } catch (JMSSecurityException ex) {
         IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with unknown user ID");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 10000)
   public void testKnownUserWrongPassword() throws Exception {
      Connection connection = null;
      try {
         connection = createConnection(fullUser, "wrongPassword", null, false);
         connection.start();
         fail("Expected JMSException");
      } catch (JMSSecurityException ex) {
         IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with incorrect password.");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 30000)
   public void testRepeatedWrongPasswordAttempts() throws Exception {
      for (int i = 0; i < 25; ++i) {
         Connection connection = null;
         try {
            connection = createConnection(fullUser, "wrongPassword", null, false);
            connection.start();
            fail("Expected JMSException");
         } catch (JMSSecurityException ex) {
            IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with incorrect password.");
         } finally {
            if (connection != null) {
               connection.close();
            }
         }
      }
   }

   @Test(timeout = 30000)
   public void testSendReceive() throws Exception {
      Connection connection = createConnection(fullUser, fullPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer p = session.createProducer(queue);
         TextMessage message = null;
         message = session.createTextMessage();
         String messageText = "hello  sent at " + new java.util.Date().toString();
         message.setText(messageText);
         p.send(message);

         // Get the message we just sent
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
         Message msg = consumer.receive(5000);
         assertNotNull(msg);
         assertTrue(msg instanceof TextMessage);
         TextMessage textMessage = (TextMessage) msg;
         assertEquals(messageText, textMessage.getText());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testConsumerNotAuthorized() throws Exception {
      Connection connection = createConnection(noprivUser, noprivPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         try {
            session.createConsumer(queue);
            fail("Should not be able to consume here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testBrowserNotAuthorized() throws Exception {
      Connection connection = createConnection(noprivUser, noprivPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         try {
            QueueBrowser browser = session.createBrowser(queue);
            // Browser is not created until an enumeration is requesteda
            browser.getEnumeration();
            fail("Should not be able to consume here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testConsumerNotAuthorizedToCreateQueues() throws Exception {
      Connection connection = createConnection(noprivUser, noprivPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName(getPrecreatedQueueSize() + 1));
         try {
            session.createConsumer(queue);
            fail("Should not be able to consume here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testProducerNotAuthorized() throws Exception {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         try {
            session.createProducer(queue);
            fail("Should not be able to produce here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testAnonymousProducerNotAuthorized() throws Exception {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer producer = session.createProducer(null);

         try {
            producer.send(queue, session.createTextMessage());
            fail("Should not be able to produce here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testCreateTemporaryQueueNotAuthorized() throws JMSException {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try {
            session.createTemporaryQueue();
         } catch (JMSSecurityException jmsse) {
            IntegrationTestLogger.LOGGER.info("Client should have thrown a JMSSecurityException but only threw JMSException");
         }

         // Should not be fatal
         assertNotNull(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testCreateTemporaryTopicNotAuthorized() throws JMSException {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try {
            session.createTemporaryTopic();
         } catch (JMSSecurityException jmsse) {
            IntegrationTestLogger.LOGGER.info("Client should have thrown a JMSSecurityException but only threw JMSException");
         }

         // Should not be fatal
         assertNotNull(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
      } finally {
         connection.close();
      }
   }
}
