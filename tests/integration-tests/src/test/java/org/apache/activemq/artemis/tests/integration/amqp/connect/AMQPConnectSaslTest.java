/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.security.auth.login.LoginException;

import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.scram.SCRAMServerSASL;
import org.apache.activemq.artemis.spi.core.security.scram.SCRAM;
import org.apache.activemq.artemis.spi.core.security.scram.ScramUtils;
import org.apache.activemq.artemis.spi.core.security.scram.UserData;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sasl.SaslOutcome;
import org.apache.qpid.proton.engine.Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.sasl.ProtonSaslAuthenticator;

public class AMQPConnectSaslTest extends AmqpClientTestSupport {

   private static final int BROKER_PORT_NUM = AMQP_PORT + 1;

   private static final String SERVER_KEYSTORE_NAME = "keystore1.jks";
   private static final String SERVER_KEYSTORE_PASSWORD = "changeit";
   private static final String CLIENT_KEYSTORE_NAME = "client_not_revoked.jks";
   private static final String CLIENT_KEYSTORE_PASSWORD = "changeit";
   private static final String TRUSTSTORE_NAME = "truststore.jks";
   private static final String TRUSTSTORE_PASSWORD = "changeit";

   private static final String USER = "MY_USER";
   private static final String PASSWD = "PASSWD_VALUE";

   private static final String PLAIN = "PLAIN";
   private static final String ANONYMOUS = "ANONYMOUS";
   private static final String EXTERNAL = "EXTERNAL";

   private Vertx vertx;
   private MockServer mockServer;

   @Override
   protected ActiveMQServer createServer() throws Exception {
      // Creates the broker used to make the outgoing connection. The port passed is for
      // that brokers acceptor. The test server connected to by the broker binds to a random port.
      return createServer(BROKER_PORT_NUM, false);
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      vertx = Vertx.vertx();
   }

   @After
   @Override
   public void tearDown() throws Exception {
      try {
         super.tearDown();
      } finally {
         if (mockServer != null) {
            mockServer.close();
         }

         CountDownLatch closeLatch = new CountDownLatch(1);
         vertx.close(x -> closeLatch.countDown());
         assertTrue("Vert.x instant not closed in alotted time", closeLatch.await(5, TimeUnit.SECONDS));
      }
   }

   @Test(timeout = 20000)
   public void testConnectsWithAnonymous() throws Exception {
      CountDownLatch serverConnectionOpen = new CountDownLatch(1);
      TestAuthenticator authenticator = new TestAuthenticator(true, PLAIN, ANONYMOUS);

      mockServer = new MockServer(vertx, () -> authenticator, serverConnection -> {
         serverConnection.openHandler(serverSender -> {
            serverConnectionOpen.countDown();
            serverConnection.closeHandler(x -> serverConnection.close());
            serverConnection.open();
         });
      });

      // No user or pass given, it will have to select ANONYMOUS even though PLAIN also offered
      AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://localhost:" + mockServer.actualPort());
      amqpConnection.setReconnectAttempts(0);// No reconnects

      server.getConfiguration().addAMQPConnection(amqpConnection);

      server.start();

      boolean awaitConnectionOpen = serverConnectionOpen.await(10, TimeUnit.SECONDS);
      assertTrue("Broker did not open connection in alotted time", awaitConnectionOpen);

      assertEquals(ANONYMOUS, authenticator.getChosenMech());
      assertArrayEquals(new byte[0], authenticator.getInitialResponse());
   }

   @Test(timeout = 20000)
   public void testConnectsWithPlain() throws Exception {
      CountDownLatch serverConnectionOpen = new CountDownLatch(1);
      TestAuthenticator authenticator = new TestAuthenticator(true, PLAIN, ANONYMOUS);

      mockServer = new MockServer(vertx, () -> authenticator, serverConnection -> {
         serverConnection.openHandler(serverSender -> {
            serverConnectionOpen.countDown();
            serverConnection.closeHandler(x -> serverConnection.close());
            serverConnection.open();
         });
      });

      // User and pass are given, it will select PLAIN
      AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://localhost:" + mockServer.actualPort());
      amqpConnection.setReconnectAttempts(0);// No reconnects
      amqpConnection.setUser(USER);
      amqpConnection.setPassword(PASSWD);

      server.getConfiguration().addAMQPConnection(amqpConnection);

      server.start();

      boolean awaitConnectionOpen = serverConnectionOpen.await(10, TimeUnit.SECONDS);
      assertTrue("Broker did not open connection in alotted time", awaitConnectionOpen);

      assertEquals(PLAIN, authenticator.getChosenMech());
      assertArrayEquals(expectedPlainInitialResponse(USER, PASSWD), authenticator.getInitialResponse());
   }

   @Test(timeout = 200000)
   public void testConnectsWithSCRAM() throws Exception {
      CountDownLatch serverConnectionOpen = new CountDownLatch(1);
      SCRAMTestAuthenticator authenticator = new SCRAMTestAuthenticator(SCRAM.SHA512);

      mockServer = new MockServer(vertx, () -> authenticator, serverConnection -> {
         serverConnection.openHandler(serverSender -> {
            serverConnectionOpen.countDown();
            serverConnection.closeHandler(x -> serverConnection.close());
            serverConnection.open();
         });
      });

      AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSScramConnect", "tcp://localhost:" + mockServer.actualPort());
      amqpConnection.setReconnectAttempts(0);// No reconnects
      amqpConnection.setUser(USER);
      amqpConnection.setPassword(PASSWD);

      server.getConfiguration().addAMQPConnection(amqpConnection);

      server.start();

      boolean awaitConnectionOpen = serverConnectionOpen.await(10, TimeUnit.SECONDS);
      assertTrue("Broker did not open connection in alotted time", awaitConnectionOpen);
      assertEquals(SCRAM.SHA512.getName(), authenticator.chosenMech);
      assertTrue(authenticator.succeeded());

   }

   @Test(timeout = 20000)
   public void testConnectsWithExternal() throws Exception {
      doConnectWithExternalTestImpl(true);
   }

   @Test(timeout = 20000)
   public void testExternalIgnoredWhenNoClientCertSupplied() throws Exception {
      doConnectWithExternalTestImpl(false);
   }

   private void doConnectWithExternalTestImpl(boolean requireClientCert) throws ExecutionException,
                                                                         InterruptedException, Exception {
      CountDownLatch serverConnectionOpen = new CountDownLatch(1);
      // The test server always offers EXTERNAL, i.e sometimes mistakenly, to verify that the broker
      // only selects it when it actually
      // has a client-cert. Real servers shouldnt actually offer the mechanism to a client that
      // didnt have to provide a cert.
      TestAuthenticator authenticator = new TestAuthenticator(true, EXTERNAL, PLAIN);

      final String keyStorePath = this.getClass().getClassLoader().getResource(SERVER_KEYSTORE_NAME).getFile();
      JksOptions jksKeyStoreOptions = new JksOptions().setPath(keyStorePath).setPassword(SERVER_KEYSTORE_PASSWORD);

      ProtonServerOptions serverOptions = new ProtonServerOptions();
      serverOptions.setSsl(true);
      serverOptions.setKeyStoreOptions(jksKeyStoreOptions);

      if (requireClientCert) {
         final String trustStorePath = this.getClass().getClassLoader().getResource(TRUSTSTORE_NAME).getFile();
         JksOptions jksTrustStoreOptions = new JksOptions().setPath(trustStorePath).setPassword(TRUSTSTORE_PASSWORD);

         serverOptions.setTrustStoreOptions(jksTrustStoreOptions);
         serverOptions.setClientAuth(ClientAuth.REQUIRED);
      }

      mockServer = new MockServer(vertx, serverOptions, () -> authenticator, serverConnection -> {
         serverConnection.openHandler(serverSender -> {
            serverConnectionOpen.countDown();
            serverConnection.closeHandler(x -> serverConnection.close());
            serverConnection.open();
         });
      });

      String amqpServerConnectionURI = "tcp://localhost:" + mockServer.actualPort() +
               "?sslEnabled=true;trustStorePath=" + TRUSTSTORE_NAME + ";trustStorePassword=" + TRUSTSTORE_PASSWORD;
      if (requireClientCert) {
         amqpServerConnectionURI +=
                  ";keyStorePath=" + CLIENT_KEYSTORE_NAME + ";keyStorePassword=" + CLIENT_KEYSTORE_PASSWORD;
      }

      AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSimpleConnect", amqpServerConnectionURI);
      amqpConnection.setReconnectAttempts(0);// No reconnects
      amqpConnection.setUser(USER); // Wont matter if EXTERNAL is offered and a client-certificate
                                    // is provided, but will otherwise.
      amqpConnection.setPassword(PASSWD);

      server.getConfiguration().addAMQPConnection(amqpConnection);

      server.start();

      boolean awaitConnectionOpen = serverConnectionOpen.await(10, TimeUnit.SECONDS);
      assertTrue("Broker did not open connection in alotted time", awaitConnectionOpen);

      if (requireClientCert) {
         assertEquals(EXTERNAL, authenticator.getChosenMech());
         assertArrayEquals(new byte[0], authenticator.getInitialResponse());
      } else {
         assertEquals(PLAIN, authenticator.getChosenMech());
         assertArrayEquals(expectedPlainInitialResponse(USER, PASSWD), authenticator.getInitialResponse());
      }
   }

   private static byte[] expectedPlainInitialResponse(String username, String password) {
      Objects.requireNonNull(username);
      Objects.requireNonNull(password);
      if (username.isEmpty() || password.isEmpty()) {
         throw new IllegalArgumentException("Must provide at least 1 character in user and pass");
      }

      byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
      byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);

      byte[] data = new byte[usernameBytes.length + passwordBytes.length + 2];
      System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
      System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);

      return data;
   }

   private static final class TestAuthenticator implements ProtonSaslAuthenticator {
      private Sasl sasl;
      private final boolean succeed;
      private final String[] offeredMechs;
      String chosenMech = null;
      byte[] initialResponse = null;
      boolean done = false;

      TestAuthenticator(boolean succeed, String... offeredMechs) {
         if (offeredMechs.length == 0) {
            throw new IllegalArgumentException("Must provide at least 1 mechanism to offer");
         }

         this.offeredMechs = offeredMechs;
         this.succeed = succeed;
      }

      @Override
      public void init(NetSocket socket, ProtonConnection protonConnection, Transport transport) {
         this.sasl = transport.sasl();
         sasl.server();
         sasl.allowSkip(false);
         sasl.setMechanisms(offeredMechs);
      }

      @Override
      public void process(Handler<Boolean> processComplete) {
         if (!done) {
            String[] remoteMechanisms = sasl.getRemoteMechanisms();
            if (remoteMechanisms.length > 0) {
               chosenMech = remoteMechanisms[0];

               initialResponse = new byte[sasl.pending()];
               sasl.recv(initialResponse, 0, initialResponse.length);
               if (succeed) {
                  sasl.done(SaslOutcome.PN_SASL_OK);
               } else {
                  sasl.done(SaslOutcome.PN_SASL_AUTH);
               }

               done = true;
            }
         }

         processComplete.handle(done);
      }

      @Override
      public boolean succeeded() {
         return succeed;
      }

      public String getChosenMech() {
         return chosenMech;
      }

      public byte[] getInitialResponse() {
         return initialResponse;
      }
   }

   private static final class SCRAMTestAuthenticator implements ProtonSaslAuthenticator {

      private final SCRAM mech;
      private Sasl sasl;
      private TestSCRAMServerSASL serverSASL;
      private String chosenMech;

      SCRAMTestAuthenticator(SCRAM mech) {
         this.mech = mech;
      }

      @Override
      public void init(NetSocket socket, ProtonConnection protonConnection, Transport transport) {
         this.sasl = transport.sasl();
         sasl.server();
         sasl.allowSkip(false);
         sasl.setMechanisms(mech.getName(), PLAIN, ANONYMOUS);
         try {
            serverSASL = new TestSCRAMServerSASL(mech);
         } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
         }

      }

      @Override
      public void process(Handler<Boolean> completionHandler) {
         String[] remoteMechanisms = sasl.getRemoteMechanisms();
         int pending = sasl.pending();
         if (remoteMechanisms.length == 0 || pending == 0) {
            completionHandler.handle(false);
            return;
         }
         chosenMech = remoteMechanisms[0];
         byte[] msg = new byte[pending];
         sasl.recv(msg, 0, msg.length);
         byte[] result = serverSASL.processSASL(msg);
         if (result != null) {
            sasl.send(result, 0, result.length);
         }
         boolean ended = serverSASL.isEnded();
         if (ended) {
            if (succeeded()) {
               sasl.done(SaslOutcome.PN_SASL_OK);
            } else {
               sasl.done(SaslOutcome.PN_SASL_AUTH);
            }
            completionHandler.handle(true);
         } else {
            completionHandler.handle(false);
         }
      }

      @Override
      public boolean succeeded() {
         SASLResult result = serverSASL.result();
         return result != null && result.isSuccess() && serverSASL.e == null;
      }

   }

   private static final class TestSCRAMServerSASL extends SCRAMServerSASL {

      private Exception e;

      TestSCRAMServerSASL(SCRAM mechanism) throws NoSuchAlgorithmException {
         super(mechanism);
      }

      @Override
      public void done() {
         // nothing to do
      }

      @Override
      protected UserData aquireUserData(String userName) throws LoginException {
         if (!USER.equals(userName)) {
            throw new LoginException("invalid username");
         }
         byte[] salt = new byte[32];
         new SecureRandom().nextBytes(salt);
         try {
            MessageDigest digest = MessageDigest.getInstance(mechanism.getDigest());
            Mac hmac = Mac.getInstance(mechanism.getHmac());
            ScramUtils.NewPasswordStringData data =
                     ScramUtils.byteArrayToStringData(ScramUtils.newPassword(PASSWD, salt, 4096, digest, hmac));
            return new UserData(data.salt, data.iterations, data.serverKey, data.storedKey);
         } catch (Exception e) {
            throw new LoginException(e.getMessage());
         }
      }

      @Override
      protected void failed(Exception e) {
         this.e = e;
      }

   }

}
