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
package org.apache.activemq.artemis.protocol.amqp.sasl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.Mac;
import javax.security.auth.login.LoginException;

import org.apache.activemq.artemis.protocol.amqp.sasl.scram.SCRAMClientSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.scram.SCRAMServerSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.scram.ScramServerFunctionalityImpl;
import org.apache.activemq.artemis.spi.core.security.scram.SCRAM;
import org.apache.activemq.artemis.spi.core.security.scram.ScramException;
import org.apache.activemq.artemis.spi.core.security.scram.ScramUtils;
import org.apache.activemq.artemis.spi.core.security.scram.UserData;
import org.apache.qpid.proton.codec.DecodeException;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * test cases for the SASL-SCRAM
 */
@RunWith(Parameterized.class)
public class SCRAMTest {

   /**
    *
    */
   private final SCRAM mechanism;
   private static final byte[] SALT = new byte[32];
   private static final String SNONCE = "server";
   private static final String CNONCE = "client";
   private static final String USERNAME = "test";
   private static final String PASSWORD = "123";

   @Parameters(name = "{0}")
   public static List<Object[]> data() {
      List<Object[]> list = new ArrayList<>();
      for (SCRAM scram : SCRAM.values()) {
         list.add(new Object[] {scram});
      }
      return list;
   }

   public SCRAMTest(SCRAM mechanism) {
      this.mechanism = mechanism;
   }

   @Test
   public void testSuccess() throws NoSuchAlgorithmException {
      TestSCRAMServerSASL serverSASL = new TestSCRAMServerSASL(mechanism, USERNAME, PASSWORD);
      TestSCRAMClientSASL clientSASL = new TestSCRAMClientSASL(mechanism, USERNAME, PASSWORD);
      byte[] clientFirst = clientSASL.getInitialResponse();
      assertNotNull(clientFirst);
      byte[] serverFirst = serverSASL.processSASL(clientFirst);
      assertNotNull(serverFirst);
      assertNull(serverSASL.result());
      byte[] clientFinal = clientSASL.getResponse(serverFirst);
      assertNotNull(clientFinal);
      assertFalse(clientFinal.length == 0);
      byte[] serverFinal = serverSASL.processSASL(clientFinal);
      assertNotNull(serverFinal);
      assertNotNull(serverSASL.result());
      assertNotNull(serverSASL.result().getSubject());
      assertEquals(USERNAME, serverSASL.result().getUser());
      assertNull(serverSASL.exception);
      assertTrue(serverSASL.result().isSuccess());
      byte[] clientCheck = clientSASL.getResponse(serverFinal);
      assertNotNull(clientCheck);
      assertTrue(clientCheck.length == 0);
   }

   @Test
   public void testWrongClientPassword() throws NoSuchAlgorithmException {
      TestSCRAMServerSASL serverSASL = new TestSCRAMServerSASL(mechanism, USERNAME, PASSWORD);
      TestSCRAMClientSASL clientSASL = new TestSCRAMClientSASL(mechanism, USERNAME, "xyz");
      byte[] clientFirst = clientSASL.getInitialResponse();
      assertNotNull(clientFirst);
      byte[] serverFirst = serverSASL.processSASL(clientFirst);
      assertNotNull(serverFirst);
      assertNull(serverSASL.result());
      byte[] clientFinal = clientSASL.getResponse(serverFirst);
      assertNotNull(clientFinal);
      assertFalse(clientFinal.length == 0);
      byte[] serverFinal = serverSASL.processSASL(clientFinal);
      assertNull(serverFinal);
      assertNotNull(serverSASL.result());
      assertFalse(serverSASL.result().isSuccess());
      assertThat(serverSASL.exception, IsInstanceOf.instanceOf(ScramException.class));
   }

   @Test(expected = DecodeException.class)
   public void testServerTryTrickClient() throws NoSuchAlgorithmException, ScramException {
      TestSCRAMClientSASL clientSASL = new TestSCRAMClientSASL(mechanism, USERNAME, PASSWORD);
      ScramServerFunctionalityImpl bad =
               new ScramServerFunctionalityImpl(mechanism.getDigest(), mechanism.getHmac(), SNONCE);
      byte[] clientFirst = clientSASL.getInitialResponse();
      assertNotNull(clientFirst);
      bad.handleClientFirstMessage(new String(clientFirst, StandardCharsets.US_ASCII));
      byte[] serverFirst =
               bad.prepareFirstMessage(generateUserData(mechanism, "bad")).getBytes(StandardCharsets.US_ASCII);
      byte[] clientFinal = clientSASL.getResponse(serverFirst);
      assertNotNull(clientFinal);
      assertFalse(clientFinal.length == 0);
      byte[] serverFinal = bad.prepareFinalMessageUnchecked(new String(clientFinal, StandardCharsets.US_ASCII))
                              .getBytes(StandardCharsets.US_ASCII);
      clientSASL.getResponse(serverFinal);
   }

   private static UserData generateUserData(SCRAM mechanism, String password) throws NoSuchAlgorithmException,
                                                                              ScramException {
      MessageDigest digest = MessageDigest.getInstance(mechanism.getDigest());
      Mac hmac = Mac.getInstance(mechanism.getHmac());
      ScramUtils.NewPasswordStringData data =
               ScramUtils.byteArrayToStringData(ScramUtils.newPassword(password, SALT, 4096, digest, hmac));
      return new UserData(data.salt, data.iterations, data.serverKey, data.storedKey);
   }


   private static final class TestSCRAMClientSASL extends SCRAMClientSASL {

      TestSCRAMClientSASL(SCRAM scram, String username, String password) {
         super(scram, username, password, CNONCE);
      }

   }

   private static final class TestSCRAMServerSASL extends SCRAMServerSASL {

      private Exception exception;
      private final String username;
      private final String password;

      TestSCRAMServerSASL(SCRAM mechanism, String username, String password) throws NoSuchAlgorithmException {
         super(mechanism, SNONCE);
         this.username = username;
         this.password = password;
      }

      @Override
      public void done() {
         // nothing to do
      }

      @Override
      protected UserData aquireUserData(String userName) throws LoginException {
         if (!this.username.equals(userName)) {
            throw new LoginException("invalid username");
         }
         try {
            return generateUserData(mechanism, password);
         } catch (Exception e) {
            throw new LoginException(e.getMessage());
         }
      }

      @Override
      protected void failed(Exception e) {
         this.exception = e;
      }

   }
}
