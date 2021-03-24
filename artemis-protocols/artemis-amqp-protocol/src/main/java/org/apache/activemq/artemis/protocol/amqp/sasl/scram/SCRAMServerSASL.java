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
package org.apache.activemq.artemis.protocol.amqp.sasl.scram;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.UUID;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.ServerSASL;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.apache.activemq.artemis.spi.core.security.scram.SCRAM;
import org.apache.activemq.artemis.spi.core.security.scram.ScramException;
import org.apache.activemq.artemis.spi.core.security.scram.UserData;
import org.jboss.logging.Logger;

public abstract class SCRAMServerSASL implements ServerSASL {

   protected final ScramServerFunctionality scram;
   protected final SCRAM mechanism;
   private SASLResult result;
   private final Logger logger;

   public SCRAMServerSASL(SCRAM mechanism, Logger logger) throws NoSuchAlgorithmException {
      this.mechanism = mechanism;
      this.logger = logger;
      this.scram = new ScramServerFunctionalityImpl(mechanism.getDigest(), mechanism.getHmac(),
                                                    UUID.randomUUID().toString());
   }

   @Override
   public String getName() {
      return mechanism.getName();
   }

   @Override
   public byte[] processSASL(byte[] bytes) {
      String message = new String(bytes, StandardCharsets.US_ASCII);
      try {
         switch (scram.getState()) {
            case INITIAL: {
               String userName = scram.handleClientFirstMessage(message);
               UserData userData = aquireUserData(userName);
               Subject saslSubject = new Subject(false, Collections.singleton(new UserPrincipal(userName)),
                                                 Collections.singleton(userData), Collections.emptySet());
               result = new SCRAMSASLResult(userName, scram, saslSubject);
               String challenge = scram.prepareFirstMessage(userData);
               return challenge.getBytes(StandardCharsets.US_ASCII);
            }
            case PREPARED_FIRST: {
               String finalMessage = scram.prepareFinalMessage(message);
               return finalMessage.getBytes(StandardCharsets.US_ASCII);
            }
            default:
               result = new SCRAMFailedSASLResult();
               break;
         }
      } catch (GeneralSecurityException | ScramException | RuntimeException e) {
         logger.warn("SASL-SCRAM Authentication failed", e);
         result = new SCRAMFailedSASLResult();
      }
      return null;
   }

   protected abstract UserData aquireUserData(String userName) throws LoginException;

   @Override
   public SASLResult result() {
      if (result instanceof SCRAMSASLResult) {
         return scram.isEnded() ? result : null;
      }
      return result;
   }

   public boolean isEnded() {
      return scram.isEnded();
   }

   private static final class SCRAMSASLResult implements SASLResult {

      private final String userName;
      private final ScramServerFunctionality scram;
      private final Subject subject;

      SCRAMSASLResult(String userName, ScramServerFunctionality scram, Subject subject) {
         this.userName = userName;
         this.scram = scram;
         this.subject = subject;
      }

      @Override
      public String getUser() {
         return userName;
      }

      @Override
      public Subject getSubject() {
         return subject;
      }

      @Override
      public boolean isSuccess() {
         return userName != null && scram.isEnded() && scram.isSuccessful();
      }

      @Override
      public String toString() {
         return "SCRAMSASLResult: userName = " + userName + ", state = " + scram.getState();
      }

   }

   private static final class SCRAMFailedSASLResult implements SASLResult {

      @Override
      public String getUser() {
         return null;
      }

      @Override
      public Subject getSubject() {
         return null;
      }

      @Override
      public boolean isSuccess() {
         return false;
      }

      @Override
      public String toString() {
         return "SCRAMFailedSASLResult";
      }

   }

}