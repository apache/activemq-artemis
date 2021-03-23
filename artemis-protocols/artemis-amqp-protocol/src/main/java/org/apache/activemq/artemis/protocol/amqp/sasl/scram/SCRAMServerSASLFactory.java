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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.AmqpInterceptor;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.ServerSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.ServerSASLFactory;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.security.jaas.DigestCallback;
import org.apache.activemq.artemis.spi.core.security.jaas.HmacCallback;
import org.apache.activemq.artemis.spi.core.security.jaas.SCRAMMechanismCallback;
import org.apache.activemq.artemis.spi.core.security.scram.SCRAM;
import org.apache.activemq.artemis.spi.core.security.scram.ScramException;
import org.apache.activemq.artemis.spi.core.security.scram.UserData;
import org.jboss.logging.Logger;

/**
 * abstract class that implements the SASL-SCRAM authentication scheme, concrete implementations
 * must supply the {@link SCRAM} type to use and be register via SPI
 */
public abstract class SCRAMServerSASLFactory implements ServerSASLFactory {

   private final Logger logger = Logger.getLogger(getClass());
   private final SCRAM scramType;

   public SCRAMServerSASLFactory(SCRAM scram) {
      this.scramType = scram;
   }

   @Override
   public String getMechanism() {
      return scramType.getName();
   }

   @Override
   public boolean isDefaultPermitted() {
      return false;
   }

   @Override
   public ServerSASL create(ActiveMQServer server, ProtocolManager<AmqpInterceptor> manager, Connection connection,
                            RemotingConnection remotingConnection) {
      try {
         if (manager instanceof ProtonProtocolManager) {
            ScramServerFunctionalityImpl scram =
                     new ScramServerFunctionalityImpl(scramType.getDigest(), scramType.getHmac(),
                                                      UUID.randomUUID().toString());
            String loginConfigScope = ((ProtonProtocolManager) manager).getSaslLoginConfigScope();
            return new SCRAMServerSASL(scramType.getName(), scram, loginConfigScope, logger);
         }
      } catch (NoSuchAlgorithmException e) {
         // can't be used then...
      }
      return null;
   }

   private static final class SCRAMServerSASL implements ServerSASL {

      private final String name;
      private final ScramServerFunctionality scram;
      private SASLResult result;
      private final String loginConfigScope;
      private final Logger logger;

      SCRAMServerSASL(String name, ScramServerFunctionality scram, String loginConfigScope, Logger logger) {
         this.name = name;
         this.scram = scram;
         this.loginConfigScope = loginConfigScope;
         this.logger = logger;
      }

      @Override
      public String getName() {
         return name;
      }

      @Override
      public byte[] processSASL(byte[] bytes) {
         String message = new String(bytes, StandardCharsets.US_ASCII);
         LoginContext loginContext = null;
         try {
            switch (scram.getState()) {
               case INITIAL: {
                  String userName = scram.handleClientFirstMessage(message);
                  if (userName != null) {

                     loginContext = new LoginContext(loginConfigScope, new CallbackHandler() {

                        @Override
                        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                           for (Callback callback : callbacks) {
                              if (callback instanceof NameCallback) {
                                 ((NameCallback) callback).setName(userName);
                              } else if (callback instanceof SCRAMMechanismCallback) {
                                 ((SCRAMMechanismCallback) callback).setMechanism(name);
                              } else if (callback instanceof DigestCallback) {
                                 ((DigestCallback) callback).setDigest(scram.getDigest());
                              } else if (callback instanceof HmacCallback) {
                                 ((HmacCallback) callback).setHmac(scram.getHmac());
                              } else {
                                 throw new UnsupportedCallbackException(callback, "Unrecognized Callback " +
                                          callback.getClass().getSimpleName());
                              }
                           }
                        }
                     });
                     loginContext.login();
                     try {
                        Subject subject = loginContext.getSubject();
                        Iterator<UserData> credentials = subject.getPublicCredentials(UserData.class).iterator();
                        Principal[] principals = subject.getPrincipals().toArray(new Principal[0]);
                        Subject saslSubject = new Subject(true, subject.getPrincipals(), subject.getPublicCredentials(),
                                                          Collections.singleton(principals));
                        if (credentials.hasNext()) {
                           result = new SCRAMSASLResult(userName, scram, saslSubject);
                           String challenge = scram.prepareFirstMessage(credentials.next());
                           return challenge.getBytes(StandardCharsets.US_ASCII);
                        }
                     } finally {
                        try {
                           loginContext.logout();
                        } catch (LoginException e1) {
                           // we can't do anything useful then but also don'T want to fail here...
                        }
                     }
                  }
                  break;
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
            if (loginContext != null) {
               try {
                  loginContext.logout();
               } catch (LoginException e1) {
                  // we can't do anything useful then...
               }
            }
         }
         return null;
      }

      @Override
      public SASLResult result() {
         if (result instanceof SCRAMSASLResult) {
            return scram.isEnded() ? result : null;
         }
         return result;
      }

      @Override
      public void done() {
      }

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
