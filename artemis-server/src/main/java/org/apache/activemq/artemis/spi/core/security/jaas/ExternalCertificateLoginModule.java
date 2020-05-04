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
package org.apache.activemq.artemis.spi.core.security.jaas;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.cert.X509Certificate;
import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;

/**
 * A LoginModule that propagates TLS certificates subject DN as a UserPrincipal.
 */
public class ExternalCertificateLoginModule implements AuditLoginModule {

   private static final Logger logger = Logger.getLogger(ExternalCertificateLoginModule.class);

   private CallbackHandler callbackHandler;
   private Subject subject;
   private String userName;

   private final Set<Principal> principals = new HashSet<>();

   @Override
   public void initialize(Subject subject,
                          CallbackHandler callbackHandler,
                          Map<String, ?> sharedState,
                          Map<String, ?> options) {
      this.subject = subject;
      this.callbackHandler = callbackHandler;
   }

   @Override
   public boolean login() throws LoginException {
      Callback[] callbacks = new Callback[1];

      callbacks[0] = new CertificateCallback();
      try {
         callbackHandler.handle(callbacks);
      } catch (IOException ioe) {
         throw new LoginException(ioe.getMessage());
      } catch (UnsupportedCallbackException uce) {
         throw new LoginException("Unable to obtain client certificates: " + uce.getMessage());
      }

      X509Certificate[] certificates = ((CertificateCallback) callbacks[0]).getCertificates();
      if (certificates != null && certificates.length > 0 && certificates[0] != null) {
         userName = certificates[0].getSubjectDN().getName();
      }

      logger.debug("Certificates: " + Arrays.toString(certificates) + ", userName: " + userName);
      return userName != null;
   }

   @Override
   public boolean commit() throws LoginException {
      if (userName != null) {
         principals.add(new UserPrincipal(userName));
         subject.getPrincipals().addAll(principals);
      }

      clear();
      logger.debug("commit");
      return true;
   }

   @Override
   public boolean abort() throws LoginException {
      registerFailureForAudit(userName);
      clear();
      logger.debug("abort");
      return true;
   }

   @Override
   public boolean logout() {
      subject.getPrincipals().removeAll(principals);
      principals.clear();
      logger.debug("logout");
      return true;
   }

   private void clear() {
      userName = null;
   }
}
