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
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import javax.security.cert.X509Certificate;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;

/**
 * A LoginModule that allows for authentication based on SSL certificates.
 * Allows for subclasses to define methods used to verify user certificates and
 * find user roles. Uses CertificateCallbacks to retrieve certificates.
 */
public abstract class CertificateLoginModule extends PropertiesLoader implements LoginModule {

   private static final Logger logger = Logger.getLogger(CertificateLoginModule.class);

   private CallbackHandler callbackHandler;
   private Subject subject;

   private X509Certificate[] certificates;
   private String username;
   private final Set<Principal> principals = new HashSet<>();

   /**
    * Overriding to allow for proper initialization. Standard JAAS.
    */
   @Override
   public void initialize(Subject subject,
                          CallbackHandler callbackHandler,
                          Map<String, ?> sharedState,
                          Map<String, ?> options) {
      this.subject = subject;
      this.callbackHandler = callbackHandler;

      init(options);
   }

   /**
    * Overriding to allow for certificate-based login. Standard JAAS.
    */
   @Override
   public boolean login() throws LoginException {
      Callback[] callbacks = new Callback[1];

      callbacks[0] = new CertificateCallback();
      try {
         callbackHandler.handle(callbacks);
      } catch (IOException ioe) {
         throw new LoginException(ioe.getMessage());
      } catch (UnsupportedCallbackException uce) {
         throw new LoginException(uce.getMessage() + " Unable to obtain client certificates.");
      }
      certificates = ((CertificateCallback) callbacks[0]).getCertificates();

      username = getUserNameForCertificates(certificates);
      if (username == null) {
         throw new FailedLoginException("No user for client certificate: " + getDistinguishedName(certificates));
      }

      if (debug) {
         logger.debug("Certificate for user: " + username);
      }
      return true;
   }

   /**
    * Overriding to complete login process. Standard JAAS.
    */
   @Override
   public boolean commit() throws LoginException {
      principals.add(new UserPrincipal(username));

      for (String role : getUserRoles(username)) {
         principals.add(new RolePrincipal(role));
      }

      subject.getPrincipals().addAll(principals);

      clear();

      if (debug) {
         logger.debug("commit");
      }
      return true;
   }

   /**
    * Standard JAAS override.
    */
   @Override
   public boolean abort() throws LoginException {
      clear();

      if (debug) {
         logger.debug("abort");
      }
      return true;
   }

   /**
    * Standard JAAS override.
    */
   @Override
   public boolean logout() {
      subject.getPrincipals().removeAll(principals);
      principals.clear();

      if (debug) {
         logger.debug("logout");
      }
      return true;
   }

   /**
    * Helper method.
    */
   private void clear() {
      certificates = null;
      username = null;
   }

   /**
    * Should return a unique name corresponding to the certificates given. The
    * name returned will be used to look up access levels as well as role
    * associations.
    *
    * @param certs The distinguished name.
    * @return The unique name if the certificate is recognized, null otherwise.
    */
   protected abstract String getUserNameForCertificates(final X509Certificate[] certs) throws LoginException;

   /**
    * Should return a set of the roles this user belongs to. The roles
    * returned will be added to the user's credentials.
    *
    * @param username The username of the client. This is the same name that
    *                 getUserNameForDn returned for the user's DN.
    * @return A Set of the names of the roles this user belongs to.
    */
   protected abstract Set<String> getUserRoles(final String username) throws LoginException;

   protected String getDistinguishedName(final X509Certificate[] certs) {
      if (certs != null && certs.length > 0 && certs[0] != null) {
         return certs[0].getSubjectDN().getName();
      } else {
         return null;
      }
   }

}
