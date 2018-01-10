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

import org.jboss.logging.Logger;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.security.Principal;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * populate a subject with kerberos credential from the handler
 */
public class Krb5LoginModule implements LoginModule {

   private static final Logger logger = Logger.getLogger(Krb5LoginModule.class);

   private Subject subject;
   private final List<Principal> principals = new LinkedList<>();
   private CallbackHandler callbackHandler;
   private boolean loginSucceeded;

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

      callbacks[0] = new Krb5Callback();
      try {
         callbackHandler.handle(callbacks);
         Principal principal = ((Krb5Callback)callbacks[0]).getPeerPrincipal();
         if (principal != null) {
            principals.add(principal);
         }
      } catch (IOException ioe) {
         throw new LoginException(ioe.getMessage());
      } catch (UnsupportedCallbackException uce) {
         throw new LoginException(uce.getMessage() + " not available to obtain information from user");
      }
      if (!principals.isEmpty()) {
         loginSucceeded = true;
      }
      logger.debug("login " + principals);
      return loginSucceeded;
   }

   @Override
   public boolean commit() throws LoginException {
      boolean result = loginSucceeded;
      if (result) {
         principals.add(new UserPrincipal(principals.get(0).getName()));
         subject.getPrincipals().addAll(principals);
      }

      clear();

      logger.debug("commit, result: " + result);

      return result;
   }

   @Override
   public boolean abort() throws LoginException {
      clear();

      logger.debug("abort");

      return true;
   }

   private void clear() {
      loginSucceeded = false;
   }

   @Override
   public boolean logout() throws LoginException {
      subject.getPrincipals().removeAll(principals);
      principals.clear();
      clear();

      logger.debug("logout");

      return true;
   }
}
