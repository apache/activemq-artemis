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
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

public class PropertiesLoginModule extends PropertiesLoader implements LoginModule {

   private static final String USER_FILE_PROP_NAME = "org.apache.activemq.jaas.properties.user";
   private static final String ROLE_FILE_PROP_NAME = "org.apache.activemq.jaas.properties.role";

   private Subject subject;
   private CallbackHandler callbackHandler;

   private Properties users;
   private Properties roles;
   private String user;
   private final Set<Principal> principals = new HashSet<>();
   private boolean loginSucceeded;

   @Override
   public void initialize(Subject subject,
                          CallbackHandler callbackHandler,
                          Map sharedState,
                          Map options) {
      this.subject = subject;
      this.callbackHandler = callbackHandler;
      loginSucceeded = false;

      init(options);
      users = load(USER_FILE_PROP_NAME, "user", options).getProps();
      roles = load(ROLE_FILE_PROP_NAME, "role", options).getProps();
   }

   @Override
   public boolean login() throws LoginException {
      Callback[] callbacks = new Callback[2];

      callbacks[0] = new NameCallback("Username: ");
      callbacks[1] = new PasswordCallback("Password: ", false);
      try {
         callbackHandler.handle(callbacks);
      }
      catch (IOException ioe) {
         throw new LoginException(ioe.getMessage());
      }
      catch (UnsupportedCallbackException uce) {
         throw new LoginException(uce.getMessage() + " not available to obtain information from user");
      }
      user = ((NameCallback) callbacks[0]).getName();
      char[] tmpPassword = ((PasswordCallback) callbacks[1]).getPassword();
      if (tmpPassword == null) {
         tmpPassword = new char[0];
      }
      if (user == null) {
         throw new FailedLoginException("user name is null");
      }
      String password = users.getProperty(user);

      if (password == null) {
         throw new FailedLoginException("User does exist");
      }
      if (!password.equals(new String(tmpPassword))) {
         throw new FailedLoginException("Password does not match");
      }
      loginSucceeded = true;

      if (debug) {
         ActiveMQServerLogger.LOGGER.debug("login " + user);
      }
      return loginSucceeded;
   }

   @Override
   public boolean commit() throws LoginException {
      boolean result = loginSucceeded;
      if (result) {
         principals.add(new UserPrincipal(user));

         for (Map.Entry<Object, Object> entry : roles.entrySet()) {
            String name = (String) entry.getKey();
            String[] userList = ((String) entry.getValue()).split(",");
            if (debug) {
               ActiveMQServerLogger.LOGGER.debug("Inspecting role '" + name + "' with user(s): " + entry.getValue());
            }
            for (int i = 0; i < userList.length; i++) {
               if (user.equals(userList[i])) {
                  principals.add(new RolePrincipal(name));
                  break;
               }
            }
         }

         subject.getPrincipals().addAll(principals);
      }

      // will whack loginSucceeded
      clear();

      if (debug) {
         ActiveMQServerLogger.LOGGER.debug("commit, result: " + result);
      }
      return result;
   }

   @Override
   public boolean abort() throws LoginException {
      clear();

      if (debug) {
         ActiveMQServerLogger.LOGGER.debug("abort");
      }
      return true;
   }

   @Override
   public boolean logout() throws LoginException {
      subject.getPrincipals().removeAll(principals);
      principals.clear();
      clear();
      if (debug) {
         ActiveMQServerLogger.LOGGER.debug("logout");
      }
      return true;
   }

   private void clear() {
      user = null;
      loginSucceeded = false;
   }
}
