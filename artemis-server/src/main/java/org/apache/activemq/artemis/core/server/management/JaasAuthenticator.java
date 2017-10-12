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
package org.apache.activemq.artemis.core.server.management;

import javax.management.remote.JMXAuthenticator;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;

public class JaasAuthenticator implements JMXAuthenticator {

   private String realm;

   public String getRealm() {
      return realm;
   }

   public void setRealm(String realm) {
      this.realm = realm;
   }

   @Override
   public Subject authenticate(Object credentials) throws SecurityException {
      if (!(credentials instanceof String[])) {
         throw new IllegalArgumentException("Expected String[2], got "
               + (credentials != null ? credentials.getClass().getName() : null));
      }

      final String[] params = (String[]) credentials;
      if (params.length != 2) {
         throw new IllegalArgumentException("Expected String[2] but length was " + params.length);
      }
      try {
         Subject subject = new Subject();
         LoginContext loginContext = new LoginContext(realm, subject, new CallbackHandler() {

            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
               for (int i = 0; i < callbacks.length; i++) {
                  if (callbacks[i] instanceof NameCallback) {
                     ((NameCallback) callbacks[i]).setName(params[0]);
                  } else if (callbacks[i] instanceof PasswordCallback) {
                     ((PasswordCallback) callbacks[i]).setPassword((params[1].toCharArray()));
                  } else {
                     throw new UnsupportedCallbackException(callbacks[i]);
                  }
               }
            }
         });
         loginContext.login();

          /*  if (subject.getPrincipals().size() == 0) {
                // there must be some Principals, but which ones required are tested later
                throw new FailedLoginException("User does not have the required role");
            }*/

         return subject;
      } catch (LoginException e) {
         throw new SecurityException("Authentication failed", e);
      }
   }
}
