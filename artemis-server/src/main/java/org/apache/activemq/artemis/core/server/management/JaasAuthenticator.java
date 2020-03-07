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
   public Subject authenticate(final Object credentials) throws SecurityException {
      try {
         Subject subject = new Subject();

         LoginContext loginContext = new LoginContext(realm, subject, new CallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
               /*
               * pull out the jmx credentials if they exist if not, guest login module will handle it
               * */
               String[] params = null;
               if (credentials instanceof String[] && ((String[]) credentials).length == 2) {
                  params = (String[]) credentials;
               }
               for (int i = 0; i < callbacks.length; i++) {
                  if (callbacks[i] instanceof NameCallback) {
                     if (params != null) {
                        ((NameCallback) callbacks[i]).setName(params[0]);
                     }
                  } else if (callbacks[i] instanceof PasswordCallback) {
                     if (params != null) {
                        ((PasswordCallback) callbacks[i]).setPassword((params[1].toCharArray()));
                     }
                  } else {
                     throw new UnsupportedCallbackException(callbacks[i]);
                  }
               }
            }
         });
         loginContext.login();
         return subject;
      } catch (LoginException e) {
         throw new SecurityException("Authentication failed", e);
      }
   }
}
