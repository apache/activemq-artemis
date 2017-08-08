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

import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.IOException;
import java.security.Principal;

import static org.apache.activemq.artemis.core.remoting.CertificateUtil.getCertsFromConnection;
import static org.apache.activemq.artemis.core.remoting.CertificateUtil.getPeerPrincipalFromConnection;

/**
 * A JAAS username password CallbackHandler.
 */
public class JaasCallbackHandler implements CallbackHandler {

   private final String username;
   private final String password;
   final RemotingConnection remotingConnection;

   public JaasCallbackHandler(String username, String password, RemotingConnection remotingConnection) {
      this.username = username;
      this.password = password;
      this.remotingConnection = remotingConnection;
   }

   @Override
   public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
         if (callback instanceof PasswordCallback) {
            PasswordCallback passwordCallback = (PasswordCallback) callback;
            if (password == null) {
               passwordCallback.setPassword(null);
            } else {
               passwordCallback.setPassword(password.toCharArray());
            }
         } else if (callback instanceof NameCallback) {
            NameCallback nameCallback = (NameCallback) callback;
            if (username == null) {
               nameCallback.setName(null);
            } else {
               nameCallback.setName(username);
            }
         } else if (callback instanceof CertificateCallback) {
            CertificateCallback certCallback = (CertificateCallback) callback;

            certCallback.setCertificates(getCertsFromConnection(remotingConnection));
         } else if (callback instanceof Krb5Callback) {
            Krb5Callback krb5Callback = (Krb5Callback) callback;

            Subject peerSubject = remotingConnection.getSubject();
            if (peerSubject != null) {
               for (Principal principal : peerSubject.getPrivateCredentials(KerberosPrincipal.class)) {
                  krb5Callback.setPeerPrincipal(principal);
                  return;
               }
            }

            krb5Callback.setPeerPrincipal(getPeerPrincipalFromConnection(remotingConnection));
         } else {
            throw new UnsupportedCallbackException(callback);
         }
      }
   }
}
