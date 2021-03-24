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

import static org.apache.activemq.artemis.core.remoting.CertificateUtil.getCertsFromConnection;
import static org.apache.activemq.artemis.core.remoting.CertificateUtil.getPeerPrincipalFromConnection;

import java.io.IOException;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

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
         } else if (callback instanceof PrincipalsCallback) {
            PrincipalsCallback principalsCallback = (PrincipalsCallback) callback;

            Subject peerSubject = remotingConnection.getSubject();
            if (peerSubject != null) {
               for (KerberosPrincipal principal : peerSubject.getPrivateCredentials(KerberosPrincipal.class)) {
                  principalsCallback.setPeerPrincipals(new Principal[] {principal});
                  return;
               }
               Set<Principal> principals = peerSubject.getPrincipals();
               if (principals.size() > 0) {
                  principalsCallback.setPeerPrincipals(principals.toArray(new Principal[0]));
                  return;
               }
            }

            Principal peerPrincipalFromConnection = getPeerPrincipalFromConnection(remotingConnection);
            if (peerPrincipalFromConnection != null) {
               principalsCallback.setPeerPrincipals(new Principal[] {peerPrincipalFromConnection});
            }
         } else {
            throw new UnsupportedCallbackException(callback);
         }
      }
   }
}
