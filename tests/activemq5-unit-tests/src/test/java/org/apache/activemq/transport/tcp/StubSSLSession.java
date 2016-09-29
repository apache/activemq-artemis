/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.transport.tcp;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

class StubSSLSession implements SSLSession {

   X509Certificate cert;
   boolean isVerified;

   public StubSSLSession(X509Certificate cert) {
      if (cert != null) {
         this.isVerified = true;
         this.cert = cert;
      } else {
         this.isVerified = false;
         this.cert = null;
      }
   }

   public void setIsVerified(boolean verified) {
      this.isVerified = verified;
   }

   @Override
   public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
      if (this.isVerified) {
         return new X509Certificate[]{this.cert};
      } else {
         throw new SSLPeerUnverifiedException("Socket is unverified.");
      }
   }

   // --- Stubbed methods ---

   @Override
   public byte[] getId() {
      return null;
   }

   @Override
   public SSLSessionContext getSessionContext() {
      return null;
   }

   @Override
   public long getCreationTime() {
      return 0;
   }

   @Override
   public long getLastAccessedTime() {
      return 0;
   }

   @Override
   public void invalidate() {
   }

   @Override
   public boolean isValid() {
      return false;
   }

   @Override
   public void putValue(String arg0, Object arg1) {
   }

   @Override
   public Object getValue(String arg0) {
      return null;
   }

   @Override
   public void removeValue(String arg0) {
   }

   @Override
   public String[] getValueNames() {
      return null;
   }

   @Override
   public Certificate[] getLocalCertificates() {
      return null;
   }

   @Override
   public javax.security.cert.X509Certificate[] getPeerCertificateChain() throws SSLPeerUnverifiedException {
      return null;
   }

   @Override
   public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
      return null;
   }

   @Override
   public Principal getLocalPrincipal() {
      return null;
   }

   @Override
   public String getCipherSuite() {
      return null;
   }

   @Override
   public String getProtocol() {
      return null;
   }

   @Override
   public String getPeerHost() {
      return null;
   }

   @Override
   public int getPeerPort() {
      return 0;
   }

   @Override
   public int getPacketBufferSize() {
      return 0;
   }

   @Override
   public int getApplicationBufferSize() {
      return 0;
   }
}
