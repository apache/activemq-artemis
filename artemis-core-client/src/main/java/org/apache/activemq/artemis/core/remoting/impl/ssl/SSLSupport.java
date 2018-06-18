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
package org.apache.activemq.artemis.core.remoting.impl.ssl;

import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CRL;
import java.security.cert.CertStore;
import java.security.cert.CertificateFactory;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.util.Collection;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.utils.ClassloadingUtil;

/**
 * Please note, this class supports PKCS#11 keystores, but there are no specific tests in the ActiveMQ Artemis test-suite to
 * validate/verify this works because this requires a functioning PKCS#11 provider which is not available by default
 * (see java.security.Security#getProviders()).  The main thing to keep in mind is that PKCS#11 keystores will have a
 * null keystore path.
 */
public class SSLSupport {
   private String keystoreProvider = TransportConstants.DEFAULT_KEYSTORE_PROVIDER;
   private String keystorePath = TransportConstants.DEFAULT_KEYSTORE_PATH;
   private String keystorePassword = TransportConstants.DEFAULT_KEYSTORE_PASSWORD;
   private String truststoreProvider = TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER;
   private String truststorePath = TransportConstants.DEFAULT_TRUSTSTORE_PATH;
   private String truststorePassword = TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD;
   private String crlPath = TransportConstants.DEFAULT_CRL_PATH;
   private String sslProvider = TransportConstants.DEFAULT_SSL_PROVIDER;
   private boolean trustAll = TransportConstants.DEFAULT_TRUST_ALL;

   public String getKeystoreProvider() {
      return keystoreProvider;
   }

   public SSLSupport setKeystoreProvider(String keystoreProvider) {
      this.keystoreProvider = keystoreProvider;
      return this;
   }

   public String getKeystorePath() {
      return keystorePath;
   }

   public SSLSupport setKeystorePath(String keystorePath) {
      this.keystorePath = keystorePath;
      return this;
   }

   public String getKeystorePassword() {
      return keystorePassword;
   }

   public SSLSupport setKeystorePassword(String keystorePassword) {
      this.keystorePassword = keystorePassword;
      return this;
   }

   public String getTruststoreProvider() {
      return truststoreProvider;
   }

   public SSLSupport setTruststoreProvider(String truststoreProvider) {
      this.truststoreProvider = truststoreProvider;
      return this;
   }

   public String getTruststorePath() {
      return truststorePath;
   }

   public SSLSupport setTruststorePath(String truststorePath) {
      this.truststorePath = truststorePath;
      return this;
   }

   public String getTruststorePassword() {
      return truststorePassword;
   }

   public SSLSupport setTruststorePassword(String truststorePassword) {
      this.truststorePassword = truststorePassword;
      return this;
   }

   public String getCrlPath() {
      return crlPath;
   }

   public SSLSupport setCrlPath(String crlPath) {
      this.crlPath = crlPath;
      return this;
   }

   public String getSslProvider() {
      return sslProvider;
   }

   public SSLSupport setSslProvider(String sslProvider) {
      this.sslProvider = sslProvider;
      return this;
   }

   public boolean isTrustAll() {
      return trustAll;
   }

   public SSLSupport setTrustAll(boolean trustAll) {
      this.trustAll = trustAll;
      return this;
   }

   public SSLContext createContext() throws Exception {
      SSLContext context = SSLContext.getInstance("TLS");
      KeyManager[] keyManagers = loadKeyManagers();
      TrustManager[] trustManagers = loadTrustManagers();
      context.init(keyManagers, trustManagers, new SecureRandom());
      return context;
   }

   public SslContext createNettyContext() throws Exception {
      KeyStore keyStore = SSLSupport.loadKeystore(keystoreProvider, keystorePath, keystorePassword);
      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, keystorePassword.toCharArray());
      return SslContextBuilder.forServer(keyManagerFactory).sslProvider(SslProvider.valueOf(sslProvider)).trustManager(loadTrustManagerFactory()).build();
   }

   public SslContext createNettyClientContext() throws Exception {
      KeyStore keyStore = SSLSupport.loadKeystore(keystoreProvider, keystorePath, keystorePassword);
      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, keystorePassword == null ? null : keystorePassword.toCharArray());
      return SslContextBuilder.forClient().sslProvider(SslProvider.valueOf(sslProvider)).keyManager(keyManagerFactory).trustManager(loadTrustManagerFactory()).build();
   }


   public static String[] parseCommaSeparatedListIntoArray(String suites) {
      String[] cipherSuites = suites.split(",");
      for (int i = 0; i < cipherSuites.length; i++) {
         cipherSuites[i] = cipherSuites[i].trim();
      }
      return cipherSuites;
   }

   public static String parseArrayIntoCommandSeparatedList(String[] suites) {
      StringBuilder supportedSuites = new StringBuilder();

      for (String suite : suites) {
         supportedSuites.append(suite);
         supportedSuites.append(", ");
      }

      // trim the last 2 characters (i.e. unnecessary comma and space)
      return supportedSuites.delete(supportedSuites.length() - 2, supportedSuites.length()).toString();
   }

   // Private -------------------------------------------------------
   private TrustManagerFactory loadTrustManagerFactory() throws Exception {
      if (trustAll) {
         //This is useful for testing but not should be used outside of that purpose
         return InsecureTrustManagerFactory.INSTANCE;
      } else if (truststorePath == null && (truststoreProvider == null || !"PKCS11".equals(truststoreProvider.toUpperCase()))) {
         return null;
      } else {
         TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
         KeyStore trustStore = SSLSupport.loadKeystore(truststoreProvider, truststorePath, truststorePassword);
         boolean ocsp = Boolean.valueOf(Security.getProperty("ocsp.enable"));

         boolean initialized = false;
         if ((ocsp || crlPath != null) && TrustManagerFactory.getDefaultAlgorithm().equalsIgnoreCase("PKIX")) {
            PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(trustStore, new X509CertSelector());
            if (crlPath != null) {
               pkixParams.setRevocationEnabled(true);
               Collection<? extends CRL> crlList = loadCRL();
               if (crlList != null) {
                  pkixParams.addCertStore(CertStore.getInstance("Collection", new CollectionCertStoreParameters(crlList)));
               }
            }
            trustMgrFactory.init(new CertPathTrustManagerParameters(pkixParams));
            initialized = true;
         }

         if (!initialized) {
            trustMgrFactory.init(trustStore);
         }
         return trustMgrFactory;
      }
   }

   private TrustManager[] loadTrustManagers() throws Exception {
      TrustManagerFactory trustManagerFactory = loadTrustManagerFactory();
      if (trustManagerFactory == null) {
         return null;
      }
      return trustManagerFactory.getTrustManagers();
   }

   private Collection<? extends CRL> loadCRL() throws Exception {
      if (crlPath == null) {
         return null;
      }
      URL resource = validateStoreURL(crlPath);
      try (InputStream is = resource.openStream()) {
         return CertificateFactory.getInstance("X.509").generateCRLs(is);
      }
   }

   private static KeyStore loadKeystore(final String keystoreProvider,
                                        final String keystorePath,
                                        final String keystorePassword) throws Exception {
      KeyStore ks = KeyStore.getInstance(keystoreProvider);
      InputStream in = null;
      try {
         if (keystorePath != null) {
            URL keystoreURL = SSLSupport.validateStoreURL(keystorePath);
            in = keystoreURL.openStream();
         }
         ks.load(in, keystorePassword == null ? null : keystorePassword.toCharArray());
      } finally {
         if (in != null) {
            try {
               in.close();
            } catch (IOException ignored) {
            }
         }
      }
      return ks;
   }

   private KeyManager[] loadKeyManagers() throws Exception {
      KeyManagerFactory factory = loadKeyManagerFactory();
      if (factory == null) {
         return null;
      }
      return factory.getKeyManagers();
   }

   private KeyManagerFactory loadKeyManagerFactory() throws Exception {
      if (keystorePath == null && (keystoreProvider == null || !"PKCS11".equals(keystoreProvider.toUpperCase()))) {
         return null;
      } else {
         KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
         KeyStore ks = SSLSupport.loadKeystore(keystoreProvider, keystorePath, keystorePassword);
         kmf.init(ks, keystorePassword == null ? null : keystorePassword.toCharArray());
         return kmf;
      }
   }

   private static URL validateStoreURL(final String storePath) throws Exception {
      assert storePath != null;

      // First see if this is a URL
      try {
         return new URL(storePath);
      } catch (MalformedURLException e) {
         File file = new File(storePath);
         if (file.exists() == true && file.isFile()) {
            return file.toURI().toURL();
         } else {
            URL url = findResource(storePath);
            if (url != null) {
               return url;
            }
         }
      }

      throw new Exception("Failed to find a store at " + storePath);
   }

   /**
    * This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    * utility class, as it would be a door to load anything you like in a safe VM.
    * For that reason any class trying to do a privileged block should do with the AccessController directly.
    */
   private static URL findResource(final String resourceName) {
      return AccessController.doPrivileged(new PrivilegedAction<URL>() {
         @Override
         public URL run() {
            return ClassloadingUtil.findResource(resourceName);
         }
      });
   }
}
