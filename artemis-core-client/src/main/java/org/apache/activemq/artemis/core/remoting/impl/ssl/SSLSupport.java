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
import java.security.PrivateKey;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CRL;
import java.security.cert.CertStore;
import java.security.cert.CertificateFactory;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Collection;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.activemq.artemis.utils.ClassloadingUtil;

/**
 * Please note, this class supports PKCS#11 keystores, but there are no specific tests in the ActiveMQ Artemis test-suite to
 * validate/verify this works because this requires a functioning PKCS#11 provider which is not available by default
 * (see java.security.Security#getProviders()).  The main thing to keep in mind is that PKCS#11 keystores will have a
 * null keystore path.
 */
public class SSLSupport {
   // Public --------------------------------------------------------

   public static SSLContext createContext(final String keystoreProvider,
                                          final String keystorePath,
                                          final String keystorePassword,
                                          final String trustStoreProvider,
                                          final String trustStorePath,
                                          final String trustStorePassword) throws Exception {

      return SSLSupport.createContext(keystoreProvider, keystorePath, keystorePassword, trustStoreProvider, trustStorePath, trustStorePassword, false, null);
   }

   public static SSLContext createContext(final String keystoreProvider,
                                          final String keystorePath,
                                          final String keystorePassword,
                                          final String trustStoreProvider,
                                          final String trustStorePath,
                                          final String trustStorePassword,
                                          final String crlPath) throws Exception {

      return SSLSupport.createContext(keystoreProvider, keystorePath, keystorePassword, trustStoreProvider, trustStorePath, trustStorePassword, false, crlPath);
   }

   public static SSLContext createContext(final String keystoreProvider,
                                          final String keystorePath,
                                          final String keystorePassword,
                                          final String trustStoreProvider,
                                          final String trustStorePath,
                                          final String trustStorePassword,
                                          final boolean trustAll) throws Exception {
      return SSLSupport.createContext(keystoreProvider, keystorePath, keystorePassword, trustStoreProvider, trustStorePath, trustStorePassword, trustAll, null);
   }

   public static SSLContext createContext(final String keystoreProvider,
                                          final String keystorePath,
                                          final String keystorePassword,
                                          final String trustStoreProvider,
                                          final String trustStorePath,
                                          final String trustStorePassword,
                                          final boolean trustAll,
                                          final String crlPath) throws Exception {
      SSLContext context = SSLContext.getInstance("TLS");
      KeyManager[] keyManagers = SSLSupport.loadKeyManagers(keystoreProvider, keystorePath, keystorePassword);
      TrustManager[] trustManagers = SSLSupport.loadTrustManager(trustStoreProvider, trustStorePath, trustStorePassword, trustAll, crlPath);
      context.init(keyManagers, trustManagers, new SecureRandom());
      return context;
   }

   public static SslContext createNettyContext(final String keystoreProvider,
                                               final String keystorePath,
                                               final String keystorePassword,
                                               final String trustStoreProvider,
                                               final String trustStorePath,
                                               final String trustStorePassword,
                                               final String sslProvider) throws Exception {

      KeyStore keyStore = SSLSupport.loadKeystore(keystoreProvider, keystorePath, keystorePassword);
      String alias = keyStore.aliases().nextElement();
      PrivateKey privateKey = (PrivateKey) keyStore.getKey(alias, keystorePassword.toCharArray());
      X509Certificate certificate = (X509Certificate) keyStore.getCertificate(alias);
      return SslContextBuilder.forServer(privateKey, certificate).sslProvider(SslProvider.valueOf(sslProvider)).trustManager(SSLSupport.loadTrustManagerFactory(trustStoreProvider, trustStorePath, trustStorePassword, false, null)).build();
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
   private static TrustManagerFactory loadTrustManagerFactory(final String trustStoreProvider,
                                                              final String trustStorePath,
                                                              final String trustStorePassword,
                                                              final boolean trustAll,
                                                              final String crlPath) throws Exception {
      if (trustAll) {
         //This is useful for testing but not should be used outside of that purpose
         return InsecureTrustManagerFactory.INSTANCE;
      } else if (trustStorePath == null && (trustStoreProvider == null || !"PKCS11".equals(trustStoreProvider.toUpperCase()))) {
         return null;
      } else {
         TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
         KeyStore trustStore = SSLSupport.loadKeystore(trustStoreProvider, trustStorePath, trustStorePassword);
         boolean ocsp = Boolean.valueOf(Security.getProperty("ocsp.enable"));

         boolean initialized = false;
         if ((ocsp || crlPath != null) && TrustManagerFactory.getDefaultAlgorithm().equalsIgnoreCase("PKIX")) {
            PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(trustStore, new X509CertSelector());
            if (crlPath != null) {
               pkixParams.setRevocationEnabled(true);
               Collection<? extends CRL> crlList = loadCRL(crlPath);
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

   private static TrustManager[] loadTrustManager(final String trustStoreProvider,
                                                  final String trustStorePath,
                                                  final String trustStorePassword,
                                                  final boolean trustAll,
                                                  final String crlPath) throws Exception {
      TrustManagerFactory trustManagerFactory = loadTrustManagerFactory(trustStoreProvider, trustStorePath, trustStorePassword, trustAll, crlPath);
      if (trustManagerFactory == null) {
         return null;
      }
      return trustManagerFactory.getTrustManagers();
   }

   private static Collection<? extends CRL> loadCRL(String crlPath) throws Exception {
      if (crlPath == null) {
         return null;
      }

      URL resource = SSLSupport.validateStoreURL(crlPath);

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

   private static KeyManager[] loadKeyManagers(final String keyStoreProvider,
                                               final String keystorePath,
                                               final String keystorePassword) throws Exception {

      KeyManagerFactory factory = loadKeyManagerFactory(keyStoreProvider, keystorePath, keystorePassword);
      if (factory == null) {
         return null;
      }
      return factory.getKeyManagers();
   }

   private static KeyManagerFactory loadKeyManagerFactory(final String keyStoreProvider,
                                                          final String keystorePath,
                                                          final String keystorePassword) throws Exception {
      if (keystorePath == null && (keyStoreProvider == null || !"PKCS11".equals(keyStoreProvider.toUpperCase()))) {
         return null;
      } else {
         KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
         KeyStore ks = SSLSupport.loadKeystore(keyStoreProvider, keystorePath, keystorePassword);
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
