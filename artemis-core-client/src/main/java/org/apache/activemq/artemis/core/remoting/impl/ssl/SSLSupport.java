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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

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
      SSLContext context = SSLContext.getInstance("TLS");
      KeyManager[] keyManagers = SSLSupport.loadKeyManagers(keystoreProvider, keystorePath, keystorePassword);
      TrustManager[] trustManagers = SSLSupport.loadTrustManager(trustStoreProvider, trustStorePath, trustStorePassword);
      context.init(keyManagers, trustManagers, new SecureRandom());
      return context;
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

      for (int i = 0; i < suites.length; i++) {
         supportedSuites.append(suites[i]);
         supportedSuites.append(", ");
      }

      // trim the last 2 characters (i.e. unnecessary comma and space)
      return supportedSuites.delete(supportedSuites.length() - 2, supportedSuites.length()).toString();
   }

   // Private -------------------------------------------------------

   private static TrustManager[] loadTrustManager(final String trustStoreProvider,
                                                  final String trustStorePath,
                                                  final String trustStorePassword) throws Exception {
      if (trustStorePath == null && (trustStoreProvider == null || !"PKCS11".equals(trustStoreProvider.toUpperCase()))) {
         return null;
      }
      else {
         TrustManagerFactory trustMgrFactory;
         KeyStore trustStore = SSLSupport.loadKeystore(trustStoreProvider, trustStorePath, trustStorePassword);
         trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
         trustMgrFactory.init(trustStore);
         return trustMgrFactory.getTrustManagers();
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
      }
      finally {
         if (in != null) {
            try {
               in.close();
            }
            catch (IOException ignored) {
            }
         }
      }
      return ks;
   }

   private static KeyManager[] loadKeyManagers(final String keyStoreProvider,
                                               final String keystorePath,
                                               final String keystorePassword) throws Exception {
      if (keystorePath == null && (keyStoreProvider == null || !"PKCS11".equals(keyStoreProvider.toUpperCase()))) {
         return null;
      }
      else {
         KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
         KeyStore ks = SSLSupport.loadKeystore(keyStoreProvider, keystorePath, keystorePassword);
         kmf.init(ks, keystorePassword == null ? null : keystorePassword.toCharArray());

         return kmf.getKeyManagers();
      }
   }

   private static URL validateStoreURL(final String storePath) throws Exception {
      assert storePath != null;

      // First see if this is a URL
      try {
         return new URL(storePath);
      }
      catch (MalformedURLException e) {
         File file = new File(storePath);
         if (file.exists() == true && file.isFile()) {
            return file.toURI().toURL();
         }
         else {
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
