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
package org.apache.activemq.artemis.core.security.jaas;

import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.cert.X509Certificate;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.spi.core.security.jaas.CertificateCallback;
import org.apache.activemq.artemis.spi.core.security.jaas.CertificateLoginModule;
import org.apache.activemq.artemis.spi.core.security.jaas.JaasCallbackHandler;
import org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoader;
import org.apache.activemq.artemis.spi.core.security.jaas.TextFileCertificateLoginModule;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TextFileCertificateLoginModuleTest {

   private static final Logger logger = Logger.getLogger(TextFileCertificateLoginModuleTest.class);

   private static final String CERT_USERS_FILE_SMALL = "cert-users-SMALL.properties";
   private static final String CERT_USERS_FILE_LARGE = "cert-users-LARGE.properties";
   private static final String CERT_GROUPS_FILE = "cert-roles.properties";

   private static final int NUMBER_SUBJECTS = 10;

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = TextFileCertificateLoginModuleTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            try {
               path = URLDecoder.decode(resource.getFile(), StandardCharsets.UTF_8.name());
               System.setProperty("java.security.auth.login.config", path);
            } catch (UnsupportedEncodingException e) {
               logger.error(e.getMessage(), e);
               throw new RuntimeException(e);
            }
         }
      }
   }

   private CertificateLoginModule loginModule;

   @Before
   public void setUp() throws Exception {
      loginModule = new TextFileCertificateLoginModule();
   }

   @After
   public void tearDown() throws Exception {
      PropertiesLoader.resetUsersAndGroupsCache();
   }

   @Test
   public void testLoginWithSMALLUsersFile() throws Exception {
      loginTest(CERT_USERS_FILE_SMALL, CERT_GROUPS_FILE);
   }

   @Test
   public void testLoginWithLARGEUsersFile() throws Exception {
      loginTest(CERT_USERS_FILE_LARGE, CERT_GROUPS_FILE);
   }

   private void loginTest(String usersFiles, String groupsFile) throws LoginException {

      HashMap<String, String> options = new HashMap<>();
      options.put("org.apache.activemq.jaas.textfiledn.user", usersFiles);
      options.put("org.apache.activemq.jaas.textfiledn.role", groupsFile);
      options.put("reload", "true");

      JaasCallbackHandler[] callbackHandlers = new JaasCallbackHandler[NUMBER_SUBJECTS];
      Subject[] subjects = new Subject[NUMBER_SUBJECTS];

      for (int i = 0; i < callbackHandlers.length; i++) {
         callbackHandlers[i] = getJaasCertificateCallbackHandler("DN=TEST_USER_" + (i + 1));
      }

      long startTime = System.currentTimeMillis();

      for (int outer = 0; outer < 500; outer++) {
         for (int i = 0; i < NUMBER_SUBJECTS; i++) {
            Subject subject = doAuthenticate(options, callbackHandlers[i]);
            subjects[i] = subject;
         }
      }

      long endTime = System.currentTimeMillis();
      long timeTaken = endTime - startTime;

      for (int i = 0; i < NUMBER_SUBJECTS; i++) {
         ActiveMQServerLogger.LOGGER.info("subject is: " + subjects[i].getPrincipals().toString());
      }

      ActiveMQServerLogger.LOGGER.info(usersFiles + ": Time taken is " + timeTaken);

   }

   private JaasCallbackHandler getJaasCertificateCallbackHandler(String user) {
      JMXPrincipal principal = new JMXPrincipal(user);
      X509Certificate cert = new StubX509Certificate(principal);
      return new JaasCallbackHandler(null, null, null) {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
               if (callback instanceof CertificateCallback) {
                  CertificateCallback certCallback = (CertificateCallback) callback;
                  certCallback.setCertificates(new X509Certificate[]{cert});
               } else {
                  throw new UnsupportedCallbackException(callback);
               }
            }
         }
      };
   }

   private Subject doAuthenticate(HashMap<String, ?> options,
                                  JaasCallbackHandler callbackHandler) throws LoginException {
      Subject mySubject = new Subject();
      loginModule.initialize(mySubject, callbackHandler, null, options);
      loginModule.login();
      loginModule.commit();
      return mySubject;
   }
}
