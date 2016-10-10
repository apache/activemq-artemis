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
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A LoginModule allowing for SSL certificate based authentication based on
 * Distinguished Names (DN) stored in text files. The DNs are parsed using a
 * Properties class where each line is &lt;user_name&gt;=&lt;user_DN&gt;. This class also
 * uses a group definition file where each line is &lt;role_name&gt;=&lt;user_name_1&gt;,&lt;user_name_2&gt;,etc.
 * The user and role files' locations must be specified in the
 * org.apache.activemq.jaas.textfiledn.user and
 * org.apache.activemq.jaas.textfiledn.role properties respectively. NOTE: This
 * class will re-read user and group files if they have been modified and the "reload"
 * option is true
 */
public class TextFileCertificateLoginModule extends CertificateLoginModule {

   private static final String USER_FILE_PROP_NAME = "org.apache.activemq.jaas.textfiledn.user";
   private static final String ROLE_FILE_PROP_NAME = "org.apache.activemq.jaas.textfiledn.role";

   private Map<String, Set<String>> rolesByUser;
   private Map<String, String> usersByDn;

   /**
    * Performs initialization of file paths. A standard JAAS override.
    */
   @Override
   public void initialize(Subject subject,
                          CallbackHandler callbackHandler,
                          Map<String, ?> sharedState,
                          Map<String, ?> options) {
      super.initialize(subject, callbackHandler, sharedState, options);
      usersByDn = load(USER_FILE_PROP_NAME, "", options).invertedPropertiesMap();
      rolesByUser = load(ROLE_FILE_PROP_NAME, "", options).invertedPropertiesValuesMap();
   }

   /**
    * Overriding to allow DN authorization based on DNs specified in text
    * files.
    *
    * @param certs The certificate the incoming connection provided.
    * @return The user's authenticated name or null if unable to authenticate
    * the user.
    * @throws LoginException Thrown if unable to find user file or connection
    *                        certificate.
    */
   @Override
   protected String getUserNameForCertificates(final X509Certificate[] certs) throws LoginException {
      if (certs == null) {
         throw new LoginException("Client certificates not found. Cannot authenticate.");
      }

      return usersByDn.get(getDistinguishedName(certs));
   }

   /**
    * Overriding to allow for role discovery based on text files.
    *
    * @param username The name of the user being examined. This is the same
    *                 name returned by getUserNameForCertificates.
    * @return A Set of name Strings for roles this user belongs to.
    * @throws LoginException Thrown if unable to find role definition file.
    */
   @Override
   protected Set<String> getUserRoles(String username) throws LoginException {
      Set<String> userRoles = rolesByUser.get(username);
      if (userRoles == null) {
         userRoles = Collections.emptySet();
      }

      return userRoles;
   }
}
