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
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A LoginModule allowing for SSL certificate based authentication based on
 * Distinguished Names (DN) stored in text files. The DNs are parsed using a
 * Properties class where each line is <user_name>=<user_DN>. This class also
 * uses a group definition file where each line is <role_name>=<user_name_1>,<user_name_2>,etc.
 * The user and role files' locations must be specified in the
 * org.apache.activemq.jaas.textfiledn.user and
 * org.apache.activemq.jaas.textfiledn.role properties respectively. NOTE: This
 * class will re-read user and group files for every authentication (i.e it does
 * live updates of allowed groups and users).
 */
public class TextFileCertificateLoginModule extends CertificateLoginModule {

   private static final String USER_FILE = "org.apache.activemq.jaas.textfiledn.user";
   private static final String ROLE_FILE = "org.apache.activemq.jaas.textfiledn.role";

   private File baseDir;
   private String usersFilePathname;
   private String rolesFilePathname;

   /**
    * Performs initialization of file paths. A standard JAAS override.
    */
   @Override
   public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
      super.initialize(subject, callbackHandler, sharedState, options);
      if (System.getProperty("java.security.auth.login.config") != null) {
         baseDir = new File(System.getProperty("java.security.auth.login.config")).getParentFile();
      }
      else {
         baseDir = new File(".");
      }

      usersFilePathname = options.get(USER_FILE) + "";
      rolesFilePathname = options.get(ROLE_FILE) + "";
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

      File usersFile = new File(baseDir, usersFilePathname);

      Properties users = new Properties();

      try {
         java.io.FileInputStream in = new java.io.FileInputStream(usersFile);
         users.load(in);
         in.close();
      }
      catch (IOException ioe) {
         throw new LoginException("Unable to load user properties file " + usersFile);
      }

      String dn = getDistinguishedName(certs);

      Enumeration<Object> keys = users.keys();
      for (Enumeration<Object> vals = users.elements(); vals.hasMoreElements(); ) {
         if (vals.nextElement().equals(dn)) {
            return (String) keys.nextElement();
         }
         else {
            keys.nextElement();
         }
      }

      return null;
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
      File rolesFile = new File(baseDir, rolesFilePathname);

      Properties roles = new Properties();
      try {
         java.io.FileInputStream in = new java.io.FileInputStream(rolesFile);
         roles.load(in);
         in.close();
      }
      catch (IOException ioe) {
         throw new LoginException("Unable to load role properties file " + rolesFile);
      }
      Set<String> userRoles = new HashSet<String>();
      for (Enumeration<Object> enumeration = roles.keys(); enumeration.hasMoreElements(); ) {
         String groupName = (String) enumeration.nextElement();
         String[] userList = (roles.getProperty(groupName) + "").split(",");
         for (int i = 0; i < userList.length; i++) {
            if (username.equals(userList[i])) {
               userRoles.add(groupName);
               break;
            }
         }
      }

      return userRoles;
   }
}
