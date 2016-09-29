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
package org.apache.activemq.artemis.ra;

import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.SecurityException;
import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;

/**
 * Credential information
 */
public class ActiveMQRACredential implements Serializable {

   /**
    * Serial version UID
    */
   static final long serialVersionUID = 210476602237497193L;

   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * The user name
    */
   private String userName;

   /**
    * The password
    */
   private String password;

   /**
    * Private constructor
    */
   private ActiveMQRACredential() {
      if (ActiveMQRACredential.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor()");
      }
   }

   /**
    * Get the user name
    *
    * @return The value
    */
   public String getUserName() {
      if (ActiveMQRACredential.trace) {
         ActiveMQRALogger.LOGGER.trace("getUserName()");
      }

      return userName;
   }

   /**
    * Set the user name
    *
    * @param userName The value
    */
   private void setUserName(final String userName) {
      if (ActiveMQRACredential.trace) {
         ActiveMQRALogger.LOGGER.trace("setUserName(" + userName + ")");
      }

      this.userName = userName;
   }

   /**
    * Get the password
    *
    * @return The value
    */
   public String getPassword() {
      if (ActiveMQRACredential.trace) {
         ActiveMQRALogger.LOGGER.trace("getPassword()");
      }

      return password;
   }

   /**
    * Set the password
    *
    * @param password The value
    */
   private void setPassword(final String password) {
      if (ActiveMQRACredential.trace) {
         ActiveMQRALogger.LOGGER.trace("setPassword(****)");
      }

      this.password = password;
   }

   /**
    * Get credentials
    *
    * @param mcf     The managed connection factory
    * @param subject The subject
    * @param info    The connection request info
    * @return The credentials
    * @throws SecurityException Thrown if the credentials can't be retrieved
    */
   public static ActiveMQRACredential getCredential(final ManagedConnectionFactory mcf,
                                                    final Subject subject,
                                                    final ConnectionRequestInfo info) throws SecurityException {
      if (ActiveMQRACredential.trace) {
         ActiveMQRALogger.LOGGER.trace("getCredential(" + mcf + ", " + subject + ", " + info + ")");
      }

      ActiveMQRACredential jc = new ActiveMQRACredential();
      if (subject == null && info != null) {
         jc.setUserName(((ActiveMQRAConnectionRequestInfo) info).getUserName());
         jc.setPassword(((ActiveMQRAConnectionRequestInfo) info).getPassword());
      } else if (subject != null) {
         PasswordCredential pwdc = GetCredentialAction.getCredential(subject, mcf);

         if (pwdc == null) {
            throw new SecurityException("No password credentials found");
         }

         jc.setUserName(pwdc.getUserName());
         jc.setPassword(new String(pwdc.getPassword()));
      } else {
         throw new SecurityException("No Subject or ConnectionRequestInfo set, could not get credentials");
      }

      return jc;
   }

   /**
    * String representation
    *
    * @return The representation
    */
   @Override
   public String toString() {
      if (ActiveMQRACredential.trace) {
         ActiveMQRALogger.LOGGER.trace("toString()");
      }

      return super.toString() + "{ username=" + userName + ", password=**** }";
   }

   /**
    * Privileged class to get credentials
    */
   private static class GetCredentialAction implements PrivilegedAction<PasswordCredential> {

      /**
       * The subject
       */
      private final Subject subject;

      /**
       * The managed connection factory
       */
      private final ManagedConnectionFactory mcf;

      /**
       * Constructor
       *
       * @param subject The subject
       * @param mcf     The managed connection factory
       */
      GetCredentialAction(final Subject subject, final ManagedConnectionFactory mcf) {
         if (ActiveMQRACredential.trace) {
            ActiveMQRALogger.LOGGER.trace("constructor(" + subject + ", " + mcf + ")");
         }

         this.subject = subject;
         this.mcf = mcf;
      }

      /**
       * Run
       *
       * @return The credential
       */
      @Override
      public PasswordCredential run() {
         if (ActiveMQRACredential.trace) {
            ActiveMQRALogger.LOGGER.trace("run()");
         }

         Set<PasswordCredential> creds = subject.getPrivateCredentials(PasswordCredential.class);
         PasswordCredential pwdc = null;

         for (PasswordCredential curCred : creds) {
            if (curCred.getManagedConnectionFactory().equals(mcf)) {
               pwdc = curCred;
               break;
            }
         }
         return pwdc;
      }

      /**
       * Get credentials
       *
       * @param subject The subject
       * @param mcf     The managed connection factory
       * @return The credential
       */
      static PasswordCredential getCredential(final Subject subject, final ManagedConnectionFactory mcf) {
         if (ActiveMQRACredential.trace) {
            ActiveMQRALogger.LOGGER.trace("getCredential(" + subject + ", " + mcf + ")");
         }

         GetCredentialAction action = new GetCredentialAction(subject, mcf);
         return AccessController.doPrivileged(action);
      }
   }
}
