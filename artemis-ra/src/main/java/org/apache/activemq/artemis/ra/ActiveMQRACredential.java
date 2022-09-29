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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * Credential information
 */
public class ActiveMQRACredential implements Serializable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   /**
    * Serial version UID
    */
   static final long serialVersionUID = 210476602237497193L;

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
      if (logger.isTraceEnabled()) {
         logger.trace("constructor()");
      }
   }

   /**
    * Get the user name
    *
    * @return The value
    */
   public String getUserName() {
      if (logger.isTraceEnabled()) {
         logger.trace("getUserName()");
      }

      return userName;
   }

   /**
    * Set the user name
    *
    * @param userName The value
    */
   private void setUserName(final String userName) {
      if (logger.isTraceEnabled()) {
         logger.trace("setUserName(" + userName + ")");
      }

      this.userName = userName;
   }

   /**
    * Get the password
    *
    * @return The value
    */
   public String getPassword() {
      if (logger.isTraceEnabled()) {
         logger.trace("getPassword()");
      }

      return password;
   }

   /**
    * Set the password
    *
    * @param password The value
    */
   private void setPassword(final String password) {
      if (logger.isTraceEnabled()) {
         logger.trace("setPassword(****)");
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
      if (logger.isTraceEnabled()) {
         logger.trace("getCredential(" + mcf + ", " + subject + ", " + info + ")");
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
      if (logger.isTraceEnabled()) {
         logger.trace("toString()");
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
         if (logger.isTraceEnabled()) {
            logger.trace("constructor(" + subject + ", " + mcf + ")");
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
         if (logger.isTraceEnabled()) {
            logger.trace("run()");
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
         if (logger.isTraceEnabled()) {
            logger.trace("getCredential(" + subject + ", " + mcf + ")");
         }

         GetCredentialAction action = new GetCredentialAction(subject, mcf);
         return AccessController.doPrivileged(action);
      }
   }
}
