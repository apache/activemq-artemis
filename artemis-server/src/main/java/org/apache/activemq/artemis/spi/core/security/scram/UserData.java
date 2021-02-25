/*
 * Copyright 2016 Ognyan Bankov
 * <p>
 * All rights reserved. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.spi.core.security.scram;

/**
 * Wrapper for user data needed for the SCRAM authentication
 */
public class UserData {
   /**
    * Salt
    */
   public final String salt;
   /**
    * Iterations used to salt the password
    */
   public final int iterations;
   /**
    * Server key
    */
   public final String serverKey;
   /**
    * Stored key
    */
   public final String storedKey;

   /**
    * Creates new UserData
    * @param salt Salt
    * @param iterations Iterations for salting
    * @param serverKey Server key
    * @param storedKey Stored key
    */
   public UserData(String salt, int iterations, String serverKey, String storedKey) {
      this.salt = salt;
      this.iterations = iterations;
      this.serverKey = serverKey;
      this.storedKey = storedKey;
   }
}
