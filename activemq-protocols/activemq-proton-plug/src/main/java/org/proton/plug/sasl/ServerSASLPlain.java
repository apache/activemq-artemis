/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.proton.plug.sasl;

import org.proton.plug.SASLResult;
import org.proton.plug.ServerSASL;

/**
 * @author Clebert Suconic
 */

public class ServerSASLPlain implements ServerSASL
{
   public static final String NAME = "PLAIN";

   @Override
   public String getName()
   {
      return NAME;
   }

   @Override
   public SASLResult processSASL(byte[] data)
   {

      String username = null;
      String password = null;
      String bytes = new String(data);
      String[] credentials = bytes.split(Character.toString((char) 0));
      int offSet = 0;
      if (credentials.length > 0)
      {
         if (credentials[0].length() == 0)
         {
            offSet = 1;
         }

         if (credentials.length >= offSet)
         {
            username = credentials[offSet];
         }
         if (credentials.length >= (offSet + 1))
         {
            password = credentials[offSet + 1];
         }
      }

      boolean success = authenticate(username, password);

      return new PlainSASLResult(success, username, password);
   }


   /**
    * Hook for subclasses to perform the authentication here
    *
    * @param user
    * @param password
    */
   protected boolean authenticate(String user, String password)
   {
      return true;
   }
}
