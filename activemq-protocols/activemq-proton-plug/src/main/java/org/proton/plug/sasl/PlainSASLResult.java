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

/**
 * @author Clebert Suconic
 */

public class PlainSASLResult implements SASLResult
{
   private boolean success;
   private String user;
   private String password;

   public PlainSASLResult(boolean success, String user, String password)
   {
      this.success = success;
      this.user = user;
      this.password = password;
   }

   @Override
   public String getUser()
   {
      return user;
   }

   public String getPassword()
   {
      return password;
   }

   @Override
   public boolean isSuccess()
   {
      return success;
   }
}
