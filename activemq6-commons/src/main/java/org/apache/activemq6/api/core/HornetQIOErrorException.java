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

package org.apache.activemq6.api.core;

import static org.apache.activemq6.api.core.HornetQExceptionType.IO_ERROR;

/**
 * Unexpected I/O error occurred on the server.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 4/30/12
 */
public final class HornetQIOErrorException extends HornetQException
{
   private static final long serialVersionUID = 797277117077787396L;

   public HornetQIOErrorException()
   {
      super(IO_ERROR);
   }

   public HornetQIOErrorException(String msg)
   {
      super(IO_ERROR, msg);
   }

   public HornetQIOErrorException(String msg, Throwable cause)
   {
      super(IO_ERROR, msg, cause);
   }
}
