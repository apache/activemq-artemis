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

package org.apache.activemq.api.core;

import static org.apache.activemq.api.core.HornetQExceptionType.INTERNAL_ERROR;

/**
 * Internal error which prevented HornetQ from performing an important operation.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 4/30/12
 */
public final class HornetQInternalErrorException extends HornetQException
{
   private static final long serialVersionUID = -5987814047521530695L;

   public HornetQInternalErrorException()
   {
      super(INTERNAL_ERROR);
   }

   public HornetQInternalErrorException(String msg)
   {
      super(INTERNAL_ERROR, msg);
   }

   public HornetQInternalErrorException(String message, Exception e)
   {
      super(INTERNAL_ERROR, message, e);
   }

   public HornetQInternalErrorException(String message, Throwable t)
   {
      super(INTERNAL_ERROR, message, t);
   }
}
