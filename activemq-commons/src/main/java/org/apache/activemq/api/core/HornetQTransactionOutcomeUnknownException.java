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

import static org.apache.activemq.api.core.HornetQExceptionType.TRANSACTION_OUTCOME_UNKNOWN;

/**
 * The outcome of a transaction is unknown.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 5/2/12
 */
public final class HornetQTransactionOutcomeUnknownException extends HornetQException
{
   private static final long serialVersionUID = 7940794286427650558L;

   public HornetQTransactionOutcomeUnknownException()
   {
      super(TRANSACTION_OUTCOME_UNKNOWN);
   }

   public HornetQTransactionOutcomeUnknownException(String msg)
   {
      super(TRANSACTION_OUTCOME_UNKNOWN, msg);
   }
}
