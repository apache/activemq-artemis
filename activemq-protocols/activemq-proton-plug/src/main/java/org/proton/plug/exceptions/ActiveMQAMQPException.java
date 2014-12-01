/**
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
package org.proton.plug.exceptions;

import org.apache.qpid.proton.amqp.Symbol;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         6/6/13
 */
public class ActiveMQAMQPException extends Exception
{

   private static final String ERROR_PREFIX = "amqp:";

   public Symbol getAmqpError()
   {
      return amqpError;
   }

   private final Symbol amqpError;

   public ActiveMQAMQPException(Symbol amqpError, String message, Throwable e)
   {
      super(message, e);
      this.amqpError = amqpError;
   }

   public ActiveMQAMQPException(Symbol amqpError, String message)
   {
      super(message);
      this.amqpError = amqpError;
   }
}
