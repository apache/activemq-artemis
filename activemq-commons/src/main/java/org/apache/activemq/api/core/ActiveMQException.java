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

/**
 * ActiveMQException is the root exception for the ActiveMQ API.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ActiveMQException extends Exception
{
   private static final long serialVersionUID = -4802014152804997417L;

   private final ActiveMQExceptionType type;

   public ActiveMQException()
   {
      type = ActiveMQExceptionType.GENERIC_EXCEPTION;
   }

   public ActiveMQException(final String msg)
   {
      super(msg);
      type = ActiveMQExceptionType.GENERIC_EXCEPTION;
   }

   /*
   * This constructor is needed only for the native layer
   */
   public ActiveMQException(int code, String msg)
   {
      super(msg);

      this.type = ActiveMQExceptionType.getType(code);
   }

   public ActiveMQException(ActiveMQExceptionType type, String msg)
   {
      super(msg);

      this.type = type;
   }

   public ActiveMQException(ActiveMQExceptionType type)
   {
      this.type = type;
   }

   public ActiveMQException(ActiveMQExceptionType type, String message, Throwable t)
   {
      super(message, t);
      this.type = type;
   }

   public ActiveMQExceptionType getType()
   {
      return type;
   }

   @Override
   public String toString()
   {
      return this.getClass().getSimpleName() + "[errorType=" + type + " message=" + getMessage() + "]";
   }

}
