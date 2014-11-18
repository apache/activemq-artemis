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
 * An error has happened at ActiveMQ's native (non-Java) code used in reading and writing data.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 5/4/12
 */
// XXX
public final class ActiveMQNativeIOError extends ActiveMQException
{
   private static final long serialVersionUID = 2355120980683293085L;

   public ActiveMQNativeIOError()
   {
      super(ActiveMQExceptionType.NATIVE_ERROR_CANT_INITIALIZE_AIO);
   }

   public ActiveMQNativeIOError(String msg)
   {
      super(ActiveMQExceptionType.NATIVE_ERROR_CANT_INITIALIZE_AIO, msg);
   }
}
