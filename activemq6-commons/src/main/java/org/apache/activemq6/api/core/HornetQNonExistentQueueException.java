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

import static org.apache.activemq6.api.core.HornetQExceptionType.QUEUE_DOES_NOT_EXIST;

/**
 * An operation failed because a queue does not exist on the server.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 4/30/12
 */
public final class HornetQNonExistentQueueException extends HornetQException
{
   private static final long serialVersionUID = -8199298881947523607L;

   public HornetQNonExistentQueueException()
   {
      super(QUEUE_DOES_NOT_EXIST);
   }

   public HornetQNonExistentQueueException(String msg)
   {
      super(QUEUE_DOES_NOT_EXIST, msg);
   }
}
