/*
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
package org.apache.activemq.artemis.api.core;

/**
 * A client operation failed because the calling resource (ClientSession, ClientProducer, etc.) is
 * closed.
 */
public final class ActiveMQObjectClosedException extends ActiveMQException {

   private static final long serialVersionUID = 809024052184914812L;

   public ActiveMQObjectClosedException() {
      super(ActiveMQExceptionType.OBJECT_CLOSED);
   }

   public ActiveMQObjectClosedException(String msg) {
      super(ActiveMQExceptionType.OBJECT_CLOSED, msg);
   }
}
