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
 * An outgoing interceptor returned false.
 * See org.apache.activemq.artemis.api.core.client.ServerLocator#addOutgoingInterceptor(org.apache.activemq.artemis.api.core.Interceptor)
 */
// XXX I doubt any reader will make much sense of this Javadoc's text.
public final class ActiveMQInterceptorRejectedPacketException extends ActiveMQException {

   private static final long serialVersionUID = -5798841227645281815L;

   public ActiveMQInterceptorRejectedPacketException() {
      super(ActiveMQExceptionType.INTERCEPTOR_REJECTED_PACKET);
   }

   public ActiveMQInterceptorRejectedPacketException(String msg) {
      super(ActiveMQExceptionType.INTERCEPTOR_REJECTED_PACKET, msg);
   }
}
