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
package org.apache.activemq.spi.core.protocol;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.server.ServerConsumer;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.spi.core.remoting.ReadyListener;

/**
 * A SessionCallback
 */
public interface SessionCallback
{
   /** This one gives a chance for Proton to have its own flow control. */
   boolean hasCredits(ServerConsumer consumerID);

   void sendProducerCreditsMessage(int credits, SimpleString address);

   void sendProducerCreditsFailMessage(int credits, SimpleString address);

   int sendMessage(ServerMessage message, ServerConsumer consumerID, int deliveryCount);

   int sendLargeMessage(ServerMessage message, ServerConsumer consumerID, long bodySize, int deliveryCount);

   int sendLargeMessageContinuation(ServerConsumer consumerID, byte[] body, boolean continues, boolean requiresResponse);

   void closed();

   void addReadyListener(ReadyListener listener);

   void removeReadyListener(ReadyListener listener);

   void disconnect(ServerConsumer consumerId, String queueName);
}
