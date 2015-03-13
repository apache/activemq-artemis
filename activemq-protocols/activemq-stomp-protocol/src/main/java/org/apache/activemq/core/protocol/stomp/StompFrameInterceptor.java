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
package org.apache.activemq.core.protocol.stomp;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.spi.core.protocol.RemotingConnection;

/**
 * This class is a simple way to intercepting client calls on ActiveMQ using STOMP protocol.
 * <p>
 * To add an interceptor to ActiveMQ server, you have to modify the server configuration file
 * {@literal activemq-configuration.xml}.<br>
 */
public abstract class StompFrameInterceptor implements Interceptor
{

   /**
    * Intercepts a packet which is received before it is sent to the channel.
    * By default does not do anything and returns true allowing other interceptors perform logic.
    *
    * @param packet     the packet being received
    * @param connection the connection the packet was received on
    * @return {@code true} to process the next interceptor and handle the packet,
    * {@code false} to abort processing of the packet
    * @throws ActiveMQException
    */
   @Override
   public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException
   {
      return true;
   }

  /**
   * Intercepts a stomp frame sent by a client.
   *
   * @param stompFrame     the stomp frame being received
   * @param connection the connection the stomp frame was received on
   * @return {@code true} to process the next interceptor and handle the stomp frame,
   * {@code false} to abort processing of the stomp frame
   */
   public abstract boolean intercept(StompFrame stompFrame, RemotingConnection connection);
}
