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
package org.proton.plug.context.server;

import org.proton.plug.AMQPConnectionContextFactory;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPServerConnectionContext;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_IDLE_TIMEOUT;
import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_CHANNEL_MAX;
import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_MAX_FRAME_SIZE;

public class ProtonServerConnectionContextFactory extends AMQPConnectionContextFactory {

   private static final ProtonServerConnectionContextFactory theInstance = new ProtonServerConnectionContextFactory();

   public static ProtonServerConnectionContextFactory getFactory() {
      return theInstance;
   }

   @Override
   public AMQPServerConnectionContext createConnection(AMQPConnectionCallback connectionCallback, Executor dispatchExecutor, ScheduledExecutorService scheduledPool) {
      return createConnection(connectionCallback, null, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_FRAME_SIZE, DEFAULT_CHANNEL_MAX, dispatchExecutor, scheduledPool);
   }

   @Override
   public AMQPServerConnectionContext createConnection(AMQPConnectionCallback connectionCallback,
                                                       String containerId,
                                                       int idleTimeout,
                                                       int maxFrameSize,
                                                       int channelMax,
                                                       Executor dispatchExecutor,
                                                       ScheduledExecutorService scheduledPool) {
      return new ProtonServerConnectionContext(connectionCallback, containerId, idleTimeout, maxFrameSize, channelMax, dispatchExecutor, scheduledPool);
   }
}
