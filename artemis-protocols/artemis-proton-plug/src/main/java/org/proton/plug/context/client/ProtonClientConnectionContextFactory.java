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
package org.proton.plug.context.client;

import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPConnectionContextFactory;
import org.proton.plug.AMQPConnectionCallback;

public class ProtonClientConnectionContextFactory extends AMQPConnectionContextFactory {

   private static final AMQPConnectionContextFactory theInstance = new ProtonClientConnectionContextFactory();

   public static AMQPConnectionContextFactory getFactory() {
      return theInstance;
   }

   @Override
   public AMQPConnectionContext createConnection(AMQPConnectionCallback connectionCallback) {
      return new ProtonClientConnectionContext(connectionCallback);
   }

   @Override
   public AMQPConnectionContext createConnection(AMQPConnectionCallback connectionCallback,
                                                 int idleTimeout,
                                                 int maxFrameSize,
                                                 int channelMax) {
      return new ProtonClientConnectionContext(connectionCallback, idleTimeout, maxFrameSize, channelMax);
   }
}
