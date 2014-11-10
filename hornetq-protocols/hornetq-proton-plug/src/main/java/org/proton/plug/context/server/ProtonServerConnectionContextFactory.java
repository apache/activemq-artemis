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

package org.proton.plug.context.server;

import org.proton.plug.AMQPConnectionContextFactory;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPServerConnectionContext;

/**
 * @author Clebert Suconic
 */

public class ProtonServerConnectionContextFactory extends AMQPConnectionContextFactory
{
   private static final ProtonServerConnectionContextFactory theInstance = new ProtonServerConnectionContextFactory();

   public static ProtonServerConnectionContextFactory getFactory()
   {
      return theInstance;
   }

   public AMQPServerConnectionContext createConnection(AMQPConnectionCallback connectionCallback)
   {
      ProtonServerConnectionContext connection = new ProtonServerConnectionContext(connectionCallback);
      return connection;
   }
}
