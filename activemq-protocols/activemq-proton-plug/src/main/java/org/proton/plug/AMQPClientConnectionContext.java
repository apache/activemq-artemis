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
package org.proton.plug;

import org.proton.plug.exceptions.ActiveMQAMQPException;

/**
 * This is valid only on a client connection.
 *
 * @author Clebert Suconic
 */

public interface AMQPClientConnectionContext extends AMQPConnectionContext
{
   /**
    * This will send an open and block for its return on AMQP protocol.
    *
    * @throws Exception
    */
   void clientOpen(ClientSASL sasl) throws Exception;

   AMQPClientSessionContext createClientSession() throws ActiveMQAMQPException;
}
