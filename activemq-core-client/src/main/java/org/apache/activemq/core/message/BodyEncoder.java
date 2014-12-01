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
package org.apache.activemq.core.message;

import java.nio.ByteBuffer;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQException;

/**
 * Class used to encode message body into buffers.
 * <br>
 * Used to send large streams over the wire
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface BodyEncoder
{
   /**
    * This method must not be called directly by ActiveMQ clients.
    */
   void open() throws ActiveMQException;

   /**
    * This method must not be called directly by ActiveMQ clients.
    */
   void close() throws ActiveMQException;

   /**
    * This method must not be called directly by ActiveMQ clients.
    */
   int encode(ByteBuffer bufferRead) throws ActiveMQException;

   /**
    * This method must not be called directly by ActiveMQ clients.
    */
   int encode(ActiveMQBuffer bufferOut, int size) throws ActiveMQException;

   /**
    * This method must not be called directly by ActiveMQ clients.
    */
   long getLargeBodySize();
}
