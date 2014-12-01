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
package org.apache.activemq.jms.client;

import javax.jms.XAQueueSession;
import javax.jms.XATopicSession;

import org.apache.activemq.api.core.client.ClientSession;

/**
 * A ActiveMQXASession
 *
 * @author clebertsuconic
 *
 *
 */
public class ActiveMQXASession extends ActiveMQSession implements XAQueueSession, XATopicSession
{

   /**
    * @param connection
    * @param transacted
    * @param xa
    * @param ackMode
    * @param session
    * @param sessionType
    */
   protected ActiveMQXASession(ActiveMQConnection connection,
                               boolean transacted,
                               boolean xa,
                               int ackMode,
                               ClientSession session,
                               int sessionType)
   {
      super(connection, transacted, xa, ackMode, session, sessionType);
   }
}
