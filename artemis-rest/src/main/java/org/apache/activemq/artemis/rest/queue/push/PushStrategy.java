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
package org.apache.activemq.artemis.rest.queue.push;

import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;

public interface PushStrategy {

   /**
    * Return false if unable to connect. Push consumer may be disabled if configured to do so when
    * unable to connect. Throw an exception if the message sent was unaccepted by the receiver.
    * ActiveMQ's retry and dead letter logic will take over from there.
    *
    * @param message
    * @return {@code false} if unable to connect
    */
   boolean push(ClientMessage message);

   void setRegistration(PushRegistration reg);

   void start() throws Exception;

   void stop() throws Exception;

   void setJmsOptions(ConnectionFactoryOptions jmsOptions);
}
