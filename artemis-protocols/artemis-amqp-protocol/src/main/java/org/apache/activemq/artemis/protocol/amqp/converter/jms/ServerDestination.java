/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.converter.jms;

import org.apache.activemq.artemis.jms.client.ActiveMQDestination;

import javax.jms.JMSException;
import javax.jms.Queue;

/**
 * This is just here to avoid all the client checks we need with valid JMS destinations, protocol convertors don't need to
 * adhere to the jms. semantics.
 */
public class ServerDestination extends ActiveMQDestination implements Queue {
   public ServerDestination(String name) {
      super(name, name, false, false, null);
   }

   @Override
   public String getQueueName() throws JMSException {
      return getName();
   }
}
