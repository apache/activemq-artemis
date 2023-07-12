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
package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.logs.BundleFactory;
import org.apache.activemq.artemis.logs.annotation.LogBundle;
import org.apache.activemq.artemis.logs.annotation.LogMessage;

/**
 * This is using a separate Logger specific for Queue / QueueImpl.
 * It's using Queue.class.getName() as the category as it would be possible to disable this logger with log4j.
 * This is sharing the codes with ActiveMQServerLogger (meaning the codes between here and ActiveMQServerLogger have to be unique).
 */
@LogBundle(projectCode = "AMQ", regexID = "22[0-9]{4}")
public interface ActiveMQQueueLogger {

   ActiveMQQueueLogger LOGGER = BundleFactory.newBundle(ActiveMQQueueLogger.class, Queue.class.getName());

   @LogMessage(id = 224127, value = "Message dispatch from paging is blocked. Address {}/Queue {} will not read any more messages from paging until pending messages are acknowledged. There are currently {} messages pending ({} bytes) with max reads at maxPageReadMessages({}) and maxPageReadBytes({}). Either increase reading attributes at the address-settings or change your consumers to acknowledge more often.", level = LogMessage.Level.WARN)
   void warnPageFlowControl(String address,
                            String queue,
                            long messageCount,
                            long messageBytes,
                            long maxMessages,
                            long maxMessagesBytes);
}
