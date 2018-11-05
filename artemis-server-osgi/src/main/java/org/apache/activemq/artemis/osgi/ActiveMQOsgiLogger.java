/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.osgi;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger code 58
 *
 * each message id must be 6 digits long starting with 58, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 581000 to 581999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQOsgiLogger extends BasicLogger {

   /**
   * * The default logger.
   */
   ActiveMQOsgiLogger LOGGER = Logger.getMessageLogger(ActiveMQOsgiLogger.class, ActiveMQOsgiLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 581000, value = "Broker config {0} found. Tracking protocols {1}", format = Message.Format.MESSAGE_FORMAT)
   void brokerConfigFound(String name, String protocols);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 581001, value = "Required protocol {0} was added for broker {1}. {2}", format = Message.Format.MESSAGE_FORMAT)
   void protocolWasAddedForBroker(String protocol, String name, String message);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 581002, value = "Required protocol {0} was removed for broker {1}. {2}", format = Message.Format.MESSAGE_FORMAT)
   void protocolWasRemovedForBroker(String protocol, String name, String message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 582000, value = "Error starting broker: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingBroker(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 582001, value = "Error stopping broker: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingBroker(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 582002, value = "Error getting dataSource provider infos.", format = Message.Format.MESSAGE_FORMAT)
   void errorGettingDataSourceProviderInfo(@Cause Exception e);

}

