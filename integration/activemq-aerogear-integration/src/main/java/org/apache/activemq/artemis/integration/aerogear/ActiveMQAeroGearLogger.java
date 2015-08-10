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
package org.apache.activemq.artemis.integration.aerogear;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 23
 *
 * each message id must be 6 digits long starting with 18, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 181000 to 181999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQAeroGearLogger extends BasicLogger {

   /**
    * The aerogear logger.
    */
   ActiveMQAeroGearLogger LOGGER = Logger.getMessageLogger(ActiveMQAeroGearLogger.class, ActiveMQAeroGearLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 231001, value = "aerogear connector connected to {0}", format = Message.Format.MESSAGE_FORMAT)
   void connected(String endpoint);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 232003, value = "removing aerogear connector as credentials are invalid", format = Message.Format.MESSAGE_FORMAT)
   void reply401();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 232004, value = "removing aerogear connector as endpoint is invalid", format = Message.Format.MESSAGE_FORMAT)
   void reply404();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 232005, value = "removing aerogear connector as unexpected respone {0} returned", format = Message.Format.MESSAGE_FORMAT)
   void replyUnknown(int status);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 232006, value = "unable to connect to aerogear server, retrying in {0} seconds", format = Message.Format.MESSAGE_FORMAT)
   void sendFailed(int retry);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 232007, value = "removing aerogear connector unable to connect after {0} attempts, giving up", format = Message.Format.MESSAGE_FORMAT)
   void unableToReconnect(int retryAttempt);
}
