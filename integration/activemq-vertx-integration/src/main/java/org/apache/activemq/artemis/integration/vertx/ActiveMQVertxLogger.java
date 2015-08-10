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
package org.apache.activemq.artemis.integration.vertx;

import org.apache.activemq.artemis.core.server.ServerMessage;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 19
 *
 * each message id must be 6 digits long starting with 19, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 191000 to 191999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQVertxLogger extends BasicLogger {

   /**
    * The vertx logger.
    */
   ActiveMQVertxLogger LOGGER = Logger.getMessageLogger(ActiveMQVertxLogger.class, ActiveMQVertxLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 192001, value = "Non vertx message: {0}", format = Message.Format.MESSAGE_FORMAT)
   void nonVertxMessage(ServerMessage message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 192002, value = "Invalid vertx type: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidVertxType(Integer type);
}
