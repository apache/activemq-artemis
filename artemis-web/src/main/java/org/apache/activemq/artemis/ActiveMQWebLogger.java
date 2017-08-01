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
package org.apache.activemq.artemis;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import java.io.File;

/**
 * Logger Code 24
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
public interface ActiveMQWebLogger extends BasicLogger {

   ActiveMQWebLogger LOGGER = Logger.getMessageLogger(ActiveMQWebLogger.class, ActiveMQWebLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 241001, value = "HTTP Server started at {0}", format = Message.Format.MESSAGE_FORMAT)
   void webserverStarted(String bind);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 241002, value = "Artemis Jolokia REST API available at {0}", format = Message.Format.MESSAGE_FORMAT)
   void jolokiaAvailable(String bind);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 244003, value = "Temporary file not deleted on shutdown: {0}", format = Message.Format.MESSAGE_FORMAT)
   void tmpFileNotDeleted(File tmpdir);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 241004, value = "Artemis Console available at {0}", format = Message.Format.MESSAGE_FORMAT)
   void consoleAvailable(String bind);
}
