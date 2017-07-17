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
package org.apache.activemq.artemis.logs;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 20
 *
 * each message id must be 6 digits long starting with 20, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 201000 to 201999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQUtilLogger extends BasicLogger {

   /**
    * The default logger.
    */
   ActiveMQUtilLogger LOGGER = Logger.getMessageLogger(ActiveMQUtilLogger.class, ActiveMQUtilLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 201000, value = "Network is healthy, starting service {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void startingService(String component);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 201001, value = "Network is unhealthy, stopping service {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void stoppingService(String component);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202000, value = "Missing privileges to set Thread Context Class Loader on Thread Factory. Using current Thread Context Class Loader",
      format = Message.Format.MESSAGE_FORMAT)
   void missingPrivsForClassloader();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202001, value = "{0} is a loopback address and will be discarded.",
      format = Message.Format.MESSAGE_FORMAT)
   void addressloopback(String address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202002, value = "Ping Address {0} wasn't reacheable.",
      format = Message.Format.MESSAGE_FORMAT)
   void addressWasntReacheable(String address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202003, value = "Ping Url {0} wasn't reacheable.",
      format = Message.Format.MESSAGE_FORMAT)
   void urlWasntReacheable(String url);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202004, value = "Error starting component {0} ",
      format = Message.Format.MESSAGE_FORMAT)
   void errorStartingComponent(@Cause Exception e, String component);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202005, value = "Error stopping component {0} ",
      format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingComponent(@Cause Exception e, String component);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202006, value = "Failed to check Url {0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToCheckURL(@Cause Exception e, String url);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202007, value = "Failed to check Address {0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToCheckAddress(@Cause Exception e, String address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202008, value = "Failed to check Address list {0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToParseAddressList(@Cause Exception e, String addressList);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202009, value = "Failed to check Url list {0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToParseUrlList(@Cause Exception e, String urlList);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202010, value = "Failed to set NIC {0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToSetNIC(@Cause Exception e, String nic);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202011, value = "Failed to read from stream {0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToReadFromStream(String stream);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202012, value = "Object cannot be serialized.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToSerializeObject(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202013, value = "Unable to deserialize object.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToDeserializeObject(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202014, value = "Unable to encode byte array into Base64 notation.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToEncodeByteArrayToBase64Notation(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202015, value = "Failed to clean up file {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToCleanupFile(String file);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 202016, value = "Could not list files to clean up in {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void failedListFilesToCleanup(String path);
}
