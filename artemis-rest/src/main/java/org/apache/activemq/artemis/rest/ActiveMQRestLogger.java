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
package org.apache.activemq.artemis.rest;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlLink;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
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
public interface ActiveMQRestLogger extends BasicLogger {

   ActiveMQRestLogger LOGGER = Logger.getMessageLogger(ActiveMQRestLogger.class, ActiveMQRestLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 181000, value = "Loading REST push store from: {0}", format = Message.Format.MESSAGE_FORMAT)
   void loadingRestStore(String path);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 181001, value = "adding REST push registration: {0}", format = Message.Format.MESSAGE_FORMAT)
   void addingPushRegistration(String id);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 181002, value = "Push consumer started for: {0}", format = Message.Format.MESSAGE_FORMAT)
   void startingPushConsumer(XmlLink link);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 182000, value = "shutdown REST consumer because of timeout for: {0}", format = Message.Format.MESSAGE_FORMAT)
   void shutdownRestConsumer(String id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 182001, value = "shutdown REST subscription because of timeout for: {0}", format = Message.Format.MESSAGE_FORMAT)
   void shutdownRestSubscription(String id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 182002, value = "Failed to push message to {0}", format = Message.Format.MESSAGE_FORMAT)
   void failedToPushMessageToUri(String uri, @Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 182003, value = "Failed to build Message from object", format = Message.Format.MESSAGE_FORMAT)
   void failedToBuildMessageFromObject(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 182004, value = "REST configuration parameter ''{0}'' is deprecated. Use ''{1}'' instead.", format = Message.Format.MESSAGE_FORMAT)
   void deprecatedConfiguration(String oldConfigParameter, String newConfigParameter);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184000, value = "Failed to load push store {0}, it is probably corrupted", format = Message.Format.MESSAGE_FORMAT)
   void errorLoadingStore(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184001, value = "Error updating store", format = Message.Format.MESSAGE_FORMAT)
   void errorUpdatingStore(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184002, value = "Failed to push message to {0} disabling push registration...", format = Message.Format.MESSAGE_FORMAT)
   void errorPushingMessage(XmlLink link);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 184003, value = "Error deleting Subscriber queue", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingSubscriberQueue(@Cause ActiveMQException e);
}
