/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.activemq6.jms.bridge;

import javax.management.ObjectName;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 *
 * Logger Code 34
 *
 * each message id must be 6 digits long starting with 12, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 341000 to 341999
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQJMSBridgeLogger extends BasicLogger
{
   /**
    * The default logger.
    */
   HornetQJMSBridgeLogger LOGGER = Logger.getMessageLogger(HornetQJMSBridgeLogger.class, HornetQJMSBridgeLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 341000, value = "Failed to set up JMS bridge connections. Most probably the source or target servers are unavailable." +
         " Will retry after a pause of {0} ms", format = Message.Format.MESSAGE_FORMAT)
   void failedToSetUpBridge(long failureRetryInterval);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 341001, value = "JMS Bridge Succeeded in reconnecting to servers" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeReconnected();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 341002, value = "Succeeded in connecting to servers" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnected();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342000, value = "Attempt to start JMS Bridge, but is already started" , format = Message.Format.MESSAGE_FORMAT)
   void errorBridgeAlreadyStarted();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342001, value = "Failed to start JMS Bridge" , format = Message.Format.MESSAGE_FORMAT)
   void errorStartingBridge();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342002, value = "Failed to unregisted JMS Bridge {0}" , format = Message.Format.MESSAGE_FORMAT)
   void errorUnregisteringBridge(ObjectName objectName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342003, value = "JMS Bridge unable to set up connections, bridge will be stopped" , format = Message.Format.MESSAGE_FORMAT)
   void errorConnectingBridge();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342004, value = "JMS Bridge Will retry after a pause of {0} ms" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeRetry(long failureRetryInterval);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342005, value = "JMS Bridge unable to set up connections, bridge will not be started" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeNotStarted();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342006, value = "Detected failure on bridge connection" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeFailure(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342009, value = "JMS Bridge failed to send + acknowledge batch, closing JMS objects"  , format = Message.Format.MESSAGE_FORMAT)
   void bridgeAckError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342010, value = "Failed to connect JMS Bridge", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnectError(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 344001, value = "Failed to start source connection" , format = Message.Format.MESSAGE_FORMAT)
   void jmsBridgeSrcConnectError(@Cause Exception e);
}
