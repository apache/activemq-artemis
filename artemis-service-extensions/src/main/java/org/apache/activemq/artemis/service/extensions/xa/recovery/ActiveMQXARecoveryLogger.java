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
package org.apache.activemq.artemis.service.extensions.xa.recovery;

import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.w3c.dom.Node;

/**
 * Logger Code 12
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
 * so an INFO message would be 121000 to 121999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQXARecoveryLogger extends BasicLogger {

   /**
    * The default logger.
    */
   ActiveMQXARecoveryLogger LOGGER = Logger.getMessageLogger(ActiveMQXARecoveryLogger.class, ActiveMQXARecoveryLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 121003, value = "JMS Server Manager Running cached command for {0}", format = Message.Format.MESSAGE_FORMAT)
   void serverRunningCachedCommand(Runnable run);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 121004, value = "JMS Server Manager Caching command for {0} since the JMS Server is not active.",
      format = Message.Format.MESSAGE_FORMAT)
   void serverCachingCommand(Object runnable);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122005, value = "Invalid \"host\" value \"0.0.0.0\" detected for \"{0}\" connector. Switching to \"{1}\". If this new address is incorrect please manually configure the connector to use the proper one.",
      format = Message.Format.MESSAGE_FORMAT)
   void invalidHostForConnector(String name, String newHost);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122007, value = "Queue {0} does not exist on the topic {1}. It was deleted manually probably.", format = Message.Format.MESSAGE_FORMAT)
   void noQueueOnTopic(String queueName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122008, value = "XA Recovery can not connect to any broker on recovery {0}", format = Message.Format.MESSAGE_FORMAT)
   void recoveryConnectFailed(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122011, value = "error unbinding {0} from JNDI", format = Message.Format.MESSAGE_FORMAT)
   void jndiUnbindError(@Cause Exception e, String key);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122012, value = "JMS Server Manager error", format = Message.Format.MESSAGE_FORMAT)
   void jmsServerError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122013, value = "Error in XA Recovery recover", format = Message.Format.MESSAGE_FORMAT)
   void xaRecoverError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122014, value = "Notified of connection failure in xa recovery connectionFactory for provider {0} will attempt reconnect on next pass",
      format = Message.Format.MESSAGE_FORMAT)
   void xaRecoverConnectionError(@Cause Exception e, ClientSessionFactory csf);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122015, value = "Can not connect to {0} on auto-generated resource recovery",
      format = Message.Format.MESSAGE_FORMAT)
   void xaRecoverAutoConnectionError(@Cause Throwable e, XARecoveryConfig csf);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 122016, value = "Error in XA Recovery", format = Message.Format.MESSAGE_FORMAT)
   void xaRecoveryError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122017, value = "Tried to correct invalid \"host\" value \"0.0.0.0\" for \"{0}\" connector, but received an exception.",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToCorrectHost(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122018, value = "Could not start recovery discovery on {0}, we will retry every recovery scan until the server is available",
      format = Message.Format.MESSAGE_FORMAT)
   void xaRecoveryStartError(XARecoveryConfig e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124000, value = "key attribute missing for JMS configuration {0}", format = Message.Format.MESSAGE_FORMAT)
   void jmsConfigMissingKey(Node e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124002, value = "Failed to start JMS deployer", format = Message.Format.MESSAGE_FORMAT)
   void jmsDeployerStartError(@Cause Exception e);
}
