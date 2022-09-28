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
package org.apache.activemq.artemis.jms.server;

import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.logs.annotation.LogBundle;
import org.apache.activemq.artemis.logs.annotation.LogMessage;
import org.apache.activemq.artemis.logs.BundleFactory;
import org.w3c.dom.Node;

/**
 * Logger Code 12
 */
@LogBundle(projectCode = "AMQ", regexID = "12[0-9]{4}")
public interface ActiveMQJMSServerLogger {

   ActiveMQJMSServerLogger LOGGER = BundleFactory.newBundle(ActiveMQJMSServerLogger.class, ActiveMQJMSServerLogger.class.getPackage().getName());

   @LogMessage(id = 121004, value = "JMS Server Manager Caching command for {} since the JMS Server is not active.",
      level = LogMessage.Level.INFO)
   void serverCachingCommand(Object runnable);

   @LogMessage(id = 122005, value = "Invalid \"host\" value \"0.0.0.0\" detected for \"{}\" connector. Switching to \"{}\". If this new address is incorrect please manually configure the connector to use the proper one.",
      level = LogMessage.Level.WARN)
   void invalidHostForConnector(String name, String newHost);

   @LogMessage(id = 122007, value = "Queue {} does not exist on the topic {}. It was deleted manually probably.", level = LogMessage.Level.WARN)
   void noQueueOnTopic(String queueName, String name);

   @LogMessage(id = 122008, value = "XA Recovery can not connect to any broker on recovery {}", level = LogMessage.Level.WARN)
   void recoveryConnectFailed(String s);

   @LogMessage(id = 122011, value = "error unbinding {} from Registry", level = LogMessage.Level.WARN)
   void bindingsUnbindError(String key, Exception e);

   @LogMessage(id = 122012, value = "JMS Server Manager error", level = LogMessage.Level.WARN)
   void jmsServerError(Exception e);

   @LogMessage(id = 122013, value = "Error in XA Recovery recover", level = LogMessage.Level.WARN)
   void xaRecoverError(Exception e);

   @LogMessage(id = 122014, value = "Notified of connection failure in xa recovery connectionFactory for provider {} will attempt reconnect on next pass",
      level = LogMessage.Level.WARN)
   void xaRecoverConnectionError(ClientSessionFactory csf, Exception e);

   @LogMessage(id = 122016, value = "Error in XA Recovery", level = LogMessage.Level.DEBUG)
   void xaRecoveryError(Exception e);

   @LogMessage(id = 122017, value = "Tried to correct invalid \"host\" value \"0.0.0.0\" for \"{}\" connector, but received an exception.",
      level = LogMessage.Level.WARN)
   void failedToCorrectHost(String name, Exception e);

   @LogMessage(id = 122018,
      value = "Failed to send notification: {}",
      level = LogMessage.Level.WARN)
   void failedToSendNotification(String notification);

   @LogMessage(id = 122019,
      value = "Unable to deactivate server",
      level = LogMessage.Level.WARN)
   void failedToDeactivateServer(Exception e);

   @LogMessage(id = 123000, value = "JMS Server Manager Running cached command for {}." + "(In the event of failover after failback has occurred, this message may be output multiple times.)",
      level = LogMessage.Level.DEBUG)
   void serverRunningCachedCommand(Runnable run);

   @LogMessage(id = 124000, value = "key attribute missing for JMS configuration {}", level = LogMessage.Level.ERROR)
   void jmsConfigMissingKey(Node e);

   @LogMessage(id = 124002, value = "Failed to start JMS deployer", level = LogMessage.Level.ERROR)
   void jmsDeployerStartError(Exception e);
}
