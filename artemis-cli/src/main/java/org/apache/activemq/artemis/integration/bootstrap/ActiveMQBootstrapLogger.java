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
package org.apache.activemq.artemis.integration.bootstrap;

import org.apache.activemq.artemis.logs.annotation.LogBundle;
import org.apache.activemq.artemis.logs.annotation.LogMessage;
import org.apache.activemq.artemis.logs.BundleFactory;

/**
 * Logger Code 10
 */
@LogBundle(projectCode = "AMQ", regexID = "10[0-9]{4}")
public interface ActiveMQBootstrapLogger {

   ActiveMQBootstrapLogger LOGGER = BundleFactory.newBundle(ActiveMQBootstrapLogger.class, ActiveMQBootstrapLogger.class.getPackage().getName());

   @LogMessage(id = 101000, value = "Starting ActiveMQ Artemis Server", level = LogMessage.Level.INFO)
   void serverStarting();

   @LogMessage(id = 101001, value = "Stopping ActiveMQ Artemis Server", level = LogMessage.Level.INFO)
   void serverStopping();

   @LogMessage(id = 101002, value = "Starting Naming server on {}:{} (rmi {}:{})", level = LogMessage.Level.INFO)
   void startedNamingService(String bindAddress, int port, String rmiBindAddress, int rmiPort);

   @LogMessage(id = 101003, value = "Halting ActiveMQ Artemis Server after user request", level = LogMessage.Level.INFO)
   void serverKilled();

   @LogMessage(id = 104000, value = "Failed to delete file {}", level = LogMessage.Level.ERROR)
   void errorDeletingFile(String name);
}
