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
package org.apache.activemq.core.server;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.persistence.OperationContext;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.security.SecurityStore;
import org.apache.activemq.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.core.server.impl.ServerSessionImpl;
import org.apache.activemq.core.server.management.ManagementService;
import org.apache.activemq.core.transaction.ResourceManager;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.spi.core.protocol.SessionCallback;

public interface ServerSessionFactory
{

   ServerSessionImpl createCoreSession(String name, String username, String password,
         int minLargeMessageSize, boolean autoCommitSends,
         boolean autoCommitAcks, boolean preAcknowledge,
         boolean persistDeliveryCountBeforeDelivery, boolean xa,
         RemotingConnection connection, StorageManager storageManager,
         PostOffice postOffice, ResourceManager resourceManager,
         SecurityStore securityStore, ManagementService managementService,
         ActiveMQServerImpl activeMQServerImpl, SimpleString managementAddress,
         SimpleString simpleString, SessionCallback callback,
         OperationContext context) throws Exception;

}
