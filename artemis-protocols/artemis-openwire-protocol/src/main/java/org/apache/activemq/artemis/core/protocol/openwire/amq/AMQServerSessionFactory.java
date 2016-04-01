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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.QueueCreator;
import org.apache.activemq.artemis.core.server.ServerSessionFactory;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;

public class AMQServerSessionFactory implements ServerSessionFactory {

   private static final AMQServerSessionFactory singleInstance = new AMQServerSessionFactory();

   public static AMQServerSessionFactory getInstance() {
      return singleInstance;
   }

   private AMQServerSessionFactory() {
   }

   @Override
   public ServerSessionImpl createCoreSession(String name,
                                              String username,
                                              String password,
                                              int minLargeMessageSize,
                                              boolean autoCommitSends,
                                              boolean autoCommitAcks,
                                              boolean preAcknowledge,
                                              boolean persistDeliveryCountBeforeDelivery,
                                              boolean xa,
                                              RemotingConnection connection,
                                              StorageManager storageManager,
                                              PostOffice postOffice,
                                              ResourceManager resourceManager,
                                              SecurityStore securityStore,
                                              ManagementService managementService,
                                              ActiveMQServerImpl activeMQServerImpl,
                                              SimpleString managementAddress,
                                              SimpleString simpleString,
                                              SessionCallback callback,
                                              QueueCreator queueCreator,
                                              OperationContext context) throws Exception {
      return new AMQServerSession(name, username, password, minLargeMessageSize, autoCommitSends, autoCommitAcks, preAcknowledge, persistDeliveryCountBeforeDelivery, xa, connection, storageManager, postOffice, resourceManager, securityStore, managementService, activeMQServerImpl, managementAddress, simpleString, callback, queueCreator, context);
   }

}
