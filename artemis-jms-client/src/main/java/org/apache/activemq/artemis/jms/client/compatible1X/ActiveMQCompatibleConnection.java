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

package org.apache.activemq.artemis.jms.client.compatible1X;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.apache.activemq.artemis.jms.client.ActiveMQXASession;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;

public class ActiveMQCompatibleConnection extends ActiveMQConnection {

   public ActiveMQCompatibleConnection(ConnectionFactoryOptions options,
                                       String username,
                                       String password,
                                       int connectionType,
                                       String clientID,
                                       int dupsOKBatchSize,
                                       int transactionBatchSize,
                                       boolean cacheDestinations,
                                       ClientSessionFactory sessionFactory) {
      super(options, username, password, connectionType, clientID, dupsOKBatchSize, transactionBatchSize, cacheDestinations, sessionFactory);
   }

   @Override
   protected ActiveMQSession createAMQSession(boolean isXA,
                                              boolean transacted,
                                              int acknowledgeMode,
                                              ClientSession session,
                                              int type) {
      if (isXA) {
         return new ActiveMQCompatibleXASession(options, this, transacted, true, acknowledgeMode, cacheDestinations, session, type);
      } else {
         return new ActiveMQCompatibleSession(options, this, transacted, false, acknowledgeMode, cacheDestinations, session, type);
      }
   }

}
