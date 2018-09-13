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
package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.replication.ReplicatedLargeMessage;

public interface LargeServerMessage extends ReplicatedLargeMessage, ICoreMessage {

   long NO_PENDING_ID = -1;

   @Override
   void addBytes(byte[] bytes) throws Exception;

   void addBytes(ActiveMQBuffer bytes) throws Exception;

   /**
    * We have to copy the large message content in case of DLQ and paged messages
    * For that we need to pre-mark the LargeMessage with a flag when it is paged
    */
   void setPaged();

   /**
    * Close the files if opened
    */
   @Override
   void releaseResources();

   @Override
   void deleteFile() throws Exception;

   void incrementDelayDeletionCount();

   void decrementDelayDeletionCount() throws Exception;

   /**
    * @return
    * @throws ActiveMQException
    */
   SequentialFile getFile() throws ActiveMQException;
}
