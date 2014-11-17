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
package org.apache.activemq.tests.integration.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.core.server.ServerMessage;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class FakeStorageManager extends NullStorageManager
{
   List<Long> messageIds = new ArrayList<Long>();

   List<Long> ackIds = new ArrayList<Long>();

   @Override
   public void storeMessage(final ServerMessage message) throws Exception
   {
      messageIds.add(message.getMessageID());
   }

   @Override
   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
      messageIds.add(message.getMessageID());
   }

   @Override
   public void deleteMessage(final long messageID) throws Exception
   {
      messageIds.remove(messageID);
   }

   @Override
   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
      ackIds.add(messageID);
   }

   @Override
   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageiD) throws Exception
   {
      ackIds.add(messageiD);
   }
}
