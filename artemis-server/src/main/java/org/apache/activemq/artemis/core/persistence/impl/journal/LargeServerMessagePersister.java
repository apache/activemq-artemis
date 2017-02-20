/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.persistence.impl.journal;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.utils.DataConstants;

public class LargeServerMessagePersister implements Persister<LargeServerMessage> {

   /**
    *  for future usage...
    *  when we have refactored large message properly
    *  this could be used to differentiate other protocols large message persisters
    */
   byte PERSISTER_ID = 11;

   public static LargeServerMessagePersister theInstance = new LargeServerMessagePersister();

   public static LargeServerMessagePersister getInstance() {
      return theInstance;
   }

   protected LargeServerMessagePersister() {
   }

   @Override
   public int getEncodeSize(LargeServerMessage record) {
      return DataConstants.SIZE_BYTE + DataConstants.SIZE_LONG + SimpleString.sizeofNullableString(record.getAddressSimpleString())
             + record.getPersistSize();
   }

   /** Sub classes must add the first short as the protocol-id */
   @Override
   public void encode(ActiveMQBuffer buffer, LargeServerMessage record) {
      buffer.writeByte(PERSISTER_ID);
      buffer.writeLong(record.getMessageID());
      buffer.writeNullableSimpleString(record.getAddressSimpleString());
      record.persist(buffer);
   }


   @Override
   public LargeServerMessage decode(ActiveMQBuffer buffer, LargeServerMessage record) {
      // the caller must consume the first byte already, as that will be used to decide what persister (protocol) to use
      buffer.readByte(); // for future usage, not used now
      long id = buffer.readLong();
      SimpleString address = buffer.readNullableSimpleString();
      record.reloadPersistence(buffer);
      record.setMessageID(id);
      record.setAddress(address);
      return record;
   }


}
