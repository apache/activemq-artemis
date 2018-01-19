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
package org.apache.activemq.artemis.core.protocol.openwire;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.DataConstants;

public enum OpenWireMessagePersister implements Persister<Message> {

   INSTANCE;
   public static final byte ID = 2;

   public static OpenWireMessagePersister getInstance() {
      return INSTANCE;
   }

   @Override
   public byte getID() {
      return ID;
   }

   @Override
   public int getEncodeSize(Message record) {
      return DataConstants.SIZE_BYTE + record.getPersistSize() +
            SimpleString.sizeofNullableString(record.getAddressSimpleString()) + DataConstants.SIZE_LONG;
   }

   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      buffer.writeByte(getID());
      buffer.writeLong(record.getMessageID());
      buffer.writeNullableSimpleString(record.getAddressSimpleString());
      record.persist(buffer);
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message record) {
      long id = buffer.readLong();
      SimpleString address = buffer.readNullableSimpleString();
      record = new OpenWireMessage();
      record.reloadPersistence(buffer);
      record.setMessageID(id);
      record.setAddress(address);
      return record;
   }

}
