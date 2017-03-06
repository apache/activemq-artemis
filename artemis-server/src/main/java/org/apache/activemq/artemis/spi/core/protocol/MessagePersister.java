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

package org.apache.activemq.artemis.spi.core.protocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.jboss.logging.Logger;

public class MessagePersister implements Persister<Message> {

   private static final Logger logger = Logger.getLogger(MessagePersister.class);

   private static final MessagePersister theInstance = new MessagePersister();

   /** This will be used for reading messages */
   private static Map<Byte, Persister<Message>> protocols = new ConcurrentHashMap<>();


   public static void registerProtocol(ProtocolManagerFactory manager) {
      Persister<Message> messagePersister = manager.getPersister();
      if (messagePersister == null) {
         logger.warn("Cannot find persister for " + manager);
      } else {
         registerPersister(manager.getStoreID(), manager.getPersister());
      }
   }

   public static void clearPersisters() {
      protocols.clear();
   }

   public static void registerPersister(byte recordType, Persister<Message> persister) {
      protocols.put(recordType, persister);
   }

   public static MessagePersister getInstance() {
      return theInstance;
   }


   protected MessagePersister() {
   }

   protected byte getID() {
      return (byte)0;
   }

   @Override
   public int getEncodeSize(Message record) {
      return 0;
   }


   /** Sub classes must add the first short as the protocol-id */
   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      buffer.writeByte(getID());
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message record) {
      byte protocol = buffer.readByte();
      Persister<Message> persister = protocols.get(protocol);
      if (persister == null) {
         throw new NullPointerException("couldn't find factory for type=" + protocol);
      }
      return persister.decode(buffer, record);
   }
}
