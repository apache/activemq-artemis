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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.core.paging.PagingStore;

/**
 * A ServerMessage
 */
public interface ServerMessage extends MessageInternal, EncodingSupport {

   ServerMessage setMessageID(long id);

   MessageReference createReference(Queue queue);

   /**
    * This will force encoding of the address, and will re-check the buffer
    * This is to avoid setMessageTransient which set the address without changing the buffer
    *
    * @param address
    */
   void forceAddress(SimpleString address);

   int incrementRefCount() throws Exception;

   int decrementRefCount() throws Exception;

   int incrementDurableRefCount();

   int decrementDurableRefCount();

   ServerMessage copy(long newID);

   ServerMessage copy();

   int getMemoryEstimate();

   int getRefCount();

   ServerMessage makeCopyForExpiryOrDLA(long newID,
                                        MessageReference originalReference,
                                        boolean expiry,
                                        boolean copyOriginalHeaders) throws Exception;

   void setOriginalHeaders(ServerMessage other, MessageReference originalReference, boolean expiry);

   void setPagingStore(PagingStore store);

   PagingStore getPagingStore();

   // Is there any _AMQ_ property being used
   boolean hasInternalProperties();

   boolean storeIsPaging();

   void encodeMessageIDToBuffer();

   byte[] getDuplicateIDBytes();

   Object getDuplicateProperty();
}
