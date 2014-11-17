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
package org.apache.activemq.core.paging;

import org.apache.activemq.core.journal.EncodingSupport;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.server.ServerMessage;

/**
 * A Paged message.
 * <p>
 * We can't just record the ServerMessage as we need other information (such as the TransactionID
 * used during paging)
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public interface PagedMessage extends EncodingSupport
{
   ServerMessage getMessage();

   /** The queues that were routed during paging */
   long[] getQueueIDs();

   void initMessage(StorageManager storageManager);

   long getTransactionID();
}
