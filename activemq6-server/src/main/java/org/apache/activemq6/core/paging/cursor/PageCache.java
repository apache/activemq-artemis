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
package org.apache.activemq6.core.paging.cursor;

import org.apache.activemq6.core.paging.PagedMessage;
import org.apache.activemq6.utils.SoftValueHashMap;

/**
 * A PageCache
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface PageCache extends SoftValueHashMap.ValueCache
{
   long getPageId();

   int getNumberOfMessages();

   void setMessages(PagedMessage[] messages);

   PagedMessage[] getMessages();

   /**
    * @return whether this cache is still being updated
    */
   boolean isLive();

   /**
    *
    * @param messageNumber The order of the message on the page
    * @return
    */
   PagedMessage getMessage(int messageNumber);

   /**
    * When the cache is being created,
    * We need to first read the files before other threads can get messages from this.
    */
   void lock();

   /**
    * You have to call this method within the same thread you called lock
    */
   void unlock();

   void close();

}
