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
package org.apache.activemq6.core.paging.cursor.impl;

import java.util.LinkedList;
import java.util.List;

import org.apache.activemq6.core.paging.PagedMessage;
import org.apache.activemq6.core.paging.cursor.LivePageCache;
import org.apache.activemq6.core.paging.impl.Page;
import org.apache.activemq6.core.server.LargeServerMessage;

/**
 * This is the same as PageCache, however this is for the page that's being currently written.
 *
 * @author clebertsuconic
 *
 *
 */
public class LivePageCacheImpl implements LivePageCache
{
   private final List<PagedMessage> messages = new LinkedList<PagedMessage>();

   private final Page page;

   private boolean isLive = true;

   public LivePageCacheImpl(final Page page)
   {
      this.page = page;
   }

   @Override
   public long getPageId()
   {
      return page.getPageId();
   }

   @Override
   public synchronized int getNumberOfMessages()
   {
      return messages.size();
   }

   @Override
   public synchronized void setMessages(PagedMessage[] messages)
   {
      // This method shouldn't be called on liveCache, but we will provide the implementation for it anyway
      for (PagedMessage msg : messages)
      {
         addLiveMessage(msg);
      }
   }

   @Override
   public synchronized PagedMessage getMessage(int messageNumber)
   {
      if (messageNumber < messages.size())
      {
         return messages.get(messageNumber);
      }
      else
      {
         return null;
      }
   }

   @Override
   public void lock()
   {
      // nothing to be done on live cache
   }

   @Override
   public void unlock()
   {
      // nothing to be done on live cache
   }

   @Override
   public synchronized boolean isLive()
   {
      return isLive;
   }

   @Override
   public synchronized void addLiveMessage(PagedMessage message)
   {
      if (message.getMessage().isLargeMessage())
      {
         ((LargeServerMessage)message.getMessage()).incrementDelayDeletionCount();
      }
      this.messages.add(message);
   }

   @Override
   public synchronized void close()
   {
      this.isLive = false;
   }

   @Override
   public synchronized PagedMessage[] getMessages()
   {
      return messages.toArray(new PagedMessage[messages.size()]);
   }

   @Override
   public String toString()
   {
      return "LivePacheCacheImpl::page=" + page.getPageId() + " number of messages=" + messages.size() + " isLive = " +
               isLive;
   }
}
