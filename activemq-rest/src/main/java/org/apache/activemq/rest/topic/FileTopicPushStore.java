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
package org.apache.activemq.rest.topic;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.rest.queue.push.FilePushStore;
import org.apache.activemq.rest.queue.push.xml.PushRegistration;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class FileTopicPushStore extends FilePushStore implements TopicPushStore
{
   public FileTopicPushStore(String dirname) throws Exception
   {
      super(dirname);
   }

   @Override
   public synchronized List<PushTopicRegistration> getByTopic(String topic)
   {
      List<PushTopicRegistration> list = new ArrayList<PushTopicRegistration>();
      for (PushRegistration reg : map.values())
      {
         PushTopicRegistration topicReg = (PushTopicRegistration)reg;
         if (topicReg.getTopic().equals(topic))
         {
            list.add(topicReg);
         }
      }
      return list;
   }
}
