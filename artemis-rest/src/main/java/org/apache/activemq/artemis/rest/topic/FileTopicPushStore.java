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
package org.apache.activemq.artemis.rest.topic;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.rest.queue.push.FilePushStore;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;

class FileTopicPushStore extends FilePushStore implements TopicPushStore {

   FileTopicPushStore(String dirname) throws Exception {
      super(dirname);
   }

   @Override
   public synchronized List<PushTopicRegistration> getByTopic(String topic) {
      List<PushTopicRegistration> list = new ArrayList<>();
      for (PushRegistration reg : map.values()) {
         PushTopicRegistration topicReg = (PushTopicRegistration) reg;
         if (topicReg.getTopic().equals(topic)) {
            list.add(topicReg);
         }
      }
      return list;
   }
}
