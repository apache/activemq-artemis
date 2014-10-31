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
package org.hornetq.core.server.cluster;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.postoffice.QueueBinding;

/**
 * A RemoteQueueBinding
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 23 Jan 2009 11:58:05
 *
 *
 */
public interface RemoteQueueBinding extends QueueBinding
{
   void addConsumer(SimpleString filterString) throws Exception;

   void removeConsumer(SimpleString filterString) throws Exception;

   void reset();

   void disconnect();

   void connect();

   long getRemoteQueueID();
}
