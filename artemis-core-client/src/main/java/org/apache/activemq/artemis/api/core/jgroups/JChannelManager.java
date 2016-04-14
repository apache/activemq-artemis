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

package org.apache.activemq.artemis.api.core.jgroups;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.JGroupsBroadcastEndpoint;
import org.jboss.logging.Logger;

/**
 * This class maintain a global Map of JChannels wrapped in JChannelWrapper for
 * the purpose of reference counting.
 *
 * Wherever a JChannel is needed it should only get it by calling the getChannel()
 * method of this class. The real disconnect of channels are also done here only.
 */
public class JChannelManager {

   private static final Logger logger = Logger.getLogger(JChannelManager.class);
   private static final boolean isTrace = logger.isTraceEnabled();

   private Map<String, JChannelWrapper> channels;

   public synchronized JChannelWrapper getJChannel(String channelName,
                                                   JGroupsBroadcastEndpoint endpoint) throws Exception {
      if (channels == null) {
         channels = new HashMap<>();
      }
      JChannelWrapper wrapper = channels.get(channelName);
      if (wrapper == null) {
         wrapper = new JChannelWrapper(this, channelName, endpoint.createChannel());
         channels.put(channelName, wrapper);
         if (isTrace)
            logger.trace("Put Channel " + channelName);
         return wrapper;
      }
      if (isTrace)
         logger.trace("Add Ref Count " + channelName);
      return wrapper.addRef();
   }

   public synchronized void removeChannel(String channelName) {
      channels.remove(channelName);
   }

}
