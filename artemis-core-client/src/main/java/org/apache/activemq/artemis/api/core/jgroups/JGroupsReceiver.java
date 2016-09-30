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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;
import org.jgroups.ReceiverAdapter;

/**
 * This class is used to receive messages from a JGroups channel.
 * Incoming messages are put into a queue.
 */
public class JGroupsReceiver extends ReceiverAdapter {

   private static final Logger logger = Logger.getLogger(JGroupsReceiver.class);

   private final BlockingQueue<byte[]> dequeue = new LinkedBlockingDeque<>();

   @Override
   public void receive(org.jgroups.Message msg) {
      if (logger.isTraceEnabled())
         logger.trace("sending message " + msg);
      dequeue.add(msg.getBuffer());
   }

   public byte[] receiveBroadcast() throws Exception {
      byte[] bytes = dequeue.take();
      if (logger.isTraceEnabled()) {
         logBytes("receiveBroadcast()", bytes);
      }

      return bytes;
   }

   private void logBytes(String methodName, byte[] bytes) {
      if (bytes != null) {
         logger.trace(methodName + "::" + bytes.length + " bytes");
      } else {
         logger.trace(methodName + ":: no bytes");
      }
   }

   public byte[] receiveBroadcast(long time, TimeUnit unit) throws Exception {
      byte[] bytes = dequeue.poll(time, unit);

      if (logger.isTraceEnabled()) {
         logBytes("receiveBroadcast(long time, TimeUnit unit)", bytes);
      }

      return bytes;
   }
}

