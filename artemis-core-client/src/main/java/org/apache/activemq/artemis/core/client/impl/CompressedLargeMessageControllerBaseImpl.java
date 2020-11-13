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
package org.apache.activemq.artemis.core.client.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.utils.InflaterWriter;

abstract class CompressedLargeMessageControllerBaseImpl implements LargeMessageController {

   protected final LargeMessageController bufferDelegate;

   protected CompressedLargeMessageControllerBaseImpl(final LargeMessageController bufferDelegate) {
      this.bufferDelegate = bufferDelegate;
   }

   @Override
   public void discardUnusedPackets() {
      bufferDelegate.discardUnusedPackets();
   }

   @Override
   public void addPacket(byte[] chunk, int flowControlSize, boolean isContinues) {
      bufferDelegate.addPacket(chunk, flowControlSize, isContinues);
   }

   @Override
   public synchronized void cancel() {
      bufferDelegate.cancel();
   }

   @Override
   public synchronized void close() {
      bufferDelegate.cancel();
   }

   @Override
   public void setOutputStream(final OutputStream output) throws ActiveMQException {
      final InflaterWriter writer = new InflaterWriter(output);
      try {
         bufferDelegate.setOutputStream(writer);
      } catch (ActiveMQException e) {
         // it means that the operation hasn't succeeded
         try {
            writer.close();
         } catch (IOException ioException) {
            ActiveMQClientLogger.LOGGER.debugf(ioException, "Errored while closing leaked InflaterWriter due to %s", e.getMessage());
         }
         throw e;
      }
   }

   @Override
   public synchronized void saveBuffer(final OutputStream output) throws ActiveMQException {
      setOutputStream(output);
      waitCompletion(0);
   }

   /**
    * @param timeWait Milliseconds to Wait. 0 means forever
    */
   @Override
   public synchronized boolean waitCompletion(final long timeWait) throws ActiveMQException {
      return bufferDelegate.waitCompletion(timeWait);
   }

   @Override
   public long getSize() {
      return this.bufferDelegate.getSize();
   }
}
