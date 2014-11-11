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
package org.apache.activemq6.core.client.impl;

import java.io.OutputStream;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.HornetQException;

/**
 * A LargeMessageBufferInternal
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface LargeMessageController extends HornetQBuffer
{
   /**
    * Returns the size of this buffer.

    */
   long getSize();

   /**
    * Discards packets unused by this buffer.
    */
   void discardUnusedPackets();

   /**
    * Closes this buffer.
    */
   void close();

   /**
    * Cancels this buffer.
    */
   void cancel();

   /**
    * Sets the OutputStream of this buffer to the specified output.
    */
   void setOutputStream(final OutputStream output) throws HornetQException;

   /**
    * Saves this buffer to the specified output. This is just a blocking version of
    * {@link #setOutputStream(OutputStream)}.
    */
   void saveBuffer(final OutputStream output) throws HornetQException;

   void addPacket(byte[] chunk, int flowControlSize, boolean isContinues);

   /**
    * Waits for the completion for the specified waiting time (in milliseconds).
    */
   boolean waitCompletion(long timeWait) throws HornetQException;

}
