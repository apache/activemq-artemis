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

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientMessage;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.utils.FutureLatch;

/**
 * A ClientConsumerInternal
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientConsumerInternal extends ClientConsumer
{
   SimpleString getQueueName();

   SimpleString getFilterString();

   boolean isBrowseOnly();

   void handleMessage(ClientMessageInternal message) throws Exception;

   void handleLargeMessage(ClientLargeMessageInternal clientLargeMessage, long largeMessageSize) throws Exception;

   void handleLargeMessageContinuation(byte[] chunk, int flowControlSize, boolean isContinues) throws Exception;

   void flowControl(final int messageBytes, final boolean discountSlowConsumer) throws HornetQException;

   void clear(boolean waitForOnMessage) throws HornetQException;

   /**
    * To be called by things like MDBs during shutdown of the server
    *
    * @throws HornetQException
    * @param future
    */
   Thread prepareForClose(FutureLatch future) throws HornetQException;

   void clearAtFailover();

   int getClientWindowSize();

   int getBufferSize();

   void cleanUp() throws HornetQException;

   void acknowledge(ClientMessage message) throws HornetQException;

   void individualAcknowledge(ClientMessage message) throws HornetQException;

   void flushAcks() throws HornetQException;

   void stop(boolean waitForOnMessage) throws HornetQException;

   void start();

   ClientSession.QueueQuery getQueueInfo();
}
