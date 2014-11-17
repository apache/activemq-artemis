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
package org.apache.activemq.core.client.impl;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.spi.core.remoting.ConsumerContext;

/**
 * A ClientSessionInternal
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionInternal extends ClientSession
{
   String getName();

   void acknowledge(ClientConsumer consumer, Message message) throws HornetQException;

   void individualAcknowledge(final ClientConsumer consumer, final Message message) throws HornetQException;

   boolean isCacheLargeMessageClient();

   int getMinLargeMessageSize();

   boolean isCompressLargeMessages();

   void expire(ClientConsumer consumer, Message message) throws HornetQException;

   void addConsumer(ClientConsumerInternal consumer);

   void addProducer(ClientProducerInternal producer);

   void removeConsumer(ClientConsumerInternal consumer) throws HornetQException;

   void removeProducer(ClientProducerInternal producer);

   void handleReceiveMessage(ConsumerContext consumerID, ClientMessageInternal message) throws Exception;

   void handleReceiveLargeMessage(ConsumerContext consumerID, ClientLargeMessageInternal clientLargeMessage, long largeMessageSize) throws Exception;

   void handleReceiveContinuation(ConsumerContext consumerID, byte[] chunk, int flowControlSize, boolean isContinues) throws Exception;

   void handleConsumerDisconnect(ConsumerContext consumerContext) throws HornetQException;

   void preHandleFailover(RemotingConnection connection);

   void handleFailover(RemotingConnection backupConnection, HornetQException cause);

   RemotingConnection getConnection();

   void cleanUp(boolean failingOver) throws HornetQException;

   void setForceNotSameRM(boolean force);

   void workDone();

   void sendProducerCreditsMessage(int credits, SimpleString address);

   ClientProducerCredits getCredits(SimpleString address, boolean anon);

   void returnCredits(SimpleString address);

   void handleReceiveProducerCredits(SimpleString address, int credits);

   void handleReceiveProducerFailCredits(SimpleString address, int credits);

   ClientProducerCreditManager getProducerCreditManager();

   void setAddress(Message message, SimpleString address);

   void setPacketSize(int packetSize);

   void resetIfNeeded() throws HornetQException;

   /**
    * This is used internally to control and educate the user
    * about using the thread boundaries properly.
    * if more than one thread is using the session simultaneously
    * this will generate a big warning on the docs.
    * There are a limited number of places where we can call this such as acks and sends. otherwise we
    * could get false warns
    */
   void startCall();

   /**
    * @see #startCall()
    */
   void endCall();

   /**
    * Sets a stop signal to true. This will cancel
    */
   void setStopSignal();

   boolean isConfirmationWindowEnabled();

   /**
    * @param handler
    */
   void scheduleConfirmation(SendAcknowledgementHandler handler, Message message);

   boolean isClosing();

   String getNodeId();
}
