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
package org.proton.plug;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.proton.plug.context.ProtonPlugSender;

/**
 * These are methods where the Proton Plug component will call your server
 */
public interface AMQPSessionCallback {

   void init(AMQPSessionContext session, SASLResult saslResult) throws Exception;

   void start();

   void onFlowConsumer(Object consumer, int credits, boolean drain);

   Object createSender(ProtonPlugSender protonSender, String queue, String filer, boolean browserOnly) throws Exception;

   void startSender(Object brokerConsumer) throws Exception;

   void createTemporaryQueue(String queueName) throws Exception;

   void createTemporaryQueue(String address, String queueName, String filter) throws Exception;

   void createDurableQueue(String address, String queueName, String filter) throws Exception;

   void offerProducerCredit(String address, int credits, int threshold, Receiver receiver);

   void deleteQueue(String address) throws Exception;

   /**
    * Returns true if a queue is found with matching name, if autoCreate=true and autoCreateJMSQueues is switched on then
    * this method will auto create the queue, with name=queueName, address=queueName, filter=null.
    *
    * @param queueName
    * @param autoCreate
    * @return
    * @throws Exception
    */
   QueueQueryResult queueQuery(String queueName, boolean autoCreate) throws Exception;

   boolean bindingQuery(String address) throws Exception;

   void closeSender(Object brokerConsumer) throws Exception;

   // This one can be a lot improved
   ProtonJMessage encodeMessage(Object message, int deliveryCount) throws Exception;

   Binary getCurrentTXID();

   String tempQueueName();

   void commitCurrentTX() throws Exception;

   void rollbackCurrentTX(boolean lastMessageReceived) throws Exception;

   void close() throws Exception;

   void ack(Object brokerConsumer, Object message) throws Exception;

   /**
    * @param brokerConsumer
    * @param message
    * @param updateCounts   this identified if the cancel was because of a failure or just cleaning up the
    *                       client's cache.
    *                       in some implementations you could call this failed
    */
   void cancel(Object brokerConsumer, Object message, boolean updateCounts) throws Exception;

   void resumeDelivery(Object consumer);

   /**
    * @param delivery
    * @param address
    * @param messageFormat
    * @param messageEncoded a Heap Buffer ByteBuffer (safe to convert into byte[])
    */
   void serverSend(Receiver receiver,
                   Delivery delivery,
                   String address,
                   int messageFormat,
                   ByteBuf messageEncoded) throws Exception;

   String getPubSubPrefix();
}
