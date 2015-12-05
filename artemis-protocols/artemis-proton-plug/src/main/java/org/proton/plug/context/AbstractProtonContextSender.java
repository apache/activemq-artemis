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
package org.proton.plug.context;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.util.CreditsSemaphore;
import org.proton.plug.util.NettyWritable;

/**
 * A this is a wrapper around an ActiveMQ Artemis ServerConsumer for handling outgoing messages and incoming acks via a Proton Sender
 */
public abstract class AbstractProtonContextSender extends ProtonInitializable implements ProtonDeliveryHandler {

   protected final AbstractProtonSessionContext protonSession;
   protected final Sender sender;
   protected final AbstractConnectionContext connection;
   protected boolean closed = false;
   protected final AMQPSessionCallback sessionSPI;
   protected CreditsSemaphore creditsSemaphore = new CreditsSemaphore(0);

   public AbstractProtonContextSender(AbstractConnectionContext connection,
                                      Sender sender,
                                      AbstractProtonSessionContext protonSession,
                                      AMQPSessionCallback server) {
      this.connection = connection;
      this.sender = sender;
      this.protonSession = protonSession;
      this.sessionSPI = server;
   }

   @Override
   public void onFlow(int credits) {
      this.creditsSemaphore.setCredits(credits);
   }

   /*
   * start the session
   * */
   public void start() throws ActiveMQAMQPException {
      sessionSPI.start();
      // protonSession.getServerSession().start();
   }

   /*
   * close the session
   * */
   @Override
   public void close() throws ActiveMQAMQPException {
      closed = true;
      protonSession.removeSender(sender);
      synchronized (connection.getLock()) {
         sender.close();
      }

      connection.flush();
   }

   @Override
   /*
   * handle an incoming Ack from Proton, basically pass to ActiveMQ Artemis to handle
   * */ public abstract void onMessage(Delivery delivery) throws ActiveMQAMQPException;

   /*
   * check the state of the consumer, i.e. are there any more messages. only really needed for browsers?
   * */
   public void checkState() {
   }

   public Sender getSender() {
      return sender;
   }

   protected int performSend(ProtonJMessage serverMessage, Object context) {
      if (!creditsSemaphore.tryAcquire()) {
         try {
            creditsSemaphore.acquire();
         }
         catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // nothing to be done here.. we just keep going
            throw new IllegalStateException(e.getMessage(), e);
         }
      }

      //presettle means we can ack the message on the dealer side before we send it, i.e. for browsers
      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;

      //we only need a tag if we are going to ack later
      byte[] tag = preSettle ? new byte[0] : protonSession.getTag();

      ByteBuf nettyBuffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);
      try {
         serverMessage.encode(new NettyWritable(nettyBuffer));

         int size = nettyBuffer.writerIndex();

         synchronized (connection.getLock()) {
            final Delivery delivery;
            delivery = sender.delivery(tag, 0, tag.length);
            delivery.setContext(context);

            // this will avoid a copy.. patch provided by Norman using buffer.array()
            sender.send(nettyBuffer.array(), nettyBuffer.arrayOffset() + nettyBuffer.readerIndex(), nettyBuffer.readableBytes());

            if (preSettle) {
               delivery.settle();
            }
            else {
               sender.advance();
            }
         }

         connection.flush();

         return size;
      }
      finally {
         nettyBuffer.release();
      }
   }
}
