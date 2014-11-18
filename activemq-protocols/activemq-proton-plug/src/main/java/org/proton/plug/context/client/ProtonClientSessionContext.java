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

package org.proton.plug.context.client;

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.proton.plug.AMQPClientReceiverContext;
import org.proton.plug.AMQPClientSenderContext;
import org.proton.plug.AMQPClientSessionContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.util.FutureRunnable;

/**
 * @author Clebert Suconic
 */

public class ProtonClientSessionContext extends AbstractProtonSessionContext implements AMQPClientSessionContext
{
   public ProtonClientSessionContext(AMQPSessionCallback sessionSPI, AbstractConnectionContext connection, Session session)
   {
      super(sessionSPI, connection, session);
   }

   public AMQPClientSenderContext createSender(String address, boolean preSettled) throws ActiveMQAMQPException
   {
      FutureRunnable futureRunnable = new FutureRunnable(1);

      ProtonClientContext amqpSender;
      synchronized (connection.getLock())
      {
         Sender sender = session.sender(address);
         sender.setSenderSettleMode(SenderSettleMode.SETTLED);
         Target target = new Target();
         target.setAddress(address);
         sender.setTarget(target);
         amqpSender = new ProtonClientContext(connection, sender, this, sessionSPI);
         amqpSender.afterInit(futureRunnable);
         sender.setContext(amqpSender);
         sender.open();
      }

      connection.flush();

      waitWithTimeout(futureRunnable);
      return amqpSender;
   }

   public AMQPClientReceiverContext createReceiver(String address) throws ActiveMQAMQPException
   {
      FutureRunnable futureRunnable = new FutureRunnable(1);

      ProtonClientReceiverContext amqpReceiver;

      synchronized (connection.getLock())
      {
         Receiver receiver = session.receiver(address);
         Source source = new Source();
         source.setAddress(address);
         receiver.setSource(source);
         amqpReceiver = new ProtonClientReceiverContext(sessionSPI, connection, this, receiver);
         receiver.setContext(amqpReceiver);
         amqpReceiver.afterInit(futureRunnable);
         receiver.open();
      }

      connection.flush();

      waitWithTimeout(futureRunnable);

      return amqpReceiver;

   }
}
