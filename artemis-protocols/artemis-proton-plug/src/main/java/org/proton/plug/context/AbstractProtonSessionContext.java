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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.AMQPSessionContext;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.exceptions.ActiveMQAMQPInternalErrorException;

/**
 * ProtonSession is a direct representation of the session on the broker.
 * It has a link between a ProtonSession and a Broker or Client Session
 * The Broker Session is linked through the ProtonSessionSPI
 */
public abstract class AbstractProtonSessionContext extends ProtonInitializable implements AMQPSessionContext
{
   protected final AbstractConnectionContext connection;

   protected final AMQPSessionCallback sessionSPI;

   protected final Session session;

   private long currentTag = 0;

   protected Map<Receiver, AbstractProtonReceiverContext> receivers = new HashMap<Receiver, AbstractProtonReceiverContext>();

   protected Map<Sender, AbstractProtonContextSender> senders = new HashMap<Sender, AbstractProtonContextSender>();

   protected boolean closed = false;

   public AbstractProtonSessionContext(AMQPSessionCallback sessionSPI, AbstractConnectionContext connection, Session session)
   {
      this.connection = connection;
      this.sessionSPI = sessionSPI;
      this.session = session;
   }

   public void initialise() throws Exception
   {
      if (!isInitialized())
      {
         super.initialise();

         if (sessionSPI != null)
         {
            try
            {
               sessionSPI.init(this, connection.getSASLResult());
            }
            catch (Exception e)
            {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
         }
      }
   }


   /**
    * TODO: maybe it needs to go?
    *
    * @param consumer
    * @param queueName
    */
   public void disconnect(Object consumer, String queueName)
   {
      AbstractProtonContextSender protonConsumer = senders.remove(consumer);
      if (protonConsumer != null)
      {
         try
         {
            protonConsumer.close();
         }
         catch (ActiveMQAMQPException e)
         {
            protonConsumer.getSender().setTarget(null);
            protonConsumer.getSender().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         }
      }
   }


   @Override
   public byte[] getTag()
   {
      return Long.toHexString(currentTag++).getBytes();
   }

   @Override
   public void replaceTag(byte[] tag)
   {
      // TODO: do we need to reuse this?
   }

   @Override
   public void close()
   {
      if (closed)
      {
         return;
      }



      // Making a copy to avoid ConcurrentModificationException during the iteration
      Set<AbstractProtonReceiverContext> receiversCopy = new HashSet<>();
      receiversCopy.addAll(receivers.values());


      for (AbstractProtonReceiverContext protonProducer : receiversCopy)
      {
         try
         {
            protonProducer.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            // TODO Logging
         }
      }
      receivers.clear();

      Set<AbstractProtonContextSender> protonSendersClone = new HashSet<>();
      protonSendersClone.addAll(senders.values());

      for (AbstractProtonContextSender protonConsumer : protonSendersClone)
      {
         try
         {
            protonConsumer.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            // TODO Logging
         }
      }
      senders.clear();
      try
      {
         if (sessionSPI != null)
         {
            sessionSPI.rollbackCurrentTX();
            sessionSPI.close();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         // TODO logging
      }
      closed = true;
   }

   @Override
   public void removeSender(Sender sender) throws ActiveMQAMQPException
   {
      senders.remove(sender);
   }

   @Override
   public void removeReceiver(Receiver receiver)
   {
      receivers.remove(receiver);
   }
}
