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

package org.hornetq.core.protocol.proton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPInternalErrorException;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.ReadyListener;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         4/10/13
 */
public class ProtonSession implements SessionCallback
{
   private final String name;

   private final ProtonRemotingConnection connection;

   private final HornetQServer server;

   private final TransportImpl protonTransport;

   private final ProtonProtocolManager protonProtocolManager;

   private ServerSession serverSession;

   private OperationContext context;

   //todo make this configurable
   private int tagCacheSize = 1000;

   private long currentTag = 0;

   private final List<byte[]> tagCache = new ArrayList<byte[]>();

   private Map<Object, ProtonProducer> producers = new HashMap<Object, ProtonProducer>();

   private Map<Long, ProtonConsumer> consumers = new HashMap<Long, ProtonConsumer>();

   private boolean closed = false;

   public ProtonSession(String name, ProtonRemotingConnection connection, ProtonProtocolManager protonProtocolManager, OperationContext operationContext, HornetQServer server, TransportImpl protonTransport)
   {
      this.name = name;
      this.connection = connection;
      context = operationContext;
      this.server = server;
      this.protonTransport = protonTransport;
      this.protonProtocolManager = protonProtocolManager;
   }

   public ServerSession getServerSession()
   {
      return serverSession;
   }

   /*
   * we need to initialise the actual server session when we receive the first linkas this tells us whether or not the
   * session is transactional
   * */
   public void initialise(boolean transacted) throws HornetQAMQPInternalErrorException
   {
      if (serverSession == null)
      {
         try
         {
            serverSession = server.createSession(name,
                                                 connection.getLogin(),
                                                 connection.getPasscode(),
                                                 HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                 connection,
                                                 !transacted,
                                                 !transacted,
                                                 false,
                                                 false,
                                                 null,
                                                 this);
         }
         catch (Exception e)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCreatingHornetQSession(e.getMessage());
         }
      }
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address)
   {
   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address)
   {
   }

   @Override
   public int sendMessage(ServerMessage message, long consumerID, int deliveryCount)
   {
      ProtonConsumer protonConsumer = consumers.get(consumerID);
      if (protonConsumer != null)
      {
         return protonConsumer.handleDelivery(message, deliveryCount);
      }
      return 0;
   }

   @Override
   public int sendLargeMessage(ServerMessage message, long consumerID, long bodySize, int deliveryCount)
   {
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
   {
      return 0;
   }

   @Override
   public void closed()
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void addReadyListener(ReadyListener listener)
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void removeReadyListener(ReadyListener listener)
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void disconnect(long consumerId, String queueName)
   {
      ProtonConsumer protonConsumer = consumers.remove(consumerId);
      if (protonConsumer != null)
      {
         try
         {
            protonConsumer.close();
         }
         catch (HornetQAMQPException e)
         {
            protonConsumer.getSender().setTarget(null);
            protonConsumer.getSender().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         }
         connection.write();
      }
   }

   public OperationContext getContext()
   {
      return context;
   }

   public void addProducer(Receiver receiver) throws HornetQAMQPException
   {
      try
      {
         ProtonProducer producer = new ProtonProducer(connection, this, protonProtocolManager, receiver);
         producer.init();
         producers.put(receiver, producer);
         receiver.setContext(producer);
         receiver.open();
      }
      catch (HornetQAMQPException e)
      {
         producers.remove(receiver);
         receiver.setTarget(null);
         receiver.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         receiver.close();
      }
   }


   public void addTransactionHandler(Coordinator coordinator, Receiver receiver)
   {
      TransactionHandler transactionHandler = new TransactionHandler(connection, coordinator, protonProtocolManager, this);
      receiver.setContext(transactionHandler);
      receiver.open();
      receiver.flow(100);
   }

   public void addConsumer(Sender sender) throws HornetQAMQPException
   {
      ProtonConsumer protonConsumer = new ProtonConsumer(connection, sender, this, server, protonProtocolManager);

      try
      {
         protonConsumer.init();
         consumers.put(protonConsumer.getConsumerID(), protonConsumer);
         sender.setContext(protonConsumer);
         sender.open();
         protonConsumer.start();
      }
      catch (HornetQAMQPException e)
      {
         consumers.remove(protonConsumer.getConsumerID());
         sender.setSource(null);
         sender.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         sender.close();
      }
   }

   public byte[] getTag()
   {
      synchronized (tagCache)
      {
         byte[] bytes;
         if (tagCache.size() > 0)
         {
            bytes = tagCache.remove(0);
         }
         else
         {
            bytes = Long.toHexString(currentTag++).getBytes();
         }
         return bytes;
      }
   }

   public void replaceTag(byte[] tag)
   {
      synchronized (tagCache)
      {
         if (tagCache.size() < tagCacheSize)
         {
            tagCache.add(tag);
         }
      }
   }

   public void close()
   {
      if (closed)
      {
         return;
      }

      for (ProtonProducer protonProducer : producers.values())
      {
         try
         {
            protonProducer.close();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorClosingSession(e);
         }
      }
      producers.clear();
      for (ProtonConsumer protonConsumer : consumers.values())
      {
         try
         {
            protonConsumer.close();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorClosingConsumer(e);
         }
      }
      consumers.clear();
      try
      {
         getServerSession().rollback(true);
         getServerSession().close(false);
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.errorClosingSession(e);
      }
      closed = true;
   }

   public void removeConsumer(long consumerID) throws HornetQAMQPException
   {
      consumers.remove(consumerID);
      try
      {
         getServerSession().closeConsumer(consumerID);
      }
      catch (Exception e)
      {
         throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorClosingConsumer(consumerID, e.getMessage());
      }
   }

   public void removeProducer(Receiver receiver)
   {
      producers.remove(receiver);
   }
}
