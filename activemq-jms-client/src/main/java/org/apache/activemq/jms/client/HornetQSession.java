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
package org.apache.activemq6.jms.client;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.TransactionInProgressException;
import javax.transaction.xa.XAResource;

import org.apache.activemq6.selector.filter.FilterException;
import org.apache.activemq6.selector.SelectorParser;
import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQQueueExistsException;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientProducer;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSession.AddressQuery;
import org.apache.activemq6.api.core.client.ClientSession.QueueQuery;

/**
 * HornetQ implementation of a JMS Session.
 * <br>
 * Note that we *do not* support JMS ASF (Application Server Facilities) optional
 * constructs such as ConnectionConsumer
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 *
 */
public class HornetQSession implements QueueSession, TopicSession
{
   public static final int TYPE_GENERIC_SESSION = 0;

   public static final int TYPE_QUEUE_SESSION = 1;

   public static final int TYPE_TOPIC_SESSION = 2;

   private static SimpleString REJECTING_FILTER = new SimpleString("_HQX=-1");

   private final HornetQConnection connection;

   private final ClientSession session;

   private final int sessionType;

   private final int ackMode;

   private final boolean transacted;

   private final boolean xa;

   private boolean recoverCalled;

   private final Set<HornetQMessageConsumer> consumers = new HashSet<HornetQMessageConsumer>();

   // Constructors --------------------------------------------------

   protected HornetQSession(final HornetQConnection connection,
                            final boolean transacted,
                            final boolean xa,
                            final int ackMode,
                            final ClientSession session,
                            final int sessionType)
   {
      this.connection = connection;

      this.ackMode = ackMode;

      this.session = session;

      this.sessionType = sessionType;

      this.transacted = transacted;

      this.xa = xa;
   }

   // Session implementation ----------------------------------------

   public BytesMessage createBytesMessage() throws JMSException
   {
      checkClosed();

      return new HornetQBytesMessage(session);
   }

   public MapMessage createMapMessage() throws JMSException
   {
      checkClosed();

      return new HornetQMapMessage(session);
   }

   public Message createMessage() throws JMSException
   {
      checkClosed();

      return new HornetQMessage(session);
   }

   public ObjectMessage createObjectMessage() throws JMSException
   {
      checkClosed();

      return new HornetQObjectMessage(session);
   }

   public ObjectMessage createObjectMessage(final Serializable object) throws JMSException
   {
      checkClosed();

      HornetQObjectMessage msg = new HornetQObjectMessage(session);

      msg.setObject(object);

      return msg;
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
      checkClosed();

      return new HornetQStreamMessage(session);
   }

   public TextMessage createTextMessage() throws JMSException
   {
      checkClosed();

      HornetQTextMessage msg = new HornetQTextMessage(session);

      msg.setText(null);

      return msg;
   }

   public TextMessage createTextMessage(final String text) throws JMSException
   {
      checkClosed();

      HornetQTextMessage msg = new HornetQTextMessage(session);

      msg.setText(text);

      return msg;
   }

   public boolean getTransacted() throws JMSException
   {
      checkClosed();

      return transacted;
   }

   public int getAcknowledgeMode() throws JMSException
   {
      checkClosed();

      return ackMode;
   }

   public boolean isXA()
   {
      return xa;
   }

   public void commit() throws JMSException
   {
      if (!transacted)
      {
         throw new IllegalStateException("Cannot commit a non-transacted session");
      }
      if (xa)
      {
         throw new TransactionInProgressException("Cannot call commit on an XA session");
      }
      try
      {
         session.commit();
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void rollback() throws JMSException
   {
      if (!transacted)
      {
         throw new IllegalStateException("Cannot rollback a non-transacted session");
      }
      if (xa)
      {
         throw new TransactionInProgressException("Cannot call rollback on an XA session");
      }

      try
      {
         session.rollback();
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void close() throws JMSException
   {
      connection.getThreadAwareContext().assertNotCompletionListenerThread();
      connection.getThreadAwareContext().assertNotMessageListenerThread();
      synchronized (connection)
      {
         try
         {
            for (HornetQMessageConsumer cons : new HashSet<HornetQMessageConsumer>(consumers))
            {
               cons.close();
            }

            session.close();

            connection.removeSession(this);
         }
         catch (HornetQException e)
         {
            throw JMSExceptionHelper.convertFromHornetQException(e);
         }
      }
   }

   public void recover() throws JMSException
   {
      if (transacted)
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }

      try
      {
         session.rollback(true);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }

      recoverCalled = true;
   }

   public MessageListener getMessageListener() throws JMSException
   {
      checkClosed();

      return null;
   }

   public void setMessageListener(final MessageListener listener) throws JMSException
   {
      checkClosed();
   }

   public void run()
   {
   }

   public MessageProducer createProducer(final Destination destination) throws JMSException
   {
      if (destination != null && !(destination instanceof HornetQDestination))
      {
         throw new InvalidDestinationException("Not a HornetQ Destination:" + destination);
      }

      try
      {
         HornetQDestination jbd = (HornetQDestination)destination;

         if (jbd != null)
         {
            ClientSession.AddressQuery response = session.addressQuery(jbd.getSimpleAddress());

            if (!response.isExists())
            {
               throw new InvalidDestinationException("Destination " + jbd.getName() + " does not exist");
            }

            connection.addKnownDestination(jbd.getSimpleAddress());
         }

         ClientProducer producer = session.createProducer(jbd == null ? null : jbd.getSimpleAddress());

         return new HornetQMessageProducer(connection, producer, jbd, session);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public MessageConsumer createConsumer(final Destination destination) throws JMSException
   {
      return createConsumer(destination, null, false);
   }

   public MessageConsumer createConsumer(final Destination destination, final String messageSelector) throws JMSException
   {
      return createConsumer(destination, messageSelector, false);
   }

   public MessageConsumer createConsumer(final Destination destination,
                                         final String messageSelector,
                                         final boolean noLocal) throws JMSException
   {
      if (destination == null)
      {
         throw new InvalidDestinationException("Cannot create a consumer with a null destination");
      }

      if (!(destination instanceof HornetQDestination))
      {
         throw new InvalidDestinationException("Not a HornetQDestination:" + destination);
      }

      HornetQDestination jbdest = (HornetQDestination)destination;

      if (jbdest.isTemporary() && !connection.containsTemporaryQueue(jbdest.getSimpleAddress()))
      {
         throw new JMSException("Can not create consumer for temporary destination " + destination +
                                " from another JMS connection");
      }

      return createConsumer(jbdest, null, messageSelector, noLocal, ConsumerDurability.NON_DURABLE);
   }

   public Queue createQueue(final String queueName) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == HornetQSession.TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a queue using a TopicSession");
      }

      try
      {
         HornetQQueue queue = lookupQueue(queueName, false);

         if (queue == null)
         {
            queue = lookupQueue(queueName, true);
         }

         if (queue == null)
         {
            throw new JMSException("There is no queue with name " + queueName);
         }
         else
         {
            return queue;
         }
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public Topic createTopic(final String topicName) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == HornetQSession.TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a topic on a QueueSession");
      }

      try
      {
         HornetQTopic topic = lookupTopic(topicName, false);

         if (topic == null)
         {
            topic = lookupTopic(topicName, true);
         }

         if (topic == null)
         {
            throw new JMSException("There is no topic with name " + topicName);
         }
         else
         {
            return topic;
         }
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public TopicSubscriber createDurableSubscriber(final Topic topic, final String name) throws JMSException
   {
      return createDurableSubscriber(topic, name, null, false);
   }

   public TopicSubscriber createDurableSubscriber(final Topic topic,
                                                  final String name,
                                                  String messageSelector,
                                                  final boolean noLocal) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == HornetQSession.TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a durable subscriber on a QueueSession");
      }
      checkTopic(topic);
      if (!(topic instanceof HornetQDestination))
      {
         throw new InvalidDestinationException("Not a HornetQTopic:" + topic);
      }
      if ("".equals(messageSelector))
      {
         messageSelector = null;
      }

      HornetQDestination jbdest = (HornetQDestination)topic;

      if (jbdest.isQueue())
      {
         throw new InvalidDestinationException("Cannot create a subscriber on a queue");
      }

      return createConsumer(jbdest, name, messageSelector, noLocal, ConsumerDurability.DURABLE);
   }

   private void checkTopic(Topic topic) throws InvalidDestinationException
   {
      if (topic == null)
      {
         throw HornetQJMSClientBundle.BUNDLE.nullTopic();
      }
   }

   @Override
   public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException
   {
      return createSharedConsumer(topic, sharedSubscriptionName, null);
   }

   /**
    * Note: Needs to throw an exception if a subscriptionName is already in use by another topic, or if the messageSelector is different
    *
    * validate multiple subscriptions on the same session.
    * validate multiple subscriptions on different sessions
    * validate failure in one connection while another connection stills fine.
    * Validate different filters in different possible scenarios
    *
    * @param topic
    * @param name
    * @param messageSelector
    * @return
    * @throws JMSException
    */
   @Override
   public MessageConsumer createSharedConsumer(Topic topic, String name, String messageSelector) throws JMSException
   {
      if (sessionType == HornetQSession.TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a shared consumer on a QueueSession");
      }
      checkTopic(topic);
      HornetQTopic localTopic;
      if (topic instanceof HornetQTopic)
      {
         localTopic = (HornetQTopic)topic;
      }
      else
      {
         localTopic = new HornetQTopic(topic.getTopicName());
      }
      return internalCreateSharedConsumer(localTopic, name, messageSelector, ConsumerDurability.NON_DURABLE, true);
   }

   @Override
   public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException
   {
      return createDurableConsumer(topic, name, null, false);
   }

   @Override
   public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException
   {
      if (sessionType == HornetQSession.TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a durable consumer on a QueueSession");
      }
      checkTopic(topic);
      HornetQTopic localTopic;
      if (topic instanceof HornetQTopic)
      {
         localTopic = (HornetQTopic)topic;
      }
      else
      {
         localTopic = new HornetQTopic(topic.getTopicName());
      }
      return createConsumer(localTopic, name, messageSelector, noLocal, ConsumerDurability.DURABLE);
   }

   @Override
   public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException
   {
      return createSharedDurableConsumer(topic, name, null);
   }

   @Override
   public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException
   {
      if (sessionType == HornetQSession.TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a shared durable consumer on a QueueSession");
      }

      checkTopic(topic);

      HornetQTopic localTopic;

      if (topic instanceof HornetQTopic)
      {
         localTopic = (HornetQTopic)topic;
      }
      else
      {
         localTopic = new HornetQTopic(topic.getTopicName());
      }
      return internalCreateSharedConsumer(localTopic, name, messageSelector, ConsumerDurability.DURABLE, true);
   }

   enum ConsumerDurability
   {
      DURABLE, NON_DURABLE;
   }


   /**
    * This is an internal method for shared consumers
    */
   private HornetQMessageConsumer internalCreateSharedConsumer(final HornetQDestination dest,
                                                               final String subscriptionName,
                                                               String selectorString,
                                                               ConsumerDurability durability,
                                                               final boolean shared) throws JMSException
   {
      try
      {

         if (dest.isQueue())
         {
            // This is not really possible unless someone makes a mistake on code
            // createSharedConsumer only accpets Topics by declaration
            throw new RuntimeException("Internal error: createSharedConsumer is only meant for Topics");
         }

         if (subscriptionName == null)
         {
            throw HornetQJMSClientBundle.BUNDLE.invalidSubscriptionName();
         }

         selectorString = "".equals(selectorString) ? null : selectorString;

         SimpleString coreFilterString = null;

         if (selectorString != null)
         {
            coreFilterString = new SimpleString(SelectorTranslator.convertToHornetQFilterString(selectorString));
         }

         ClientConsumer consumer;

         SimpleString autoDeleteQueueName = null;

         AddressQuery response = session.addressQuery(dest.getSimpleAddress());

         if (!response.isExists())
         {
            throw HornetQJMSClientBundle.BUNDLE.destinationDoesNotExist(dest.getSimpleAddress());
         }

         SimpleString queueName;

         if (dest.isTemporary() && durability == ConsumerDurability.DURABLE)
         {
            throw new InvalidDestinationException("Cannot create a durable subscription on a temporary topic");
         }

         queueName = new SimpleString(HornetQDestination.createQueueNameForDurableSubscription(durability == ConsumerDurability.DURABLE, connection.getClientID(),
                                                                                               subscriptionName));

         if (durability == ConsumerDurability.DURABLE)
         {
            try
            {
               session.createSharedQueue(dest.getSimpleAddress(), queueName, coreFilterString, true);
            }
            catch (HornetQQueueExistsException ignored)
            {
               // We ignore this because querying and then creating the queue wouldn't be idempotent
               // we could also add a parameter to ignore existence what would require a bigger work around to avoid
               // compatibility.
            }
         }
         else
         {
            session.createSharedQueue(dest.getSimpleAddress(), queueName, coreFilterString, false);
         }

         consumer = session.createConsumer(queueName, null, false);

         HornetQMessageConsumer jbc = new HornetQMessageConsumer(connection, this,
                                                                 consumer,
                                                                 false,
                                                                 dest,
                                                                 selectorString,
                                                                 autoDeleteQueueName);

         consumers.add(jbc);

         return jbc;
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }



   private HornetQMessageConsumer createConsumer(final HornetQDestination dest,
                                                 final String subscriptionName,
                                                 String selectorString, final boolean noLocal,
                                                 ConsumerDurability durability) throws JMSException
   {
      try
      {
         selectorString = "".equals(selectorString) ? null : selectorString;

         if (noLocal)
         {
            connection.setHasNoLocal();

            String filter;
            if (connection.getClientID() != null)
            {
               filter =
                        HornetQConnection.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + connection.getClientID() +
                                 "'";
            }
            else
            {
               filter = HornetQConnection.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + connection.getUID() + "'";
            }

            if (selectorString != null)
            {
               selectorString += " AND " + filter;
            }
            else
            {
               selectorString = filter;
            }
         }

         SimpleString coreFilterString = null;

         if (selectorString != null)
         {
            coreFilterString = new SimpleString(SelectorTranslator.convertToHornetQFilterString(selectorString));
         }

         ClientConsumer consumer;

         SimpleString autoDeleteQueueName = null;

         if (dest.isQueue())
         {
            AddressQuery response = session.addressQuery(dest.getSimpleAddress());

            if (!response.isExists())
            {
               throw new InvalidDestinationException("Queue " + dest.getName() + " does not exist");
            }

            connection.addKnownDestination(dest.getSimpleAddress());

            consumer = session.createConsumer(dest.getSimpleAddress(), coreFilterString, false);
         }
         else
         {
            AddressQuery response = session.addressQuery(dest.getSimpleAddress());

            if (!response.isExists())
            {
               throw new InvalidDestinationException("Topic " + dest.getName() + " does not exist");
            }

            connection.addKnownDestination(dest.getSimpleAddress());

            SimpleString queueName;

            if (subscriptionName == null)
            {
               if (durability != ConsumerDurability.NON_DURABLE)
                  throw new RuntimeException();
               // Non durable sub

               queueName = new SimpleString(UUID.randomUUID().toString());

               session.createTemporaryQueue(dest.getSimpleAddress(), queueName, coreFilterString);

               consumer = session.createConsumer(queueName, null, false);

               autoDeleteQueueName = queueName;
            }
            else
            {
               // Durable sub
               if (durability != ConsumerDurability.DURABLE)
                  throw new RuntimeException();
               if (connection.getClientID() == null)
               {
                  throw new IllegalStateException("Cannot create durable subscription - client ID has not been set");
               }

               if (dest.isTemporary())
               {
                  throw new InvalidDestinationException("Cannot create a durable subscription on a temporary topic");
               }

               queueName = new SimpleString(HornetQDestination.createQueueNameForDurableSubscription(true, connection.getClientID(),
                                                                                                     subscriptionName));

               QueueQuery subResponse = session.queueQuery(queueName);

               if (!subResponse.isExists())
               {
                  session.createQueue(dest.getSimpleAddress(), queueName, coreFilterString, true);
               }
               else
               {
                  // Already exists
                  if (subResponse.getConsumerCount() > 0)
                  {
                     throw new IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
                  }

                  // From javax.jms.Session Javadoc (and also JMS 1.1 6.11.1):
                  // A client can change an existing durable subscription by
                  // creating a durable
                  // TopicSubscriber with the same name and a new topic and/or
                  // message selector.
                  // Changing a durable subscriber is equivalent to
                  // unsubscribing (deleting) the old
                  // one and creating a new one.

                  SimpleString oldFilterString = subResponse.getFilterString();

                  boolean selectorChanged = coreFilterString == null && oldFilterString != null ||
                                            oldFilterString == null &&
                                            coreFilterString != null ||
                                            oldFilterString != null &&
                                            coreFilterString != null &&
                                            !oldFilterString.equals(coreFilterString);

                  SimpleString oldTopicName = subResponse.getAddress();

                  boolean topicChanged = !oldTopicName.equals(dest.getSimpleAddress());

                  if (selectorChanged || topicChanged)
                  {
                     // Delete the old durable sub
                     session.deleteQueue(queueName);

                     // Create the new one
                     session.createQueue(dest.getSimpleAddress(), queueName, coreFilterString, true);
                  }
               }

               consumer = session.createConsumer(queueName, null, false);
            }
         }

         HornetQMessageConsumer jbc = new HornetQMessageConsumer(connection,
                                                                 this,
                                                                 consumer,
                                                                 noLocal,
                                                                 dest,
                                                                 selectorString,
                                                                 autoDeleteQueueName);

         consumers.add(jbc);

         return jbc;
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void ackAllConsumers() throws JMSException
   {
      checkClosed();
   }

   public QueueBrowser createBrowser(final Queue queue) throws JMSException
   {
      return createBrowser(queue, null);
   }

   public QueueBrowser createBrowser(final Queue queue, String filterString) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == HornetQSession.TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a browser on a TopicSession");
      }
      if (queue == null)
      {
         throw new InvalidDestinationException("Cannot create a browser with a null queue");
      }
      if (!(queue instanceof HornetQDestination))
      {
         throw new InvalidDestinationException("Not a HornetQQueue:" + queue);
      }
      if ("".equals(filterString))
      {
         filterString = null;
      }

      // eager test of the filter syntax as required by JMS spec
      try
      {
         if (filterString != null)
         {
            SelectorParser.parse(filterString.trim());
         }
      }
      catch (FilterException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(HornetQJMSClientBundle.BUNDLE.invalidFilter(e, new SimpleString(filterString)));
      }

      HornetQDestination jbq = (HornetQDestination)queue;

      if (!jbq.isQueue())
      {
         throw new InvalidDestinationException("Cannot create a browser on a topic");
      }

      try
      {
         AddressQuery message = session.addressQuery(new SimpleString(jbq.getAddress()));
         if (!message.isExists())
         {
            throw new InvalidDestinationException(jbq.getAddress() + " does not exist");
         }
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }

      return new HornetQQueueBrowser((HornetQQueue)jbq, filterString, session);

   }

   public TemporaryQueue createTemporaryQueue() throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == HornetQSession.TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary queue using a TopicSession");
      }

      try
      {
         HornetQTemporaryQueue queue = HornetQDestination.createTemporaryQueue(this);

         SimpleString simpleAddress = queue.getSimpleAddress();

         session.createTemporaryQueue(simpleAddress, simpleAddress);

         connection.addTemporaryQueue(simpleAddress);

         return queue;
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public TemporaryTopic createTemporaryTopic() throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == HornetQSession.TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary topic on a QueueSession");
      }

      try
      {
         HornetQTemporaryTopic topic = HornetQDestination.createTemporaryTopic(this);

         SimpleString simpleAddress = topic.getSimpleAddress();

         // We create a dummy subscription on the topic, that never receives messages - this is so we can perform JMS
         // checks when routing messages to a topic that
         // does not exist - otherwise we would not be able to distinguish from a non existent topic and one with no
         // subscriptions - core has no notion of a topic

         session.createTemporaryQueue(simpleAddress, simpleAddress, HornetQSession.REJECTING_FILTER);

         connection.addTemporaryQueue(simpleAddress);

         return topic;
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void unsubscribe(final String name) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == HornetQSession.TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot unsubscribe using a QueueSession");
      }

      SimpleString queueName = new SimpleString(HornetQDestination.createQueueNameForDurableSubscription(true, connection.getClientID(),
                                                                                                         name));

      try
      {
         QueueQuery response = session.queueQuery(queueName);

         if (!response.isExists())
         {
            throw new InvalidDestinationException("Cannot unsubscribe, subscription with name " + name +
                                                  " does not exist");
         }

         if (response.getConsumerCount() != 0)
         {
            throw new IllegalStateException("Cannot unsubscribe durable subscription " + name +
                                            " since it has active subscribers");
         }

         session.deleteQueue(queueName);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   // XASession implementation

   public Session getSession() throws JMSException
   {
      if (!xa)
      {
         throw new IllegalStateException("Isn't an XASession");
      }

      return this;
   }

   public XAResource getXAResource()
   {
      return session.getXAResource();
   }

   // QueueSession implementation

   public QueueReceiver createReceiver(final Queue queue, final String messageSelector) throws JMSException
   {
      return (QueueReceiver)createConsumer(queue, messageSelector);
   }

   public QueueReceiver createReceiver(final Queue queue) throws JMSException
   {
      return (QueueReceiver)createConsumer(queue);
   }

   public QueueSender createSender(final Queue queue) throws JMSException
   {
      return (QueueSender)createProducer(queue);
   }

   // XAQueueSession implementation

   public QueueSession getQueueSession() throws JMSException
   {
      return (QueueSession)getSession();
   }

   // TopicSession implementation

   public TopicPublisher createPublisher(final Topic topic) throws JMSException
   {
      return (TopicPublisher)createProducer(topic);
   }

   public TopicSubscriber createSubscriber(final Topic topic, final String messageSelector, final boolean noLocal) throws JMSException
   {
      return (TopicSubscriber)createConsumer(topic, messageSelector, noLocal);
   }

   public TopicSubscriber createSubscriber(final Topic topic) throws JMSException
   {
      return (TopicSubscriber)createConsumer(topic);
   }

   // XATopicSession implementation

   public TopicSession getTopicSession() throws JMSException
   {
      return (TopicSession)getSession();
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return "HornetQSession->" + session;
   }

   public ClientSession getCoreSession()
   {
      return session;
   }

   public boolean isRecoverCalled()
   {
      return recoverCalled;
   }

   public void setRecoverCalled(final boolean recoverCalled)
   {
      this.recoverCalled = recoverCalled;
   }

   public void deleteTemporaryTopic(final HornetQDestination tempTopic) throws JMSException
   {
      if (!tempTopic.isTemporary())
      {
         throw new InvalidDestinationException("Not a temporary topic " + tempTopic);
      }

      try
      {
         AddressQuery response = session.addressQuery(tempTopic.getSimpleAddress());

         if (!response.isExists())
         {
            throw new InvalidDestinationException("Cannot delete temporary topic " + tempTopic.getName() +
                                                  " does not exist");
         }

         if (response.getQueueNames().size() > 1)
         {
            throw new IllegalStateException("Cannot delete temporary topic " + tempTopic.getName() +
                                            " since it has subscribers");
         }

         SimpleString address = tempTopic.getSimpleAddress();

         session.deleteQueue(address);

         connection.removeTemporaryQueue(address);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void deleteTemporaryQueue(final HornetQDestination tempQueue) throws JMSException
   {
      if (!tempQueue.isTemporary())
      {
         throw new InvalidDestinationException("Not a temporary queue " + tempQueue);
      }
      try
      {
         QueueQuery response = session.queueQuery(tempQueue.getSimpleAddress());

         if (!response.isExists())
         {
            throw new InvalidDestinationException("Cannot delete temporary queue " + tempQueue.getName() +
                                                  " does not exist");
         }

         if (response.getConsumerCount() > 0)
         {
            throw new IllegalStateException("Cannot delete temporary queue " + tempQueue.getName() +
                                            " since it has subscribers");
         }

         SimpleString address = tempQueue.getSimpleAddress();

         session.deleteQueue(address);

         connection.removeTemporaryQueue(address);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void start() throws JMSException
   {
      try
      {
         session.start();
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void stop() throws JMSException
   {
      try
      {
         session.stop();
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void removeConsumer(final HornetQMessageConsumer consumer)
   {
      consumers.remove(consumer);
   }

   // Package protected ---------------------------------------------

   void deleteQueue(final SimpleString queueName) throws JMSException
   {
      if (!session.isClosed())
      {
         try
         {
            session.deleteQueue(queueName);
         }
         catch (HornetQException ignore)
         {
            // Exception on deleting queue shouldn't prevent close from completing
         }
      }
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkClosed() throws JMSException
   {
      if (session.isClosed())
      {
         throw new IllegalStateException("Session is closed");
      }
   }

   private HornetQQueue lookupQueue(final String queueName, boolean isTemporary) throws HornetQException
   {
      HornetQQueue queue;

      if (isTemporary)
      {
         queue = HornetQDestination.createTemporaryQueue(queueName);
      }
      else
      {
         queue = HornetQDestination.createQueue(queueName);
      }

      QueueQuery response = session.queueQuery(queue.getSimpleAddress());

      if (response.isExists())
      {
         return queue;
      }
      else
      {
         return null;
      }
   }

   private HornetQTopic lookupTopic(final String topicName, final boolean isTemporary) throws HornetQException
   {

      HornetQTopic topic;

      if (isTemporary)
      {
         topic = HornetQDestination.createTemporaryTopic(topicName);
      }
      else
      {
         topic = HornetQDestination.createTopic(topicName);
      }

      AddressQuery query = session.addressQuery(topic.getSimpleAddress());

      if (!query.isExists())
      {
         return null;
      }
      else
      {
         return topic;
      }
   }

   // Inner classes -------------------------------------------------

}
