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
package org.apache.activemq.artemis.ra;

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
import javax.jms.XAQueueSession;
import javax.jms.XATopicSession;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;

/**
 * A joint interface for JMS sessions
 */
public class ActiveMQRASession implements QueueSession, TopicSession, XAQueueSession, XATopicSession {

   /**
    * The managed connection
    */
   private ActiveMQRAManagedConnection mc;

   /**
    * The connection request info
    */
   private final ActiveMQRAConnectionRequestInfo cri;

   /**
    * The session factory
    */
   private ActiveMQRASessionFactory sf;

   /**
    * The message consumers
    */
   private final Set<MessageConsumer> consumers;

   /**
    * The message producers
    */
   private final Set<MessageProducer> producers;

   /**
    * Constructor
    *
    * @param mc  The managed connection
    * @param cri The connection request info
    */
   public ActiveMQRASession(final ActiveMQRAManagedConnection mc, final ActiveMQRAConnectionRequestInfo cri) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + mc + ", " + cri + ")");
      }

      this.mc = mc;
      this.cri = cri;
      sf = null;
      consumers = new HashSet<>();
      producers = new HashSet<>();
   }

   /**
    * Set the session factory
    *
    * @param sf The session factory
    */
   public void setActiveMQSessionFactory(final ActiveMQRASessionFactory sf) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setActiveMQSessionFactory(" + sf + ")");
      }

      this.sf = sf;
   }

   /**
    * Lock
    *
    * @throws JMSException          Thrown if an error occurs
    * @throws IllegalStateException The session is closed
    */
   protected void lock() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("lock()");
      }

      final ActiveMQRAManagedConnection mcLocal = this.mc;
      if (mcLocal != null) {
         mcLocal.tryLock();
      } else {
         throw new IllegalStateException("Connection is not associated with a managed connection. " + this);
      }
   }

   /**
    * Unlock
    */
   protected void unlock() {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("unlock()");
      }

      final ActiveMQRAManagedConnection mcLocal = this.mc;
      if (mcLocal != null) {
         mcLocal.unlock();
      }

      // We recreate the lock when returned to the pool
      // so missing the unlock after disassociation is not important
   }

   /**
    * Create a bytes message
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public BytesMessage createBytesMessage() throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createBytesMessage" + session);
      }

      return session.createBytesMessage();
   }

   /**
    * Create a map message
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public MapMessage createMapMessage() throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createMapMessage(), " + session);
      }

      return session.createMapMessage();
   }

   /**
    * Create a message
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Message createMessage() throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createMessage" + session);
      }

      return session.createMessage();
   }

   /**
    * Create an object message
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ObjectMessage createObjectMessage() throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createObjectMessage" + session);
      }

      return session.createObjectMessage();
   }

   /**
    * Create an object message
    *
    * @param object The object
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ObjectMessage createObjectMessage(final Serializable object) throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createObjectMessage(" + object + ")" + session);
      }

      return session.createObjectMessage(object);
   }

   /**
    * Create a stream message
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public StreamMessage createStreamMessage() throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createStreamMessage" + session);
      }

      return session.createStreamMessage();
   }

   /**
    * Create a text message
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TextMessage createTextMessage() throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createTextMessage" + session);
      }

      return session.createTextMessage();
   }

   /**
    * Create a text message
    *
    * @param string The text
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TextMessage createTextMessage(final String string) throws JMSException {
      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createTextMessage(" + string + ")" + session);
      }

      return session.createTextMessage(string);
   }

   /**
    * Get transacted
    *
    * @return True if transacted; otherwise false
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean getTransacted() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getTransacted()");
      }

      getSessionInternal();
      return cri.isTransacted();
   }

   /**
    * Get the message listener -- throws IllegalStateException
    *
    * @return The message listener
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public MessageListener getMessageListener() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getMessageListener()");
      }

      throw new IllegalStateException("Method not allowed");
   }

   /**
    * Set the message listener -- Throws IllegalStateException
    *
    * @param listener The message listener
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setMessageListener(final MessageListener listener) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setMessageListener(" + listener + ")");
      }

      throw new IllegalStateException("Method not allowed");
   }

   /**
    * Always throws an Error.
    *
    * @throws Error Method not allowed.
    */
   @Override
   public void run() {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("run()");
      }

      throw new Error("Method not allowed");
   }

   /**
    * Closes the session. Sends a ConnectionEvent.CONNECTION_CLOSED to the
    * managed connection.
    *
    * @throws JMSException Failed to close session.
    */
   @Override
   public void close() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("close()");
      }

      sf.closeSession(this);
      closeSession();
   }

   /**
    * Commit
    *
    * @throws JMSException Failed to close session.
    */
   @Override
   public void commit() throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.XA_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION ||
         cri.getType() == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new TransactionInProgressException("XA connection");
      }

      lock();
      try {
         Session session = getSessionInternal();

         if (cri.isTransacted() == false) {
            throw new IllegalStateException("Session is not transacted");
         }

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("Commit session " + this);
         }

         session.commit();
      } finally {
         unlock();
      }
   }

   /**
    * Rollback
    *
    * @throws JMSException Failed to close session.
    */
   @Override
   public void rollback() throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.XA_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION ||
         cri.getType() == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new TransactionInProgressException("XA connection");
      }

      lock();
      try {
         Session session = getSessionInternal();

         if (cri.isTransacted() == false) {
            throw new IllegalStateException("Session is not transacted");
         }

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("Rollback session " + this);
         }

         session.rollback();
      } finally {
         unlock();
      }
   }

   /**
    * Recover
    *
    * @throws JMSException Failed to close session.
    */
   @Override
   public void recover() throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (cri.isTransacted()) {
            throw new IllegalStateException("Session is transacted");
         }

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("Recover session " + this);
         }

         session.recover();
      } finally {
         unlock();
      }
   }

   /**
    * Create a topic
    *
    * @param topicName The topic name
    * @return The topic
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Topic createTopic(final String topicName) throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION) {
         throw new IllegalStateException("Cannot create topic for javax.jms.QueueSession");
      }

      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createTopic " + session + " topicName=" + topicName);
      }

      Topic result = session.createTopic(topicName);

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createdTopic " + session + " topic=" + result);
      }

      return result;
   }

   /**
    * Create a topic subscriber
    *
    * @param topic The topic
    * @return The subscriber
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TopicSubscriber createSubscriber(final Topic topic) throws JMSException {
      lock();
      try {
         TopicSession session = getTopicSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSubscriber " + session + " topic=" + topic);
         }

         TopicSubscriber result = session.createSubscriber(topic);
         result = new ActiveMQRATopicSubscriber(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdSubscriber " + session + " ActiveMQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a topic subscriber
    *
    * @param topic           The topic
    * @param messageSelector The message selector
    * @param noLocal         If true inhibits the delivery of messages published by its own connection
    * @return The subscriber
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TopicSubscriber createSubscriber(final Topic topic,
                                           final String messageSelector,
                                           final boolean noLocal) throws JMSException {
      lock();
      try {
         TopicSession session = getTopicSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSubscriber " + session +
                                             " topic=" +
                                             topic +
                                             " selector=" +
                                             messageSelector +
                                             " noLocal=" +
                                             noLocal);
         }

         TopicSubscriber result = session.createSubscriber(topic, messageSelector, noLocal);
         result = new ActiveMQRATopicSubscriber(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdSubscriber " + session + " ActiveMQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a durable topic subscriber
    *
    * @param topic The topic
    * @param name  The name
    * @return The subscriber
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TopicSubscriber createDurableSubscriber(final Topic topic, final String name) throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION) {
         throw new IllegalStateException("Cannot create durable subscriber from javax.jms.QueueSession");
      }

      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createDurableSubscriber " + session + " topic=" + topic + " name=" + name);
         }

         TopicSubscriber result = session.createDurableSubscriber(topic, name);
         result = new ActiveMQRATopicSubscriber(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdDurableSubscriber " + session + " ActiveMQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a topic subscriber
    *
    * @param topic           The topic
    * @param name            The name
    * @param messageSelector The message selector
    * @param noLocal         If true inhibits the delivery of messages published by its own connection
    * @return The subscriber
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TopicSubscriber createDurableSubscriber(final Topic topic,
                                                  final String name,
                                                  final String messageSelector,
                                                  final boolean noLocal) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createDurableSubscriber " + session +
                                             " topic=" +
                                             topic +
                                             " name=" +
                                             name +
                                             " selector=" +
                                             messageSelector +
                                             " noLocal=" +
                                             noLocal);
         }

         TopicSubscriber result = session.createDurableSubscriber(topic, name, messageSelector, noLocal);
         result = new ActiveMQRATopicSubscriber(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdDurableSubscriber " + session + " ActiveMQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a topic publisher
    *
    * @param topic The topic
    * @return The publisher
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TopicPublisher createPublisher(final Topic topic) throws JMSException {
      lock();
      try {
         TopicSession session = getTopicSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createPublisher " + session + " topic=" + topic);
         }

         TopicPublisher result = session.createPublisher(topic);
         result = new ActiveMQRATopicPublisher(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdPublisher " + session + " publisher=" + result);
         }

         addProducer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a temporary topic
    *
    * @return The temporary topic
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TemporaryTopic createTemporaryTopic() throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION) {
         throw new IllegalStateException("Cannot create temporary topic for javax.jms.QueueSession");
      }

      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createTemporaryTopic " + session);
         }

         TemporaryTopic temp = session.createTemporaryTopic();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdTemporaryTopic " + session + " temp=" + temp);
         }

         sf.addTemporaryTopic(temp);

         return temp;
      } finally {
         unlock();
      }
   }

   /**
    * Unsubscribe
    *
    * @param name The name
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void unsubscribe(final String name) throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION) {
         throw new IllegalStateException("Cannot unsubscribe for javax.jms.QueueSession");
      }

      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("unsubscribe " + session + " name=" + name);
         }

         session.unsubscribe(name);
      } finally {
         unlock();
      }
   }

   /**
    * Create a browser
    *
    * @param queue The queue
    * @return The browser
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public QueueBrowser createBrowser(final Queue queue) throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new IllegalStateException("Cannot create browser for javax.jms.TopicSession");
      }

      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createBrowser " + session + " queue=" + queue);
      }

      QueueBrowser result = session.createBrowser(queue);

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createdBrowser " + session + " browser=" + result);
      }

      return result;
   }

   /**
    * Create a browser
    *
    * @param queue           The queue
    * @param messageSelector The message selector
    * @return The browser
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public QueueBrowser createBrowser(final Queue queue, final String messageSelector) throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new IllegalStateException("Cannot create browser for javax.jms.TopicSession");
      }

      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createBrowser " + session + " queue=" + queue + " selector=" + messageSelector);
      }

      QueueBrowser result = session.createBrowser(queue, messageSelector);

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createdBrowser " + session + " browser=" + result);
      }

      return result;
   }

   /**
    * Create a queue
    *
    * @param queueName The queue name
    * @return The queue
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Queue createQueue(final String queueName) throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new IllegalStateException("Cannot create browser or javax.jms.TopicSession");
      }

      Session session = getSessionInternal();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createQueue " + session + " queueName=" + queueName);
      }

      Queue result = session.createQueue(queueName);

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("createdQueue " + session + " queue=" + result);
      }

      return result;
   }

   /**
    * Create a queue receiver
    *
    * @param queue The queue
    * @return The queue receiver
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public QueueReceiver createReceiver(final Queue queue) throws JMSException {
      lock();
      try {
         QueueSession session = getQueueSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createReceiver " + session + " queue=" + queue);
         }

         QueueReceiver result = session.createReceiver(queue);
         result = new ActiveMQRAQueueReceiver(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdReceiver " + session + " receiver=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a queue receiver
    *
    * @param queue           The queue
    * @param messageSelector
    * @return The queue receiver
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public QueueReceiver createReceiver(final Queue queue, final String messageSelector) throws JMSException {
      lock();
      try {
         QueueSession session = getQueueSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createReceiver " + session + " queue=" + queue + " selector=" + messageSelector);
         }

         QueueReceiver result = session.createReceiver(queue, messageSelector);
         result = new ActiveMQRAQueueReceiver(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdReceiver " + session + " receiver=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a queue sender
    *
    * @param queue The queue
    * @return The queue sender
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public QueueSender createSender(final Queue queue) throws JMSException {
      lock();
      try {
         QueueSession session = getQueueSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSender " + session + " queue=" + queue);
         }

         QueueSender result = session.createSender(queue);
         result = new ActiveMQRAQueueSender(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdSender " + session + " sender=" + result);
         }

         addProducer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a temporary queue
    *
    * @return The temporary queue
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TemporaryQueue createTemporaryQueue() throws JMSException {
      if (cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new IllegalStateException("Cannot create temporary queue for javax.jms.TopicSession");
      }

      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createTemporaryQueue " + session);
         }

         TemporaryQueue temp = session.createTemporaryQueue();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdTemporaryQueue " + session + " temp=" + temp);
         }

         sf.addTemporaryQueue(temp);

         return temp;
      } finally {
         unlock();
      }
   }

   /**
    * Create a message consumer
    *
    * @param destination The destination
    * @return The message consumer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public MessageConsumer createConsumer(final Destination destination) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createConsumer " + session + " dest=" + destination);
         }

         MessageConsumer result = session.createConsumer(destination);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a message consumer
    *
    * @param destination     The destination
    * @param messageSelector The message selector
    * @return The message consumer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public MessageConsumer createConsumer(final Destination destination,
                                         final String messageSelector) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createConsumer " + session +
                                             " dest=" +
                                             destination +
                                             " messageSelector=" +
                                             messageSelector);
         }

         MessageConsumer result = session.createConsumer(destination, messageSelector);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a message consumer
    *
    * @param destination     The destination
    * @param messageSelector The message selector
    * @param noLocal         If true inhibits the delivery of messages published by its own connection
    * @return The message consumer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public MessageConsumer createConsumer(final Destination destination,
                                         final String messageSelector,
                                         final boolean noLocal) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createConsumer " + session +
                                             " dest=" +
                                             destination +
                                             " messageSelector=" +
                                             messageSelector +
                                             " noLocal=" +
                                             noLocal);
         }

         MessageConsumer result = session.createConsumer(destination, messageSelector, noLocal);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Create a message producer
    *
    * @param destination The destination
    * @return The message producer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public MessageProducer createProducer(final Destination destination) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createProducer " + session + " dest=" + destination);
         }

         MessageProducer result = session.createProducer(destination);
         result = new ActiveMQRAMessageProducer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdProducer " + session + " producer=" + result);
         }

         addProducer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Get the acknowledge mode
    *
    * @return The mode
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int getAcknowledgeMode() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getAcknowledgeMode()");
      }

      getSessionInternal();
      return cri.getAcknowledgeMode();
   }

   /**
    * Get the XA resource
    */
   @Override
   public XAResource getXAResource() {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getXAResource()");
      }

      if (cri.getType() == ActiveMQRAConnectionFactory.CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION ||
         cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION) {
         return null;
      }

      try {
         lock();

         return getXAResourceInternal();
      } catch (Throwable t) {
         return null;
      } finally {
         unlock();
      }
   }

   /**
    * Returns the ID of the Node that this session is associated with.
    *
    * @return Node ID
    */
   public String getNodeId() throws JMSException {
      ActiveMQSession session = (ActiveMQSession) getSessionInternal();
      ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal) session.getCoreSession().getSessionFactory();
      return factory.getLiveNodeId();
   }

   /**
    * Get the session
    *
    * @return The session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Session getSession() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getNonXAsession()");
      }

      if (cri.getType() == ActiveMQRAConnectionFactory.CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION ||
         cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION) {
         throw new IllegalStateException("Non XA connection");
      }

      lock();
      try {
         return this;
      } finally {
         unlock();
      }
   }

   /**
    * Get the queue session
    *
    * @return The queue session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public QueueSession getQueueSession() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getQueueSession()");
      }

      if (cri.getType() == ActiveMQRAConnectionFactory.CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION ||
         cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION) {
         throw new IllegalStateException("Non XA connection");
      }

      lock();
      try {
         return this;
      } finally {
         unlock();
      }
   }

   /**
    * Get the topic session
    *
    * @return The topic session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TopicSession getTopicSession() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getTopicSession()");
      }

      if (cri.getType() == ActiveMQRAConnectionFactory.CONNECTION || cri.getType() == ActiveMQRAConnectionFactory.QUEUE_CONNECTION ||
         cri.getType() == ActiveMQRAConnectionFactory.TOPIC_CONNECTION) {
         throw new IllegalStateException("Non XA connection");
      }

      lock();
      try {
         return this;
      } finally {
         unlock();
      }
   }

   @Override
   public MessageConsumer createSharedConsumer(final Topic topic,
                                               final String sharedSubscriptionName) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSharedConsumer " + session + " topic=" + topic + ", sharedSubscriptionName=" + sharedSubscriptionName);
         }

         MessageConsumer result = session.createSharedConsumer(topic, sharedSubscriptionName);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   @Override
   public MessageConsumer createSharedConsumer(final Topic topic,
                                               final String sharedSubscriptionName,
                                               final String messageSelector) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSharedConsumer " + session + " topic=" + topic +
                                             ", sharedSubscriptionName=" + sharedSubscriptionName + ", messageSelector=" + messageSelector);
         }

         MessageConsumer result = session.createSharedConsumer(topic, sharedSubscriptionName, messageSelector);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   @Override
   public MessageConsumer createDurableConsumer(final Topic topic, final String name) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSharedConsumer " + session + " topic=" + topic + ", name=" + name);
         }

         MessageConsumer result = session.createDurableConsumer(topic, name);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   @Override
   public MessageConsumer createDurableConsumer(Topic topic,
                                                String name,
                                                String messageSelector,
                                                boolean noLocal) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createDurableConsumer " + session + " topic=" + topic + ", name=" + name +
                                             ", messageSelector=" + messageSelector + ", noLocal=" + noLocal);
         }

         MessageConsumer result = session.createDurableConsumer(topic, name, messageSelector, noLocal);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   @Override
   public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSharedDurableConsumer " + session + " topic=" + topic + ", name=" +
                                             name);
         }

         MessageConsumer result = session.createSharedDurableConsumer(topic, name);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   @Override
   public MessageConsumer createSharedDurableConsumer(Topic topic,
                                                      String name,
                                                      String messageSelector) throws JMSException {
      lock();
      try {
         Session session = getSessionInternal();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createSharedDurableConsumer " + session + " topic=" + topic + ", name=" +
                                             name + ", messageSelector=" + messageSelector);
         }

         MessageConsumer result = session.createSharedDurableConsumer(topic, name, messageSelector);
         result = new ActiveMQRAMessageConsumer(result, this);

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      } finally {
         unlock();
      }
   }

   /**
    * Set the managed connection
    *
    * @param managedConnection The managed connection
    */
   void setManagedConnection(final ActiveMQRAManagedConnection managedConnection) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setManagedConnection(" + managedConnection + ")");
      }

      if (mc != null) {
         mc.removeHandle(this);
      }

      mc = managedConnection;
   }

   /**
    * for tests only
    */
   public ManagedConnection getManagedConnection() {
      return mc;
   }

   /**
    * Destroy
    */
   void destroy() {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("destroy()");
      }

      mc = null;
   }

   /**
    * Start
    *
    * @throws JMSException Thrown if an error occurs
    */
   void start() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("start()");
      }

      if (mc != null) {
         mc.start();
      }
   }

   /**
    * Stop
    *
    * @throws JMSException Thrown if an error occurs
    */
   void stop() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("stop()");
      }

      if (mc != null) {
         mc.stop();
      }
   }

   /**
    * Check strict
    *
    * @throws JMSException Thrown if an error occurs
    */
   void checkStrict() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("checkStrict()");
      }

      if (mc != null) {
         throw new IllegalStateException(ActiveMQRASessionFactory.ISE);
      }
   }

   /**
    * Close session
    *
    * @throws JMSException Thrown if an error occurs
    */
   void closeSession() throws JMSException {
      if (mc != null) {
         ActiveMQRALogger.LOGGER.trace("Closing session");

         try {
            mc.stop();
         } catch (Throwable t) {
            ActiveMQRALogger.LOGGER.trace("Error stopping managed connection", t);
         }

         synchronized (consumers) {
            for (Iterator<MessageConsumer> i = consumers.iterator(); i.hasNext(); ) {
               ActiveMQRAMessageConsumer consumer = (ActiveMQRAMessageConsumer) i.next();
               try {
                  consumer.closeConsumer();
               } catch (Throwable t) {
                  ActiveMQRALogger.LOGGER.trace("Error closing consumer", t);
               }
               i.remove();
            }
         }

         synchronized (producers) {
            for (Iterator<MessageProducer> i = producers.iterator(); i.hasNext(); ) {
               ActiveMQRAMessageProducer producer = (ActiveMQRAMessageProducer) i.next();
               try {
                  producer.closeProducer();
               } catch (Throwable t) {
                  ActiveMQRALogger.LOGGER.trace("Error closing producer", t);
               }
               i.remove();
            }
         }

         mc.removeHandle(this);
         ConnectionEvent ev = new ConnectionEvent(mc, ConnectionEvent.CONNECTION_CLOSED);
         ev.setConnectionHandle(this);
         mc.sendEvent(ev);
         mc = null;
      }
   }

   /**
    * Add consumer
    *
    * @param consumer The consumer
    */
   void addConsumer(final MessageConsumer consumer) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("addConsumer(" + consumer + ")");
      }

      synchronized (consumers) {
         consumers.add(consumer);
      }
   }

   /**
    * Remove consumer
    *
    * @param consumer The consumer
    */
   void removeConsumer(final MessageConsumer consumer) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("removeConsumer(" + consumer + ")");
      }

      synchronized (consumers) {
         consumers.remove(consumer);
      }
   }

   /**
    * Add producer
    *
    * @param producer The producer
    */
   void addProducer(final MessageProducer producer) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("addProducer(" + producer + ")");
      }

      synchronized (producers) {
         producers.add(producer);
      }
   }

   /**
    * Remove producer
    *
    * @param producer The producer
    */
   void removeProducer(final MessageProducer producer) {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("removeProducer(" + producer + ")");
      }

      synchronized (producers) {
         producers.remove(producer);
      }
   }

   /**
    * Get the session and ensure that it is open
    *
    * @return The session
    * @throws JMSException          Thrown if an error occurs
    * @throws IllegalStateException The session is closed
    */
   Session getSessionInternal() throws JMSException {
      if (mc == null) {
         throw new IllegalStateException("The session is closed");
      }

      Session session = mc.getSession();

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getSessionInternal " + session + " for " + this);
      }

      return session;
   }

   /**
    * Get the XA resource and ensure that it is open
    *
    * @return The XA Resource
    * @throws JMSException          Thrown if an error occurs
    * @throws IllegalStateException The session is closed
    */
   XAResource getXAResourceInternal() throws JMSException {
      if (mc == null) {
         throw new IllegalStateException("The session is closed");
      }

      try {
         XAResource xares = mc.getXAResource();

         if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
            ActiveMQRALogger.LOGGER.trace("getXAResourceInternal " + xares + " for " + this);
         }

         return xares;
      } catch (ResourceException e) {
         JMSException jmse = new JMSException("Unable to get XA Resource");
         jmse.initCause(e);
         throw jmse;
      }
   }

   /**
    * Get the queue session
    *
    * @return The queue session
    * @throws JMSException          Thrown if an error occurs
    * @throws IllegalStateException The session is closed
    */
   QueueSession getQueueSessionInternal() throws JMSException {
      Session s = getSessionInternal();
      if (!(s instanceof QueueSession)) {
         throw new InvalidDestinationException("Attempting to use QueueSession methods on: " + this);
      }
      return (QueueSession) s;
   }

   /**
    * Get the topic session
    *
    * @return The topic session
    * @throws JMSException          Thrown if an error occurs
    * @throws IllegalStateException The session is closed
    */
   TopicSession getTopicSessionInternal() throws JMSException {
      Session s = getSessionInternal();
      if (!(s instanceof TopicSession)) {
         throw new InvalidDestinationException("Attempting to use TopicSession methods on: " + this);
      }
      return (TopicSession) s;
   }

   public void checkState() throws JMSException {
      if (mc != null) {
         mc.checkTransactionActive();
      }
   }
}
