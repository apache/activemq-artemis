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

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.XAJMSContext;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;
import javax.naming.Reference;
import javax.resource.Referenceable;
import javax.resource.spi.ConnectionManager;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionForContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionForContextImpl;

/**
 * Implements the JMS Connection API and produces {@link ActiveMQRASession} objects.
 */
public final class ActiveMQRASessionFactoryImpl extends ActiveMQConnectionForContextImpl implements ActiveMQRASessionFactory, ActiveMQConnectionForContext, Referenceable {

   /**
    * Trace enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * Are we closed?
    */
   private boolean closed = false;

   /**
    * The naming reference
    */
   private Reference reference;

   /**
    * The user name
    */
   private String userName;

   /**
    * The password
    */
   private String password;

   /**
    * The client ID
    */
   private String clientID;

   /**
    * The connection type
    */
   private final int type;

   /**
    * Whether we are started
    */
   private boolean started = false;

   /**
    * The managed connection factory
    */
   private final ActiveMQRAManagedConnectionFactory mcf;
   private TransactionManager tm;

   /**
    * The connection manager
    */
   private ConnectionManager cm;

   /**
    * The sessions
    */
   private final Set<ActiveMQRASession> sessions = new HashSet<>();

   /**
    * The temporary queues
    */
   private final Set<TemporaryQueue> tempQueues = new HashSet<>();

   /**
    * The temporary topics
    */
   private final Set<TemporaryTopic> tempTopics = new HashSet<>();

   /**
    * Constructor
    *
    * @param mcf  The managed connection factory
    * @param cm   The connection manager
    * @param type The connection type
    */
   public ActiveMQRASessionFactoryImpl(final ActiveMQRAManagedConnectionFactory mcf,
                                       final ConnectionManager cm,
                                       final TransactionManager tm,
                                       final int type) {
      this.mcf = mcf;

      this.tm = tm;

      if (cm == null) {
         this.cm = new ActiveMQRAConnectionManager();
      } else {
         this.cm = cm;
      }

      this.type = type;

      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + mcf + ", " + cm + ", " + type);
      }
   }

   @Override
   public JMSContext createContext(int sessionMode) {
      boolean inJtaTx = inJtaTransaction();
      int sessionModeToUse;
      switch (sessionMode) {
         case Session.AUTO_ACKNOWLEDGE:
         case Session.DUPS_OK_ACKNOWLEDGE:
         case ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE:
         case ActiveMQJMSConstants.PRE_ACKNOWLEDGE:
            sessionModeToUse = sessionMode;
            break;
         //these are prohibited in JEE unless not in a JTA tx where they should be ignored and auto_ack used
         case Session.CLIENT_ACKNOWLEDGE:
            if (!inJtaTx) {
               throw ActiveMQRABundle.BUNDLE.invalidSessionTransactedModeRuntime();
            }
            sessionModeToUse = Session.AUTO_ACKNOWLEDGE;
            break;
         case Session.SESSION_TRANSACTED:
            if (!inJtaTx) {
               throw ActiveMQRABundle.BUNDLE.invalidClientAcknowledgeModeRuntime();
            }
            sessionModeToUse = Session.AUTO_ACKNOWLEDGE;
            break;
         default:
            throw ActiveMQRABundle.BUNDLE.invalidAcknowledgeMode(sessionMode);
      }
      incrementRefCounter();

      return new ActiveMQRAJMSContext(this, sessionModeToUse, threadAwareContext);
   }

   @Override
   public XAJMSContext createXAContext() {
      incrementRefCounter();

      return new ActiveMQRAXAJMSContext(this, threadAwareContext);
   }

   /**
    * Set the naming reference
    *
    * @param reference The reference
    */
   @Override
   public void setReference(final Reference reference) {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("setReference(" + reference + ")");
      }

      this.reference = reference;
   }

   /**
    * Get the naming reference
    *
    * @return The reference
    */
   @Override
   public Reference getReference() {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("getReference()");
      }

      return reference;
   }

   /**
    * Set the user name
    *
    * @param name The user name
    */
   public void setUserName(final String name) {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("setUserName(" + name + ")");
      }

      userName = name;
   }

   /**
    * Set the password
    *
    * @param password The password
    */
   public void setPassword(final String password) {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("setPassword(****)");
      }

      this.password = password;
   }

   /**
    * Get the client ID
    *
    * @return The client ID
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public String getClientID() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("getClientID()");
      }

      checkClosed();

      if (clientID == null) {
         return ((ActiveMQResourceAdapter) mcf.getResourceAdapter()).getProperties().getClientID();
      }

      return clientID;
   }

   /**
    * Set the client ID -- throws IllegalStateException
    *
    * @param cID The client ID
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setClientID(final String cID) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("setClientID(" + cID + ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Create a queue session
    *
    * @param transacted      Use transactions
    * @param acknowledgeMode The acknowledge mode
    * @return The queue session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public QueueSession createQueueSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createQueueSession(" + transacted + ", " + acknowledgeMode + ")");
      }

      checkClosed();

      if (type == ActiveMQRAConnectionFactory.TOPIC_CONNECTION || type == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new IllegalStateException("Can not get a queue session from a topic connection");
      }

      return allocateConnection(transacted, acknowledgeMode, type);
   }

   /**
    * Create a XA queue session
    *
    * @return The XA queue session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public XAQueueSession createXAQueueSession() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createXAQueueSession()");
      }

      checkClosed();

      if (type == ActiveMQRAConnectionFactory.CONNECTION || type == ActiveMQRAConnectionFactory.TOPIC_CONNECTION ||
         type == ActiveMQRAConnectionFactory.XA_TOPIC_CONNECTION) {
         throw new IllegalStateException("Can not get a topic session from a queue connection");
      }

      return allocateConnection(type);
   }

   /**
    * Create a connection consumer -- throws IllegalStateException
    *
    * @param queue           The queue
    * @param messageSelector The message selector
    * @param sessionPool     The session pool
    * @param maxMessages     The number of max messages
    * @return The connection consumer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ConnectionConsumer createConnectionConsumer(final Queue queue,
                                                      final String messageSelector,
                                                      final ServerSessionPool sessionPool,
                                                      final int maxMessages) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createConnectionConsumer(" + queue +
                                          ", " +
                                          messageSelector +
                                          ", " +
                                          sessionPool +
                                          ", " +
                                          maxMessages +
                                          ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Create a topic session
    *
    * @param transacted      Use transactions
    * @param acknowledgeMode The acknowledge mode
    * @return The topic session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public TopicSession createTopicSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createTopicSession(" + transacted + ", " + acknowledgeMode + ")");
      }

      checkClosed();

      if (type == ActiveMQRAConnectionFactory.QUEUE_CONNECTION || type == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION) {
         throw new IllegalStateException("Can not get a topic session from a queue connection");
      }

      return allocateConnection(transacted, acknowledgeMode, type);
   }

   /**
    * Create a XA topic session
    *
    * @return The XA topic session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public XATopicSession createXATopicSession() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createXATopicSession()");
      }

      checkClosed();

      if (type == ActiveMQRAConnectionFactory.CONNECTION || type == ActiveMQRAConnectionFactory.QUEUE_CONNECTION ||
         type == ActiveMQRAConnectionFactory.XA_QUEUE_CONNECTION) {
         throw new IllegalStateException("Can not get a topic session from a queue connection");
      }

      return allocateConnection(type);
   }

   /**
    * Create a connection consumer -- throws IllegalStateException
    *
    * @param topic           The topic
    * @param messageSelector The message selector
    * @param sessionPool     The session pool
    * @param maxMessages     The number of max messages
    * @return The connection consumer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ConnectionConsumer createConnectionConsumer(final Topic topic,
                                                      final String messageSelector,
                                                      final ServerSessionPool sessionPool,
                                                      final int maxMessages) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createConnectionConsumer(" + topic +
                                          ", " +
                                          messageSelector +
                                          ", " +
                                          sessionPool +
                                          ", " +
                                          maxMessages +
                                          ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Create a durable connection consumer -- throws IllegalStateException
    *
    * @param topic            The topic
    * @param subscriptionName The subscription name
    * @param messageSelector  The message selector
    * @param sessionPool      The session pool
    * @param maxMessages      The number of max messages
    * @return The connection consumer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ConnectionConsumer createDurableConnectionConsumer(final Topic topic,
                                                             final String subscriptionName,
                                                             final String messageSelector,
                                                             final ServerSessionPool sessionPool,
                                                             final int maxMessages) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createConnectionConsumer(" + topic +
                                          ", " +
                                          subscriptionName +
                                          ", " +
                                          messageSelector +
                                          ", " +
                                          sessionPool +
                                          ", " +
                                          maxMessages +
                                          ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Create a connection consumer -- throws IllegalStateException
    *
    * @param destination The destination
    * @param pool        The session pool
    * @param maxMessages The number of max messages
    * @return The connection consumer
    * @throws JMSException Thrown if an error occurs
    */
   public ConnectionConsumer createConnectionConsumer(final Destination destination,
                                                      final ServerSessionPool pool,
                                                      final int maxMessages) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createConnectionConsumer(" + destination +
                                          ", " +
                                          pool +
                                          ", " +
                                          maxMessages +
                                          ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Create a connection consumer -- throws IllegalStateException
    *
    * @param destination The destination
    * @param name        The name
    * @param pool        The session pool
    * @param maxMessages The number of max messages
    * @return The connection consumer
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ConnectionConsumer createConnectionConsumer(final Destination destination,
                                                      final String name,
                                                      final ServerSessionPool pool,
                                                      final int maxMessages) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createConnectionConsumer(" + destination +
                                          ", " +
                                          name +
                                          ", " +
                                          pool +
                                          ", " +
                                          maxMessages +
                                          ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Create a session
    *
    * @param transacted      Use transactions
    * @param acknowledgeMode The acknowledge mode
    * @return The session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Session createSession(final boolean transacted, final int acknowledgeMode) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createSession(" + transacted + ", " + acknowledgeMode + ")");
      }

      checkClosed();
      return allocateConnection(transacted, acknowledgeMode, type);
   }

   /**
    * Create a XA session
    *
    * @return The XA session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public XASession createXASession() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createXASession()");
      }

      checkClosed();
      return allocateConnection(type);
   }

   /**
    * Get the connection metadata
    *
    * @return The connection metadata
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ConnectionMetaData getMetaData() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("getMetaData()");
      }

      checkClosed();
      return mcf.getMetaData();
   }

   /**
    * Get the exception listener -- throws IllegalStateException
    *
    * @return The exception listener
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public ExceptionListener getExceptionListener() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("getExceptionListener()");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Set the exception listener -- throws IllegalStateException
    *
    * @param listener The exception listener
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setExceptionListener(final ExceptionListener listener) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("setExceptionListener(" + listener + ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Start
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void start() throws JMSException {
      checkClosed();

      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("start() " + this);
      }

      synchronized (sessions) {
         if (started) {
            return;
         }
         started = true;
         for (ActiveMQRASession session : sessions) {
            session.start();
         }
      }
   }

   /**
    * Stop
    *
    * @throws IllegalStateException
    * @throws JMSException          Thrown if an error occurs
    */
   @Override
   public void stop() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("stop() " + this);
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Close
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void close() throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("close() " + this);
      }

      if (closed) {
         return;
      }

      closed = true;

      synchronized (sessions) {
         for (Iterator<ActiveMQRASession> i = sessions.iterator(); i.hasNext(); ) {
            ActiveMQRASession session = i.next();
            try {
               session.closeSession();
            } catch (Throwable t) {
               ActiveMQRALogger.LOGGER.trace("Error closing session", t);
            }
            i.remove();
         }
      }

      synchronized (tempQueues) {
         for (Iterator<TemporaryQueue> i = tempQueues.iterator(); i.hasNext(); ) {
            TemporaryQueue temp = i.next();
            try {
               if (ActiveMQRASessionFactoryImpl.trace) {
                  ActiveMQRALogger.LOGGER.trace("Closing temporary queue " + temp + " for " + this);
               }
               temp.delete();
            } catch (Throwable t) {
               ActiveMQRALogger.LOGGER.trace("Error deleting temporary queue", t);
            }
            i.remove();
         }
      }

      synchronized (tempTopics) {
         for (Iterator<TemporaryTopic> i = tempTopics.iterator(); i.hasNext(); ) {
            TemporaryTopic temp = i.next();
            try {
               if (ActiveMQRASessionFactoryImpl.trace) {
                  ActiveMQRALogger.LOGGER.trace("Closing temporary topic " + temp + " for " + this);
               }
               temp.delete();
            } catch (Throwable t) {
               ActiveMQRALogger.LOGGER.trace("Error deleting temporary queue", t);
            }
            i.remove();
         }
      }
   }

   /**
    * Close session
    *
    * @param session The session
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void closeSession(final ActiveMQRASession session) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("closeSession(" + session + ")");
      }

      synchronized (sessions) {
         sessions.remove(session);
      }
   }

   /**
    * Add temporary queue
    *
    * @param temp The temporary queue
    */
   @Override
   public void addTemporaryQueue(final TemporaryQueue temp) {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("addTemporaryQueue(" + temp + ")");
      }

      synchronized (tempQueues) {
         tempQueues.add(temp);
      }
   }

   /**
    * Add temporary topic
    *
    * @param temp The temporary topic
    */
   @Override
   public void addTemporaryTopic(final TemporaryTopic temp) {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("addTemporaryTopic(" + temp + ")");
      }

      synchronized (tempTopics) {
         tempTopics.add(temp);
      }
   }

   @Override
   public Session createSession(int sessionMode) throws JMSException {
      return createSession(sessionMode == Session.SESSION_TRANSACTED, sessionMode);
   }

   @Override
   public Session createSession() throws JMSException {
      return createSession(Session.AUTO_ACKNOWLEDGE);
   }

   @Override
   public ConnectionConsumer createSharedConnectionConsumer(Topic topic,
                                                            String subscriptionName,
                                                            String messageSelector,
                                                            ServerSessionPool sessionPool,
                                                            int maxMessages) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createSharedConnectionConsumer(" + topic + ", " + subscriptionName + ", " +
                                          messageSelector + ", " + sessionPool + ", " + maxMessages + ")");
      }

      throw new IllegalStateException(ISE);
   }

   @Override
   public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic,
                                                                   String subscriptionName,
                                                                   String messageSelector,
                                                                   ServerSessionPool sessionPool,
                                                                   int maxMessages) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("createSharedDurableConnectionConsumer(" + topic + ", " + subscriptionName +
                                          ", " + messageSelector + ", " + sessionPool + ", " + maxMessages + ")");
      }

      throw new IllegalStateException(ISE);
   }

   /**
    * Allocation a connection
    *
    * @param sessionType The session type
    * @return The session
    * @throws JMSException Thrown if an error occurs
    */
   protected ActiveMQRASession allocateConnection(final int sessionType) throws JMSException {
      return allocateConnection(false, Session.AUTO_ACKNOWLEDGE, sessionType);
   }

   /**
    * Allocate a connection
    *
    * @param transacted      Use transactions
    * @param acknowledgeMode The acknowledge mode
    * @param sessionType     The session type
    * @return The session
    * @throws JMSException Thrown if an error occurs
    */
   protected ActiveMQRASession allocateConnection(boolean transacted,
                                                  int acknowledgeMode,
                                                  final int sessionType) throws JMSException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("allocateConnection(" + transacted +
                                          ", " +
                                          acknowledgeMode +
                                          ", " +
                                          sessionType +
                                          ")");
      }

      try {
         synchronized (sessions) {
            if (sessions.isEmpty() == false) {
               throw new IllegalStateException("Only allowed one session per connection. See the J2EE spec, e.g. J2EE1.4 Section 6.6");
            }
            //from createSession
            // In a Java EE web or EJB container, when there is an active JTA transaction in progress:
            //Both arguments {@code transacted} and {@code acknowledgeMode} are ignored.
            if (inJtaTransaction()) {
               transacted = true;
               //from getAcknowledgeMode
               // If the session is not transacted, returns the
               // current acknowledgement mode for the session.
               // If the session
               // is transacted, returns SESSION_TRANSACTED.
               acknowledgeMode = Session.SESSION_TRANSACTED;
            } else {
               //In the Java EE web or EJB container, when there is no active JTA transaction in progress
               // The argument {@code transacted} is ignored.

               //The session will always be non-transacted,
               transacted = false;
               switch (acknowledgeMode) {
                  //using one of the two acknowledgement modes AUTO_ACKNOWLEDGE and DUPS_OK_ACKNOWLEDGE.
                  case Session.AUTO_ACKNOWLEDGE:
                  case Session.DUPS_OK_ACKNOWLEDGE:
                     //plus our own
                  case ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE:
                  case ActiveMQJMSConstants.PRE_ACKNOWLEDGE:
                     break;
                  //The value {@code Session.CLIENT_ACKNOWLEDGE} may not be used.
                  case Session.CLIENT_ACKNOWLEDGE:
                     throw ActiveMQRABundle.BUNDLE.invalidClientAcknowledgeModeRuntime();
                     //same with this although the spec doesn't explicitly say
                  case Session.SESSION_TRANSACTED:
                     throw ActiveMQRABundle.BUNDLE.invalidSessionTransactedModeRuntime();
                  default:
                     throw ActiveMQRABundle.BUNDLE.invalidAcknowledgeMode(acknowledgeMode);
               }
            }

            ActiveMQRAConnectionRequestInfo info = new ActiveMQRAConnectionRequestInfo(transacted, acknowledgeMode, sessionType);
            info.setUserName(userName);
            info.setPassword(password);
            info.setClientID(clientID);
            info.setDefaults(((ActiveMQResourceAdapter) mcf.getResourceAdapter()).getProperties());

            if (ActiveMQRASessionFactoryImpl.trace) {
               ActiveMQRALogger.LOGGER.trace("Allocating session for " + this + " with request info=" + info);
            }

            ActiveMQRASession session = (ActiveMQRASession) cm.allocateConnection(mcf, info);

            try {
               if (ActiveMQRASessionFactoryImpl.trace) {
                  ActiveMQRALogger.LOGGER.trace("Allocated  " + this + " session=" + session);
               }

               session.setActiveMQSessionFactory(this);

               if (started) {
                  session.start();
               }

               sessions.add(session);

               return session;
            } catch (Throwable t) {
               try {
                  session.close();
               } catch (Throwable ignored) {
               }
               if (t instanceof Exception) {
                  throw (Exception) t;
               } else {
                  throw new RuntimeException("Unexpected error: ", t);
               }
            }
         }
      } catch (Exception e) {
         Throwable current = e;
         while (current != null && !(current instanceof JMSException)) {
            current = current.getCause();
         }

         if (current != null && current instanceof JMSException) {
            throw (JMSException) current;
         } else {
            JMSException je = new JMSException("Could not create a session: " + e.getMessage());
            je.setLinkedException(e);
            je.initCause(e);
            throw je;
         }
      }
   }

   /**
    * Check if we are closed
    *
    * @throws IllegalStateException Thrown if closed
    */
   protected void checkClosed() throws IllegalStateException {
      if (ActiveMQRASessionFactoryImpl.trace) {
         ActiveMQRALogger.LOGGER.trace("checkClosed()" + this);
      }

      if (closed) {
         throw new IllegalStateException("The connection is closed");
      }
   }

   private boolean inJtaTransaction() {
      boolean inJtaTx = false;
      if (tm != null) {
         Transaction tx = null;
         try {
            tx = tm.getTransaction();
         } catch (SystemException e) {
            //assume false
         }
         inJtaTx = tx != null;
      }
      return inJtaTx;
   }
}
