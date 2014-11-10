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
package org.hornetq.ra;

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

import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.jms.client.HornetQConnectionForContext;
import org.hornetq.jms.client.HornetQConnectionForContextImpl;

/**
 * Implements the JMS Connection API and produces {@link HornetQRASession} objects.
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public final class HornetQRASessionFactoryImpl extends HornetQConnectionForContextImpl implements
   HornetQRASessionFactory, HornetQConnectionForContext, Referenceable
{
   /**
    * Trace enabled
    */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

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
   private final HornetQRAManagedConnectionFactory mcf;
   private TransactionManager tm;

   /**
    * The connection manager
    */
   private ConnectionManager cm;

   /**
    * The sessions
    */
   private final Set<HornetQRASession> sessions = new HashSet<HornetQRASession>();

   /**
    * The temporary queues
    */
   private final Set<TemporaryQueue> tempQueues = new HashSet<TemporaryQueue>();

   /**
    * The temporary topics
    */
   private final Set<TemporaryTopic> tempTopics = new HashSet<TemporaryTopic>();

   /**
    * Constructor
    *
    * @param mcf  The managed connection factory
    * @param cm   The connection manager
    * @param type The connection type
    */
   public HornetQRASessionFactoryImpl(final HornetQRAManagedConnectionFactory mcf,
                                      final ConnectionManager cm,
                                      final TransactionManager tm,
                                      final int type)
   {
      this.mcf = mcf;

      this.tm = tm;

      if (cm == null)
      {
         this.cm = new HornetQRAConnectionManager();
      }
      else
      {
         this.cm = cm;
      }

      this.type = type;

      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + mcf + ", " + cm + ", " + type);
      }
   }

   public JMSContext createContext(int sessionMode)
   {
      boolean inJtaTx = inJtaTransaction();
      int sessionModeToUse;
      switch (sessionMode)
      {
         case Session.AUTO_ACKNOWLEDGE:
         case Session.DUPS_OK_ACKNOWLEDGE:
         case HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE:
         case HornetQJMSConstants.PRE_ACKNOWLEDGE:
            sessionModeToUse = sessionMode;
            break;
         //these are prohibited in JEE unless not in a JTA tx where they should be ignored and auto_ack used
         case Session.CLIENT_ACKNOWLEDGE:
            if (!inJtaTx)
            {
               throw HornetQRABundle.BUNDLE.invalidSessionTransactedModeRuntime();
            }
            sessionModeToUse = Session.AUTO_ACKNOWLEDGE;
            break;
         case Session.SESSION_TRANSACTED:
            if (!inJtaTx)
            {
               throw HornetQRABundle.BUNDLE.invalidClientAcknowledgeModeRuntime();
            }
            sessionModeToUse = Session.AUTO_ACKNOWLEDGE;
            break;
         default:
            throw HornetQRABundle.BUNDLE.invalidAcknowledgeMode(sessionMode);
      }
      incrementRefCounter();

      return new HornetQRAJMSContext(this, sessionModeToUse, threadAwareContext);
   }

   public XAJMSContext createXAContext()
   {
      incrementRefCounter();

      return new HornetQRAXAJMSContext(this, threadAwareContext);
   }

   /**
    * Set the naming reference
    *
    * @param reference The reference
    */
   public void setReference(final Reference reference)
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("setReference(" + reference + ")");
      }

      this.reference = reference;
   }

   /**
    * Get the naming reference
    *
    * @return The reference
    */
   public Reference getReference()
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("getReference()");
      }

      return reference;
   }

   /**
    * Set the user name
    *
    * @param name The user name
    */
   public void setUserName(final String name)
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("setUserName(" + name + ")");
      }

      userName = name;
   }

   /**
    * Set the password
    *
    * @param password The password
    */
   public void setPassword(final String password)
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("setPassword(****)");
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
   public String getClientID() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("getClientID()");
      }

      checkClosed();

      if (clientID == null)
      {
         return ((HornetQResourceAdapter) mcf.getResourceAdapter()).getProperties().getClientID();
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
   public void setClientID(final String cID) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("setClientID(" + cID + ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
   }

   /**
    * Create a queue session
    *
    * @param transacted      Use transactions
    * @param acknowledgeMode The acknowledge mode
    * @return The queue session
    * @throws JMSException Thrown if an error occurs
    */
   public QueueSession createQueueSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createQueueSession(" + transacted + ", " + acknowledgeMode + ")");
      }

      checkClosed();

      if (type == HornetQRAConnectionFactory.TOPIC_CONNECTION || type == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
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
   public XAQueueSession createXAQueueSession() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXAQueueSession()");
      }

      checkClosed();

      if (type == HornetQRAConnectionFactory.CONNECTION || type == HornetQRAConnectionFactory.TOPIC_CONNECTION ||
         type == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
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
   public ConnectionConsumer createConnectionConsumer(final Queue queue,
                                                      final String messageSelector,
                                                      final ServerSessionPool sessionPool,
                                                      final int maxMessages) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnectionConsumer(" + queue +
                                         ", " +
                                         messageSelector +
                                         ", " +
                                         sessionPool +
                                         ", " +
                                         maxMessages +
                                         ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
   }

   /**
    * Create a topic session
    *
    * @param transacted      Use transactions
    * @param acknowledgeMode The acknowledge mode
    * @return The topic session
    * @throws JMSException Thrown if an error occurs
    */
   public TopicSession createTopicSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createTopicSession(" + transacted + ", " + acknowledgeMode + ")");
      }

      checkClosed();

      if (type == HornetQRAConnectionFactory.QUEUE_CONNECTION || type == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION)
      {
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
   public XATopicSession createXATopicSession() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXATopicSession()");
      }

      checkClosed();

      if (type == HornetQRAConnectionFactory.CONNECTION || type == HornetQRAConnectionFactory.QUEUE_CONNECTION ||
         type == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION)
      {
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
   public ConnectionConsumer createConnectionConsumer(final Topic topic,
                                                      final String messageSelector,
                                                      final ServerSessionPool sessionPool,
                                                      final int maxMessages) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnectionConsumer(" + topic +
                                         ", " +
                                         messageSelector +
                                         ", " +
                                         sessionPool +
                                         ", " +
                                         maxMessages +
                                         ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
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
                                                             final int maxMessages) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnectionConsumer(" + topic +
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

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
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
                                                      final int maxMessages) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnectionConsumer(" + destination +
                                         ", " +
                                         pool +
                                         ", " +
                                         maxMessages +
                                         ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
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
                                                      final int maxMessages) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnectionConsumer(" + destination +
                                         ", " +
                                         name +
                                         ", " +
                                         pool +
                                         ", " +
                                         maxMessages +
                                         ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
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
   public Session createSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createSession(" + transacted + ", " + acknowledgeMode + ")");
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
   public XASession createXASession() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXASession()");
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
   public ConnectionMetaData getMetaData() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("getMetaData()");
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
   public ExceptionListener getExceptionListener() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("getExceptionListener()");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
   }

   /**
    * Set the exception listener -- throws IllegalStateException
    *
    * @param listener The exception listener
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setExceptionListener(final ExceptionListener listener) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("setExceptionListener(" + listener + ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
   }

   /**
    * Start
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void start() throws JMSException
   {
      checkClosed();

      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("start() " + this);
      }

      synchronized (sessions)
      {
         if (started)
         {
            return;
         }
         started = true;
         for (HornetQRASession session : sessions)
         {
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
   public void stop() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("stop() " + this);
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
   }

   /**
    * Close
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void close() throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("close() " + this);
      }

      if (closed)
      {
         return;
      }

      closed = true;

      synchronized (sessions)
      {
         for (Iterator<HornetQRASession> i = sessions.iterator(); i.hasNext(); )
         {
            HornetQRASession session = i.next();
            try
            {
               session.closeSession();
            }
            catch (Throwable t)
            {
               HornetQRALogger.LOGGER.trace("Error closing session", t);
            }
            i.remove();
         }
      }

      synchronized (tempQueues)
      {
         for (Iterator<TemporaryQueue> i = tempQueues.iterator(); i.hasNext(); )
         {
            TemporaryQueue temp = i.next();
            try
            {
               if (HornetQRASessionFactoryImpl.trace)
               {
                  HornetQRALogger.LOGGER.trace("Closing temporary queue " + temp + " for " + this);
               }
               temp.delete();
            }
            catch (Throwable t)
            {
               HornetQRALogger.LOGGER.trace("Error deleting temporary queue", t);
            }
            i.remove();
         }
      }

      synchronized (tempTopics)
      {
         for (Iterator<TemporaryTopic> i = tempTopics.iterator(); i.hasNext(); )
         {
            TemporaryTopic temp = i.next();
            try
            {
               if (HornetQRASessionFactoryImpl.trace)
               {
                  HornetQRALogger.LOGGER.trace("Closing temporary topic " + temp + " for " + this);
               }
               temp.delete();
            }
            catch (Throwable t)
            {
               HornetQRALogger.LOGGER.trace("Error deleting temporary queue", t);
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
   public void closeSession(final HornetQRASession session) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("closeSession(" + session + ")");
      }

      synchronized (sessions)
      {
         sessions.remove(session);
      }
   }

   /**
    * Add temporary queue
    *
    * @param temp The temporary queue
    */
   public void addTemporaryQueue(final TemporaryQueue temp)
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("addTemporaryQueue(" + temp + ")");
      }

      synchronized (tempQueues)
      {
         tempQueues.add(temp);
      }
   }

   /**
    * Add temporary topic
    *
    * @param temp The temporary topic
    */
   public void addTemporaryTopic(final TemporaryTopic temp)
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("addTemporaryTopic(" + temp + ")");
      }

      synchronized (tempTopics)
      {
         tempTopics.add(temp);
      }
   }

   @Override
   public Session createSession(int sessionMode) throws JMSException
   {
      return createSession(sessionMode == Session.SESSION_TRANSACTED, sessionMode);
   }

   @Override
   public Session createSession() throws JMSException
   {
      return createSession(Session.AUTO_ACKNOWLEDGE);
   }

   @Override
   public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createSharedConnectionConsumer(" + topic + ", " + subscriptionName + ", " +
                                         messageSelector + ", " + sessionPool + ", " + maxMessages + ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
   }

   @Override
   public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createSharedDurableConnectionConsumer(" + topic + ", " + subscriptionName +
                                         ", " + messageSelector + ", " + sessionPool + ", " + maxMessages + ")");
      }

      throw new IllegalStateException(HornetQRASessionFactory.ISE);
   }

   /**
    * Allocation a connection
    *
    * @param sessionType The session type
    * @return The session
    * @throws JMSException Thrown if an error occurs
    */
   protected HornetQRASession allocateConnection(final int sessionType) throws JMSException
   {
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
   protected HornetQRASession allocateConnection(boolean transacted, int acknowledgeMode, final int sessionType) throws JMSException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("allocateConnection(" + transacted +
                                         ", " +
                                         acknowledgeMode +
                                         ", " +
                                         sessionType +
                                         ")");
      }

      try
      {
         synchronized (sessions)
         {
            if (sessions.isEmpty() == false)
            {
               throw new IllegalStateException("Only allowed one session per connection. See the J2EE spec, e.g. J2EE1.4 Section 6.6");
            }
            //from createSession
            // In a Java EE web or EJB container, when there is an active JTA transaction in progress:
            //Both arguments {@code transacted} and {@code acknowledgeMode} are ignored.
            if (inJtaTransaction())
            {
               transacted = true;
               //from getAcknowledgeMode
               // If the session is not transacted, returns the
               // current acknowledgement mode for the session.
               // If the session
               // is transacted, returns SESSION_TRANSACTED.
               acknowledgeMode = Session.SESSION_TRANSACTED;
            }
            //In the Java EE web or EJB container, when there is no active JTA transaction in progress
            // The argument {@code transacted} is ignored.
            else
            {
               //The session will always be non-transacted,
               transacted = false;
               switch (acknowledgeMode)
               {
                  //using one of the two acknowledgement modes AUTO_ACKNOWLEDGE and DUPS_OK_ACKNOWLEDGE.
                  case Session.AUTO_ACKNOWLEDGE:
                  case Session.DUPS_OK_ACKNOWLEDGE:
                     //plus our own
                  case HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE:
                  case HornetQJMSConstants.PRE_ACKNOWLEDGE:
                     break;
                  //The value {@code Session.CLIENT_ACKNOWLEDGE} may not be used.
                  case Session.CLIENT_ACKNOWLEDGE:
                     throw HornetQRABundle.BUNDLE.invalidClientAcknowledgeModeRuntime();
                     //same with this although the spec doesn't explicitly say
                  case Session.SESSION_TRANSACTED:
                     throw HornetQRABundle.BUNDLE.invalidSessionTransactedModeRuntime();
                  default:
                     throw HornetQRABundle.BUNDLE.invalidAcknowledgeMode(acknowledgeMode);
               }
            }

            HornetQRAConnectionRequestInfo info = new HornetQRAConnectionRequestInfo(transacted,
                                                                                     acknowledgeMode,
                                                                                     sessionType);
            info.setUserName(userName);
            info.setPassword(password);
            info.setClientID(clientID);
            info.setDefaults(((HornetQResourceAdapter) mcf.getResourceAdapter()).getProperties());

            if (HornetQRASessionFactoryImpl.trace)
            {
               HornetQRALogger.LOGGER.trace("Allocating session for " + this + " with request info=" + info);
            }

            HornetQRASession session = (HornetQRASession) cm.allocateConnection(mcf, info);

            try
            {
               if (HornetQRASessionFactoryImpl.trace)
               {
                  HornetQRALogger.LOGGER.trace("Allocated  " + this + " session=" + session);
               }

               session.setHornetQSessionFactory(this);

               if (started)
               {
                  session.start();
               }

               sessions.add(session);

               return session;
            }
            catch (Throwable t)
            {
               try
               {
                  session.close();
               }
               catch (Throwable ignored)
               {
               }
               if (t instanceof Exception)
               {
                  throw (Exception) t;
               }
               else
               {
                  throw new RuntimeException("Unexpected error: ", t);
               }
            }
         }
      }
      catch (Exception e)
      {
         Throwable current = e;
         while (current != null && !(current instanceof JMSException))
         {
            current = current.getCause();
         }

         if (current != null && current instanceof JMSException)
         {
            throw (JMSException) current;
         }
         else
         {
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
   protected void checkClosed() throws IllegalStateException
   {
      if (HornetQRASessionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("checkClosed()" + this);
      }

      if (closed)
      {
         throw new IllegalStateException("The connection is closed");
      }
   }

   private boolean inJtaTransaction()
   {
      boolean inJtaTx = false;
      if (tm != null)
      {
         Transaction tx = null;
         try
         {
            tx = tm.getTransaction();
         }
         catch (SystemException e)
         {
            //assume false
         }
         inJtaTx = tx != null;
      }
      return inJtaTx;
   }
}
