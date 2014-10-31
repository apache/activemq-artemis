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
package org.hornetq.jms.client;

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.FailoverEventType;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.version.Version;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.UUIDGenerator;
import org.hornetq.utils.VersionLoader;

/**
 * HornetQ implementation of a JMS Connection.
 * <p>
 * The flat implementation of {@link TopicConnection} and {@link QueueConnection} is per design,
 * following the common usage of these as one flat API in JMS 1.1.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQConnection extends HornetQConnectionForContextImpl implements TopicConnection, QueueConnection
{
   // Constants ------------------------------------------------------------------------------------
   public static final int TYPE_GENERIC_CONNECTION = 0;

   public static final int TYPE_QUEUE_CONNECTION = 1;

   public static final int TYPE_TOPIC_CONNECTION = 2;

   public static final String EXCEPTION_FAILOVER = "FAILOVER";

   public static final String EXCEPTION_DISCONNECT = "DISCONNECT";

   public static final SimpleString CONNECTION_ID_PROPERTY_NAME = new SimpleString("__HQ_CID");

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final int connectionType;

   private final Set<HornetQSession> sessions = new org.hornetq.utils.ConcurrentHashSet<HornetQSession>();

   private final Set<SimpleString> tempQueues = new org.hornetq.utils.ConcurrentHashSet<SimpleString>();

   private final Set<SimpleString> knownDestinations = new ConcurrentHashSet<SimpleString>();

   private volatile boolean hasNoLocal;

   private volatile ExceptionListener exceptionListener;

   private volatile FailoverEventListener failoverEventListener;

   private volatile boolean justCreated = true;

   private volatile ConnectionMetaData metaData;

   private volatile boolean closed;

   private volatile boolean started;

   private String clientID;

   private final ClientSessionFactory sessionFactory;

   private final SimpleString uid;

   private final String username;

   private final String password;

   private final SessionFailureListener listener = new JMSFailureListener(this);

   private final FailoverEventListener failoverListener = new FailoverEventListenerImpl(this);

   private final Version thisVersion;

   private final int dupsOKBatchSize;

   private final int transactionBatchSize;

   private ClientSession initialSession;

   private final Exception creationStack;

   private HornetQConnectionFactory factoryReference;

   // Constructors ---------------------------------------------------------------------------------

   public HornetQConnection(final String username, final String password, final int connectionType,
                            final String clientID, final int dupsOKBatchSize, final int transactionBatchSize,
                            final ClientSessionFactory sessionFactory)
   {
      this.username = username;

      this.password = password;

      this.connectionType = connectionType;

      this.clientID = clientID;

      this.sessionFactory = sessionFactory;

      uid = UUIDGenerator.getInstance().generateSimpleStringUUID();

      thisVersion = VersionLoader.getVersion();

      this.dupsOKBatchSize = dupsOKBatchSize;

      this.transactionBatchSize = transactionBatchSize;

      creationStack = new Exception();
   }

   /**
    * This internal method serves basically the Resource Adapter.
    * The resource adapter plays with an XASession and a non XASession.
    * When there is no enlisted transaction, the EE specification mandates that the commit should
    * be done as if it was a nonXA Session (i.e. SessionTransacted).
    * For that reason we have this method to force that nonXASession, since the JMS Javadoc
    * mandates createSession to return a XASession.
    */
   public Session createNonXASession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();

      return createSessionInternal(false, transacted, acknowledgeMode, HornetQConnection.TYPE_GENERIC_CONNECTION);
   }

   /**
    * This internal method serves basically the Resource Adapter.
    * The resource adapter plays with an XASession and a non XASession.
    * When there is no enlisted transaction, the EE specification mandates that the commit should
    * be done as if it was a nonXA Session (i.e. SessionTransacted).
    * For that reason we have this method to force that nonXASession, since the JMS Javadoc
    * mandates createSession to return a XASession.
    */
   public Session createNonXATopicSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();

      return createSessionInternal(false, transacted, acknowledgeMode, HornetQConnection.TYPE_TOPIC_CONNECTION);
   }

   /**
    * This internal method serves basically the Resource Adapter.
    * The resource adapter plays with an XASession and a non XASession.
    * When there is no enlisted transaction, the EE specification mandates that the commit should
    * be done as if it was a nonXA Session (i.e. SessionTransacted).
    * For that reason we have this method to force that nonXASession, since the JMS Javadoc
    * mandates createSession to return a XASession.
    */
   public Session createNonXAQueueSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();

      return createSessionInternal(false, transacted, acknowledgeMode, HornetQConnection.TYPE_QUEUE_CONNECTION);
   }


   // Connection implementation --------------------------------------------------------------------

   public synchronized Session createSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();

      return createSessionInternal(false, transacted, checkAck(transacted, acknowledgeMode), HornetQConnection.TYPE_GENERIC_CONNECTION);
   }

   public String getClientID() throws JMSException
   {
      checkClosed();

      return clientID;
   }

   public void setClientID(final String clientID) throws JMSException
   {
      checkClosed();

      if (this.clientID != null)
      {
         throw new IllegalStateException("Client id has already been set");
      }

      if (!justCreated)
      {
         throw new IllegalStateException("setClientID can only be called directly after the connection is created");
      }

      try
      {
         initialSession.addUniqueMetaData("jms-client-id", clientID);
      }
      catch (HornetQException e)
      {
         if (e.getType() == HornetQExceptionType.DUPLICATE_METADATA)
         {
            throw new InvalidClientIDException("clientID=" + clientID + " was already set into another connection");
         }
      }

      this.clientID = clientID;
      try
      {
         this.addSessionMetaData(initialSession);
      }
      catch (HornetQException e)
      {
         JMSException ex = new JMSException("Internal error setting metadata jms-client-id");
         ex.setLinkedException(e);
         ex.initCause(e);
         throw ex;
      }

      justCreated = false;
   }

   public ConnectionMetaData getMetaData() throws JMSException
   {
      checkClosed();

      justCreated = false;

      if (metaData == null)
      {
         metaData = new HornetQConnectionMetaData(thisVersion);
      }

      return metaData;
   }

   public ExceptionListener getExceptionListener() throws JMSException
   {
      checkClosed();

      justCreated = false;

      return exceptionListener;
   }

   public void setExceptionListener(final ExceptionListener listener) throws JMSException
   {
      checkClosed();

      exceptionListener = listener;
      justCreated = false;
   }

   public synchronized void start() throws JMSException
   {
      checkClosed();

      for (HornetQSession session : sessions)
      {
         session.start();
      }

      justCreated = false;
      started = true;
   }

   public synchronized void signalStopToAllSessions()
   {
      for (HornetQSession session : sessions)
      {
         ClientSession coreSession = session.getCoreSession();
         if (coreSession instanceof ClientSessionInternal)
         {
            ClientSessionInternal internalSession = (ClientSessionInternal) coreSession;
            internalSession.setStopSignal();
         }
      }

   }

   public synchronized void stop() throws JMSException
   {
      threadAwareContext.assertNotMessageListenerThread();

      checkClosed();

      for (HornetQSession session : sessions)
      {
         session.stop();
      }

      justCreated = false;
      started = false;
   }

   public final synchronized void close() throws JMSException
   {
      threadAwareContext.assertNotCompletionListenerThread();
      threadAwareContext.assertNotMessageListenerThread();

      if (closed)
      {
         return;
      }

      sessionFactory.close();

      try
      {
         for (HornetQSession session : new HashSet<HornetQSession>(sessions))
         {
            session.close();
         }

         try
         {
            if (!tempQueues.isEmpty())
            {
               // Remove any temporary queues

               for (SimpleString queueName : tempQueues)
               {
                  if (!initialSession.isClosed())
                  {
                     try
                     {
                        initialSession.deleteQueue(queueName);
                     }
                     catch (HornetQException ignore)
                     {
                        // Exception on deleting queue shouldn't prevent close from completing
                     }
                  }
               }
            }
         }
         finally
         {
            if (initialSession != null)
            {
               initialSession.close();
            }
         }

         closed = true;
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public ConnectionConsumer
   createConnectionConsumer(final Destination destination, final String messageSelector,
                            final ServerSessionPool sessionPool, final int maxMessages) throws JMSException
   {
      checkClosed();

      checkTempQueues(destination);

      // We offer a RA, so no need to implement this for MDBs
      return null;
   }

   private void checkTempQueues(Destination destination) throws JMSException
   {
      HornetQDestination jbdest = (HornetQDestination) destination;

      if (jbdest.isTemporary() && !containsTemporaryQueue(jbdest.getSimpleAddress()))
      {
         throw new JMSException("Can not create consumer for temporary destination " + destination +
                                   " from another JMS connection");
      }
   }

   public ConnectionConsumer
   createDurableConnectionConsumer(final Topic topic, final String subscriptionName,
                                   final String messageSelector, final ServerSessionPool sessionPool,
                                   final int maxMessages) throws JMSException
   {
      checkClosed();
      // As spec. section 4.11
      if (connectionType == HornetQConnection.TYPE_QUEUE_CONNECTION)
      {
         String msg = "Cannot create a durable connection consumer on a QueueConnection";
         throw new javax.jms.IllegalStateException(msg);
      }
      checkTempQueues(topic);
      // We offer RA, so no need for this
      return null;
   }

   @Override
   public Session createSession(int sessionMode) throws JMSException
   {
      checkClosed();
      return createSessionInternal(false, sessionMode == Session.SESSION_TRANSACTED, sessionMode, HornetQSession.TYPE_GENERIC_SESSION);

   }

   @Override
   public Session createSession() throws JMSException
   {
      checkClosed();
      return createSessionInternal(false, false, Session.AUTO_ACKNOWLEDGE, HornetQSession.TYPE_GENERIC_SESSION);
   }

   // QueueConnection implementation ---------------------------------------------------------------

   public QueueSession createQueueSession(final boolean transacted, int acknowledgeMode) throws JMSException
   {
      checkClosed();
      return createSessionInternal(false, transacted, checkAck(transacted, acknowledgeMode), HornetQSession.TYPE_QUEUE_SESSION);
   }

   /**
    * I'm keeping this as static as the same check will be done within RA.
    * This is to conform with TCK Tests where we must return ackMode exactly as they want if transacted=false
    */
   public static int checkAck(boolean transacted, int acknowledgeMode)
   {
      if (!transacted && acknowledgeMode == Session.SESSION_TRANSACTED)
      {
         return Session.AUTO_ACKNOWLEDGE;
      }

      return acknowledgeMode;
   }

   public ConnectionConsumer
   createConnectionConsumer(final Queue queue, final String messageSelector,
                            final ServerSessionPool sessionPool, final int maxMessages) throws JMSException
   {
      checkClosed();
      checkTempQueues(queue);
      return null;
   }

   // TopicConnection implementation ---------------------------------------------------------------

   public TopicSession createTopicSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();
      return createSessionInternal(false, transacted, checkAck(transacted, acknowledgeMode), HornetQSession.TYPE_TOPIC_SESSION);
   }

   public ConnectionConsumer
   createConnectionConsumer(final Topic topic, final String messageSelector,
                            final ServerSessionPool sessionPool, final int maxMessages) throws JMSException
   {
      checkClosed();
      checkTempQueues(topic);
      return null;
   }

   @Override
   public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
   {
      return null; // we offer RA
   }

   @Override
   public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
   {
      return null; // we offer RA
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * Sets a FailureListener for the session which is notified if a failure occurs on the session.
    *
    * @param listener the listener to add
    * @throws JMSException
    */
   public void setFailoverListener(final FailoverEventListener listener) throws JMSException
   {
      checkClosed();

      justCreated = false;

      this.failoverEventListener = listener;

   }

   /**
    * @return {@link FailoverEventListener} the current failover event listener for this connection
    * @throws JMSException
    */
   public FailoverEventListener getFailoverListener() throws JMSException
   {
      checkClosed();

      justCreated = false;

      return failoverEventListener;
   }

   public void addTemporaryQueue(final SimpleString queueAddress)
   {
      tempQueues.add(queueAddress);
      knownDestinations.add(queueAddress);
   }

   public void removeTemporaryQueue(final SimpleString queueAddress)
   {
      tempQueues.remove(queueAddress);
   }

   public void addKnownDestination(final SimpleString address)
   {
      knownDestinations.add(address);
   }

   public boolean containsKnownDestination(final SimpleString address)
   {
      return knownDestinations.contains(address);
   }

   public boolean containsTemporaryQueue(final SimpleString queueAddress)
   {
      return tempQueues.contains(queueAddress);
   }

   public boolean hasNoLocal()
   {
      return hasNoLocal;
   }

   public void setHasNoLocal()
   {
      hasNoLocal = true;
   }

   public SimpleString getUID()
   {
      return uid;
   }

   public void removeSession(final HornetQSession session)
   {
      sessions.remove(session);
   }

   public ClientSession getInitialSession()
   {
      return initialSession;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // In case the user forgets to close the connection manually

   @Override
   protected final void finalize() throws Throwable
   {
      if (!closed)
      {
         HornetQJMSClientLogger.LOGGER.connectionLeftOpen(creationStack);

         close();
      }
   }

   protected boolean isXA()
   {
      return false;
   }

   protected final HornetQSession
   createSessionInternal(final boolean isXA, final boolean transacted, int acknowledgeMode, final int type) throws JMSException
   {
      if (transacted)
      {
         acknowledgeMode = Session.SESSION_TRANSACTED;
      }

      try
      {
         ClientSession session;

         if (acknowledgeMode == Session.SESSION_TRANSACTED)
         {
            session =
               sessionFactory.createSession(username, password, isXA, false, false,
                                            sessionFactory.getServerLocator().isPreAcknowledge(),
                                            transactionBatchSize);
         }
         else if (acknowledgeMode == Session.AUTO_ACKNOWLEDGE)
         {
            session =
               sessionFactory.createSession(username, password, isXA, true, true,
                                            sessionFactory.getServerLocator().isPreAcknowledge(), 0);
         }
         else if (acknowledgeMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            session =
               sessionFactory.createSession(username, password, isXA, true, true,
                                            sessionFactory.getServerLocator().isPreAcknowledge(), dupsOKBatchSize);
         }
         else if (acknowledgeMode == Session.CLIENT_ACKNOWLEDGE)
         {
            session =
               sessionFactory.createSession(username, password, isXA, true, false,
                                            sessionFactory.getServerLocator().isPreAcknowledge(),
                                            transactionBatchSize);
         }
         else if (acknowledgeMode == HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE)
         {
            session =
               sessionFactory.createSession(username, password, isXA, true, false, false, transactionBatchSize);
         }
         else if (acknowledgeMode == HornetQJMSConstants.PRE_ACKNOWLEDGE)
         {
            session = sessionFactory.createSession(username, password, isXA, true, false, true, transactionBatchSize);
         }
         else
         {
            throw new JMSRuntimeException("Invalid ackmode: " + acknowledgeMode);
         }

         justCreated = false;

         // Setting multiple times on different sessions doesn't matter since RemotingConnection
         // maintains
         // a set (no duplicates)
         session.addFailureListener(listener);
         session.addFailoverListener(failoverListener);

         HornetQSession jbs = createHQSession(isXA, transacted, acknowledgeMode, session, type);

         sessions.add(jbs);

         if (started)
         {
            session.start();
         }

         this.addSessionMetaData(session);

         return jbs;
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   // Private --------------------------------------------------------------------------------------

   /**
    * @param transacted
    * @param acknowledgeMode
    * @param session
    * @param type
    * @return
    */
   protected HornetQSession createHQSession(boolean isXA, boolean transacted, int acknowledgeMode, ClientSession session, int type)
   {
      if (isXA)
      {
         return new HornetQXASession(this, transacted, true, acknowledgeMode, session, type);
      }
      else
      {
         return new HornetQSession(this, transacted, false, acknowledgeMode, session, type);
      }
   }

   protected final void checkClosed() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Connection is closed");
      }
   }

   public void authorize() throws JMSException
   {
      try
      {
         initialSession = sessionFactory.createSession(username, password, false, false, false, false, 0);

         addSessionMetaData(initialSession);

         initialSession.addFailureListener(listener);
         initialSession.addFailoverListener(failoverListener);
      }
      catch (HornetQException me)
      {
         throw JMSExceptionHelper.convertFromHornetQException(me);
      }
   }

   private void addSessionMetaData(ClientSession session) throws HornetQException
   {
      session.addMetaData("jms-session", "");
      if (clientID != null)
      {
         session.addMetaData("jms-client-id", clientID);
      }
   }

   public void setReference(HornetQConnectionFactory factory)
   {
      this.factoryReference = factory;
   }

   public boolean isStarted()
   {
      return started;
   }


   // Inner classes --------------------------------------------------------------------------------

   private static class JMSFailureListener implements SessionFailureListener
   {
      private final WeakReference<HornetQConnection> connectionRef;

      JMSFailureListener(final HornetQConnection connection)
      {
         connectionRef = new WeakReference<HornetQConnection>(connection);
      }

      @Override
      public synchronized void connectionFailed(final HornetQException me, boolean failedOver)
      {
         if (me == null)
         {
            return;
         }

         HornetQConnection conn = connectionRef.get();

         if (conn != null)
         {
            try
            {
               final ExceptionListener exceptionListener = conn.getExceptionListener();

               if (exceptionListener != null)
               {
                  final JMSException je =
                     new JMSException(me.toString(), failedOver ? EXCEPTION_FAILOVER : EXCEPTION_DISCONNECT);

                  je.initCause(me);

                  new Thread(new Runnable()
                  {
                     public void run()
                     {
                        exceptionListener.onException(je);
                     }
                  }).start();
               }
            }
            catch (JMSException e)
            {
               if (!conn.closed)
               {
                  HornetQJMSClientLogger.LOGGER.errorCallingExcListener(e);
               }
            }
         }
      }

      @Override
      public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
      {
         connectionFailed(me, failedOver);
      }

      public void beforeReconnect(final HornetQException me)
      {

      }

   }

   private static class FailoverEventListenerImpl implements FailoverEventListener
   {
      private final WeakReference<HornetQConnection> connectionRef;

      FailoverEventListenerImpl(final HornetQConnection connection)
      {
         connectionRef = new WeakReference<HornetQConnection>(connection);
      }

      @Override
      public void failoverEvent(final FailoverEventType eventType)
      {
         HornetQConnection conn = connectionRef.get();

         if (conn != null)
         {
            try
            {
               final FailoverEventListener failoverListener = conn.getFailoverListener();

               if (failoverListener != null)
               {

                  new Thread(new Runnable()
                  {
                     public void run()
                     {
                        failoverListener.failoverEvent(eventType);
                     }
                  }).start();
               }
            }
            catch (JMSException e)
            {
               if (!conn.closed)
               {
                  HornetQJMSClientLogger.LOGGER.errorCallingFailoverListener(e);
               }
            }
         }

      }
   }
}
