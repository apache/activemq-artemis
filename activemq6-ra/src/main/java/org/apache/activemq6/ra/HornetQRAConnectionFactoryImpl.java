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
package org.apache.activemq6.ra;

import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAJMSContext;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;

import org.apache.activemq6.jms.client.HornetQConnectionFactory;
import org.apache.activemq6.jms.referenceable.ConnectionFactoryObjectFactory;
import org.apache.activemq6.jms.referenceable.SerializableObjectRefAddr;

/**
 * The connection factory
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAConnectionFactoryImpl implements HornetQRAConnectionFactory
{
   /**
    * Serial version UID
    */
   static final long serialVersionUID = 7981708919479859360L;
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * The managed connection factory
    */
   private final HornetQRAManagedConnectionFactory mcf;

   /**
    * The connection manager
    */
   private ConnectionManager cm;

   /**
    * Naming reference
    */
   private Reference reference;

   /**
    * Constructor
    *
    * @param mcf The managed connection factory
    * @param cm  The connection manager
    */
   public HornetQRAConnectionFactoryImpl(final HornetQRAManagedConnectionFactory mcf, final ConnectionManager cm)
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + mcf + ", " + cm + ")");
      }

      this.mcf = mcf;

      if (cm == null)
      {
         // This is standalone usage, no appserver
         this.cm = new HornetQRAConnectionManager();
         if (HornetQRAConnectionFactoryImpl.trace)
         {
            HornetQRALogger.LOGGER.trace("Created new ConnectionManager=" + this.cm);
         }
      }
      else
      {
         this.cm = cm;
      }

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Using ManagedConnectionFactory=" + mcf + ", ConnectionManager=" + cm);
      }
   }

   /**
    * Set the reference
    *
    * @param reference The reference
    */
   public void setReference(final Reference reference)
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("setReference(" + reference + ")");
      }

      this.reference = reference;
   }

   /**
    * Get the reference
    *
    * @return The reference
    */
   public Reference getReference()
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("getReference()");
      }
      if (reference == null)
      {
         try
         {
            reference = new Reference(this.getClass().getCanonicalName(),
                                      new SerializableObjectRefAddr("HornetQ-CF", this),
                                      ConnectionFactoryObjectFactory.class.getCanonicalName(),
                                      null);
         }
         catch (NamingException e)
         {
            HornetQRALogger.LOGGER.errorCreatingReference(e);
         }
      }

      return reference;

   }

   /**
    * Create a queue connection
    *
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public QueueConnection createQueueConnection() throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createQueueConnection()");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.QUEUE_CONNECTION);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created queue connection: " + s);
      }

      return s;
   }

   /**
    * Create a queue connection
    *
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public QueueConnection createQueueConnection(final String userName, final String password) throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createQueueConnection(" + userName + ", ****)");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.QUEUE_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);

      validateUser(s);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created queue connection: " + s);
      }

      return s;
   }

   /**
    * Create a topic connection
    *
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public TopicConnection createTopicConnection() throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createTopicConnection()");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.TOPIC_CONNECTION);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created topic connection: " + s);
      }

      return s;
   }

   /**
    * Create a topic connection
    *
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public TopicConnection createTopicConnection(final String userName, final String password) throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createTopicConnection(" + userName + ", ****)");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.TOPIC_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      validateUser(s);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created topic connection: " + s);
      }

      return s;
   }

   /**
    * Create a connection
    *
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public Connection createConnection() throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnection()");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf, cm, getResourceAdapter().getTM(), HornetQRAConnectionFactory.CONNECTION);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created connection: " + s);
      }

      return s;
   }

   /**
    * Create a connection
    *
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public Connection createConnection(final String userName, final String password) throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnection(" + userName + ", ****)");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf, cm, getResourceAdapter().getTM(), HornetQRAConnectionFactory.CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);

      validateUser(s);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created connection: " + s);
      }

      return s;
   }

   /**
    * Create a XA queue connection
    *
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXAQueueConnection()");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.XA_QUEUE_CONNECTION);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created queue connection: " + s);
      }

      return s;
   }

   /**
    * Create a XA  queue connection
    *
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public XAQueueConnection createXAQueueConnection(final String userName, final String password) throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXAQueueConnection(" + userName + ", ****)");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.XA_QUEUE_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      validateUser(s);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created queue connection: " + s);
      }

      return s;
   }

   /**
    * Create a XA topic connection
    *
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public XATopicConnection createXATopicConnection() throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXATopicConnection()");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.XA_TOPIC_CONNECTION);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created topic connection: " + s);
      }

      return s;
   }

   /**
    * Create a XA topic connection
    *
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public XATopicConnection createXATopicConnection(final String userName, final String password) throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXATopicConnection(" + userName + ", ****)");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf,
                                                                      cm,
                                                                      getResourceAdapter().getTM(),
                                                                      HornetQRAConnectionFactory.XA_TOPIC_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      validateUser(s);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created topic connection: " + s);
      }

      return s;
   }

   /**
    * Create a XA connection
    *
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public XAConnection createXAConnection() throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXAConnection()");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf, cm, getResourceAdapter().getTM(), HornetQRAConnectionFactory.XA_CONNECTION);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created connection: " + s);
      }

      return s;
   }

   /**
    * Create a XA connection
    *
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @throws JMSException Thrown if the operation fails
    */
   public XAConnection createXAConnection(final String userName, final String password) throws JMSException
   {
      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("createXAConnection(" + userName + ", ****)");
      }

      HornetQRASessionFactoryImpl s = new HornetQRASessionFactoryImpl(mcf, cm, getResourceAdapter().getTM(), HornetQRAConnectionFactory.XA_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      validateUser(s);

      if (HornetQRAConnectionFactoryImpl.trace)
      {
         HornetQRALogger.LOGGER.trace("Created connection: " + s);
      }

      return s;
   }

   @Override
   public JMSContext createContext()
   {
      return createContext(null, null);
   }

   @Override
   public JMSContext createContext(String userName, String password)
   {
      return createContext(userName, password, Session.AUTO_ACKNOWLEDGE);
   }

   @Override
   public JMSContext createContext(String userName, String password, int sessionMode)
   {
      @SuppressWarnings("resource")
      HornetQRASessionFactoryImpl conn = new HornetQRASessionFactoryImpl(mcf, cm, getResourceAdapter().getTM(), HornetQRAConnectionFactory.CONNECTION);
      conn.setUserName(userName);
      conn.setPassword(password);
      try
      {
         validateUser(conn);
      }
      catch (JMSSecurityException e)
      {
         JMSSecurityRuntimeException e2 = new JMSSecurityRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
      catch (JMSException e)
      {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
      return conn.createContext(sessionMode);
   }

   @Override
   public JMSContext createContext(int sessionMode)
   {
      return createContext(null, null, sessionMode);
   }

   @Override
   public XAJMSContext createXAContext()
   {
      return createXAContext(null, null);
   }

   @Override
   public XAJMSContext createXAContext(String userName, String password)
   {
      HornetQRASessionFactoryImpl conn = new HornetQRASessionFactoryImpl(mcf, cm, getResourceAdapter().getTM(), HornetQRAConnectionFactory.XA_CONNECTION);
      conn.setUserName(userName);
      conn.setPassword(password);
      try
      {
         validateUser(conn);
      }
      catch (JMSSecurityException e)
      {
         JMSSecurityRuntimeException e2 = new JMSSecurityRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
      catch (JMSException e)
      {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
      return conn.createXAContext();
   }

   private void validateUser(HornetQRASessionFactoryImpl s) throws JMSException
   {
      Session session = s.createSession();
      session.close();
   }

   @Override
   public HornetQConnectionFactory getDefaultFactory() throws ResourceException
   {
      return ((HornetQResourceAdapter) mcf.getResourceAdapter()).getDefaultHornetQConnectionFactory();
   }

   @Override
   public HornetQResourceAdapter getResourceAdapter()
   {
      return (HornetQResourceAdapter) mcf.getResourceAdapter();
   }
}
