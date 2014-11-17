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
package org.apache.activemq.jms.tests;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;

import org.apache.activemq.core.client.impl.ClientSessionInternal;
import org.apache.activemq.jms.tests.util.ProxyAssertSupport;
import org.jboss.tm.TxUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * A XATestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 *
 */
public class XATest extends HornetQServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected TransactionManager tm;

   protected Transaction suspendedTx;

   protected XAConnectionFactory xacf;

   protected ConnectionFactory cf;

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      cf = getConnectionFactory();

      xacf = getXAConnectionFactory();

      tm = getTransactionManager();

      ProxyAssertSupport.assertTrue(tm instanceof TransactionManagerImple);

      suspendedTx = tm.suspend();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (TxUtils.isUncommitted(tm))
      {
         // roll it back
         try
         {
            tm.rollback();
         }
         catch (Throwable ignore)
         {
            // The connection will probably be closed so this may well throw an exception
         }
      }
      if (tm.getTransaction() != null)
      {
         Transaction tx = tm.suspend();
         if (tx != null)
         {
            log.warn("Transaction still associated with thread " + tx +
                     " at status " +
                     TxUtils.getStatusAsString(tx.getStatus()));
         }
      }

      if (suspendedTx != null)
      {
         tm.resume(suspendedTx);
      }
   }

   // Public --------------------------------------------------------

   @Test
   public void test2PCSendCommit1PCOptimization() throws Exception
   {
      // Since both resources have same RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn = xacf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
         TextMessage m2 = (TextMessage)cons.receive(1000);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(1000);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void test2PCSendCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn = xacf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();

         XAResource res = sess.getXAResource();
         XAResource res2 = new DummyXAResource();

         // To prevent 1PC optimization being used
         // res.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();

         tx.enlistResource(res);

         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void test2PCSendRollback1PCOptimization() throws Exception
   {
      // Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = xacf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
         Message m2 = cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(m2);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void test2PCSendFailOnPrepare() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = xacf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         // prevent 1Pc optimisation
         // res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource(true);
         XAResource res3 = new DummyXAResource();
         XAResource res4 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);
         tx.enlistResource(res3);
         tx.enlistResource(res4);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);
         tx.delistResource(res3, XAResource.TMSUCCESS);
         tx.delistResource(res4, XAResource.TMSUCCESS);

         try
         {
            tm.commit();

            ProxyAssertSupport.fail("should not get here");
         }
         catch (Exception e)
         {
            // We should expect this
         }

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
         Message m2 = cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(m2);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void test2PCSendRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = xacf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         // prevent 1Pc optimisation
         // res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
         Message m2 = cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(m2);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void test2PCReceiveCommit1PCOptimization() throws Exception
   {
      // Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue1);

         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());

         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         // New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         Message m3 = cons.receive(HornetQServerTestCase.MIN_TIMEOUT);

         ProxyAssertSupport.assertNull(m3);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void test2PCReceiveCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         // res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue1);

         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());

         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

         // New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         Message m3 = cons.receive(HornetQServerTestCase.MIN_TIMEOUT);

         ProxyAssertSupport.assertNull(m3);

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void test2PCReceiveRollback1PCOptimization() throws Exception
   {
      // Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);

         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue1);

         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         // Message should be redelivered

         // New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         TextMessage m3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m3);
         ProxyAssertSupport.assertEquals("XATest1", m3.getText());
         m3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m3);
         ProxyAssertSupport.assertEquals("XATest2", m3.getText());

         ProxyAssertSupport.assertTrue(m3.getJMSRedelivered());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void test2PCReceiveRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);

         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();
         // res.setForceNotSameRM(true);

         XAResource res2 = new DummyXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         MessageConsumer cons = sess.createConsumer(queue1);

         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         // Message should be redelivered

         // New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);
         tx.enlistResource(res2);

         TextMessage m3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m3);
         ProxyAssertSupport.assertEquals("XATest1", m3.getText());
         m3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m3);
         ProxyAssertSupport.assertEquals("XATest2", m3.getText());

         ProxyAssertSupport.assertTrue(m3.getJMSRedelivered());

         tx.delistResource(res, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.commit();

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void test1PCSendCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn = xacf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void test1PCSendRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;
      try
      {
         conn = xacf.createXAConnection();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageProducer prod = sess.createProducer(queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         Message m = sess.createTextMessage("XATest1");
         prod.send(queue1, m);
         m = sess.createTextMessage("XATest2");
         prod.send(queue1, m);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.rollback();

         conn2 = cf.createConnection();
         conn2.start();
         Session sessReceiver = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sessReceiver.createConsumer(queue1);
         Message m2 = cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(m2);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void test1PCReceiveCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         conn2.start();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageConsumer cons = sess.createConsumer(queue1);

         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());
         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();

         // New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);

         Message m3 = cons.receive(HornetQServerTestCase.MIN_TIMEOUT);

         ProxyAssertSupport.assertNull(m3);

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void test1PCReceiveRollback() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("XATest1");
         prod.send(m);
         m = sessProducer.createTextMessage("XATest2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess = conn.createXASession();
         XAResource res = sess.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res);

         MessageConsumer cons = sess.createConsumer(queue1);

         TextMessage m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest1", m2.getText());

         m2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m2);
         ProxyAssertSupport.assertEquals("XATest2", m2.getText());

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.rollback();

         // Message should be redelivered

         // New tx
         tm.begin();
         tx = tm.getTransaction();
         tx.enlistResource(res);

         TextMessage m3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m3);
         ProxyAssertSupport.assertEquals("XATest1", m3.getText());

         m3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(m3);
         ProxyAssertSupport.assertEquals("XATest2", m3.getText());

         ProxyAssertSupport.assertTrue(m3.getJMSRedelivered());

         tx.delistResource(res, XAResource.TMSUCCESS);

         tm.commit();

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void testMultipleSessionsOneTxCommitAcknowledge1PCOptimization() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      // Since both resources have some RM, TM will probably use 1PC optimization

      try
      {
         // First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         XAResource res2 = sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Receive the messages, one on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish1", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue1);
         TextMessage r2 = (TextMessage)cons2.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish2", r2.getText());

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         // commit
         tm.commit();

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r3 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r3);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void testMultipleSessionsOneTxCommitAcknowledge() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         // First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         ClientSessionInternal res1 = (ClientSessionInternal)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionInternal res2 = (ClientSessionInternal)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Receive the messages, one on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish1", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue1);
         TextMessage r2 = (TextMessage)cons2.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish2", r2.getText());

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         // commit
         tm.commit();

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r3 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r3);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void testMultipleSessionsOneTxRollbackAcknowledge1PCOptimization() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      // Since both resources have some RM, TM will probably use 1PC optimization

      try
      {
         // First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish3");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish4");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         ClientSessionInternal res1 = (ClientSessionInternal)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionInternal res2 = (ClientSessionInternal)sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Receive the messages, two on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish2", r1.getText());

         cons1.close();

         MessageConsumer cons2 = sess2.createConsumer(queue1);
         TextMessage r2 = (TextMessage)cons2.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish3", r2.getText());

         r2 = (TextMessage)cons2.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish4", r2.getText());

         cons2.close();

         // rollback

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         // Rollback causes cancel which is asynch
         Thread.sleep(1000);

         // We cannot assume anything about the order in which the transaction manager rollsback
         // the sessions - this is implementation dependent

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r);

         boolean session1First = false;

         if (r.getText().equals("jellyfish1"))
         {
            session1First = true;
         }
         else if (r.getText().equals("jellyfish3"))
         {
            session1First = false;
         }
         else
         {
            ProxyAssertSupport.fail("Unexpected message");
         }

         if (session1First)
         {
            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish2", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish3", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish4", r.getText());

         }
         else
         {
            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish4", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish1", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish2", r.getText());
         }

         r = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);

         ProxyAssertSupport.assertNull(r);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void testMultipleSessionsOneTxRollbackAcknowledge() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         // First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish3");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish4");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         ClientSessionInternal res1 = (ClientSessionInternal)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionInternal res2 = (ClientSessionInternal)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Receive the messages, two on each consumer
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish2", r1.getText());

         cons1.close();

         // Cancel is asynch
         Thread.sleep(500);

         MessageConsumer cons2 = sess2.createConsumer(queue1);
         TextMessage r2 = (TextMessage)cons2.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish3", r2.getText());

         r2 = (TextMessage)cons2.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish4", r2.getText());

         // rollback

         cons2.close();

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         tm.rollback();

         // Rollback causes cancel which is asynch
         Thread.sleep(1000);

         // We cannot assume anything about the order in which the transaction manager rollsback
         // the sessions - this is implementation dependent

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r);

         boolean session1First = false;

         if (r.getText().equals("jellyfish1"))
         {
            session1First = true;
         }
         else if (r.getText().equals("jellyfish3"))
         {
            session1First = false;
         }
         else
         {
            ProxyAssertSupport.fail("Unexpected message");
         }

         if (session1First)
         {
            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish2", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish3", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish4", r.getText());

         }
         else
         {
            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish4", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish1", r.getText());

            r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

            ProxyAssertSupport.assertNotNull(r);

            ProxyAssertSupport.assertEquals("jellyfish2", r.getText());
         }

         r = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);

         ProxyAssertSupport.assertNull(r);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void testMultipleSessionsOneTxRollbackAcknowledgeForceFailureInCommit() throws Exception
   {
      XAConnection conn = null;
      Connection conn2 = null;

      try
      {
         // First send 4 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);

         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish3");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish4");
         prod.send(m);

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();
         DummyXAResource res2 = new DummyXAResource(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         MessageConsumer cons1 = sess1.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish1", r1.getText());

         r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish2", r1.getText());

         r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish3", r1.getText());

         r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish4", r1.getText());

         r1 = (TextMessage)cons1.receive(1000);

         ProxyAssertSupport.assertNull(r1);

         cons1.close();

         // try and commit - and we're going to make the dummyxaresource throw an exception on commit,
         // which should cause rollback to be called on the other resource

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         // rollback will cause an attempt to deliver messages locally to the original consumers.
         // the original consumer has closed, so it will cancelled to the server
         // the server cancel is asynch, so we need to sleep for a bit to make sure it completes
         log.trace("Forcing failure");
         try
         {
            tm.commit();
            ProxyAssertSupport.fail("should not get here");
         }
         catch (Exception e)
         {
            // We should expect this
         }

         Thread.sleep(1000);

         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r);

         ProxyAssertSupport.assertEquals("jellyfish1", r.getText());

         r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r);

         ProxyAssertSupport.assertEquals("jellyfish2", r.getText());

         r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r);

         ProxyAssertSupport.assertEquals("jellyfish3", r.getText());

         r = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r);

         ProxyAssertSupport.assertEquals("jellyfish4", r.getText());

         r = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);

         ProxyAssertSupport.assertNull(r);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }

   @Test
   public void testMultipleSessionsOneTxCommitSend1PCOptimization() throws Exception
   {
      // Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         XAResource res2 = sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         // commit
         tm.commit();

         // Messages should be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("echidna1", r1.getText());

         TextMessage r2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("echidna2", r2.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void testMultipleSessionsOneTxCommitSend() throws Exception
   {
      // Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         ClientSessionInternal res1 = (ClientSessionInternal)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionInternal res2 = (ClientSessionInternal)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         // commit
         tm.commit();

         // Messages should be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("echidna1", r1.getText());

         TextMessage r2 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("echidna2", r2.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }

   @Test
   public void testMultipleSessionsOneTxRollbackSend1PCOptimization() throws Exception
   {
      // Since both resources have some RM, TM will probably use 1PC optimization

      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         XAResource res2 = sess2.getXAResource();

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         // rollback
         tm.rollback();

         // Messages should not be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r1);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void testMultipleSessionsOneTxRollbackSend() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = xacf.createXAConnection();
         conn.start();

         tm.begin();

         // Create 2 sessions and enlist them
         XASession sess1 = conn.createXASession();
         ClientSessionInternal res1 = (ClientSessionInternal)sess1.getXAResource();
         XASession sess2 = conn.createXASession();
         ClientSessionInternal res2 = (ClientSessionInternal)sess2.getXAResource();
         res1.setForceNotSameRM(true);
         res2.setForceNotSameRM(true);

         Transaction tx = tm.getTransaction();
         tx.enlistResource(res1);
         tx.enlistResource(res2);

         // Send 2 messages - one from each session

         MessageProducer prod1 = sess1.createProducer(queue1);
         MessageProducer prod2 = sess2.createProducer(queue1);

         prod1.send(sess1.createTextMessage("echidna1"));
         prod2.send(sess2.createTextMessage("echidna2"));

         tx.delistResource(res1, XAResource.TMSUCCESS);
         tx.delistResource(res2, XAResource.TMSUCCESS);

         // rollback
         tm.rollback();

         // Messages should not be in queue

         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue1);
         conn2.start();

         TextMessage r1 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r1);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void testOneSessionTwoTransactionsCommitAcknowledge() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         // First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = xacf.createXAConnection();

         // Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         conn.start();
         MessageConsumer cons1 = sess1.createConsumer(queue1);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         // Receive one message in one tx

         TextMessage r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish1", r1.getText());

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         // suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         // Receive 2nd message in a different tx
         TextMessage r2 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish2", r2.getText());

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         // commit this transaction
         tm.commit();

         // verify that no messages are available
         conn2.close();
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue1);
         TextMessage r3 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r3);

         // now resume the first tx and then commit it
         tm.resume(suspended);

         tm.commit();
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   @Test
   public void testOneSessionTwoTransactionsRollbackAcknowledge() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         // First send 2 messages
         conn2 = cf.createConnection();
         Session sessProducer = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProducer.createProducer(queue1);
         Message m = sessProducer.createTextMessage("jellyfish1");
         prod.send(m);
         m = sessProducer.createTextMessage("jellyfish2");
         prod.send(m);

         conn = xacf.createXAConnection();

         // Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         conn.start();
         MessageConsumer cons1 = sess1.createConsumer(queue1);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         // Receive one message in one tx

         TextMessage r1 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("jellyfish1", r1.getText());

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         // suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         // Receive 2nd message in a different tx
         TextMessage r2 = (TextMessage)cons1.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r2);
         ProxyAssertSupport.assertEquals("jellyfish2", r2.getText());

         cons1.close();

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         // rollback this transaction
         tm.rollback();

         // verify that second message is available
         conn2.close();
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue1);

         TextMessage r3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(r3);
         ProxyAssertSupport.assertEquals("jellyfish2", r3.getText());
         r3 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r3);

         // rollback the other tx
         tm.resume(suspended);
         tm.rollback();

         // Verify the first message is now available
         r3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r3);
         ProxyAssertSupport.assertEquals("jellyfish1", r3.getText());
         r3 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r3);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }

   @Test
   public void testOneSessionTwoTransactionsCommitSend() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {
         conn = xacf.createXAConnection();

         // Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         MessageProducer prod1 = sess1.createProducer(queue1);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         // Send a message
         prod1.send(sess1.createTextMessage("kangaroo1"));

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         // suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         // Send another message in another tx using the same session
         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         // Send a message
         prod1.send(sess1.createTextMessage("kangaroo2"));

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         // commit this transaction
         tm.commit();

         // verify only kangaroo2 message is sent
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r1);
         ProxyAssertSupport.assertEquals("kangaroo2", r1.getText());
         TextMessage r2 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);
         ProxyAssertSupport.assertNull(r2);

         // now resume the first tx and then commit it
         tm.resume(suspended);

         tm.commit();

         // verify that the first text message is received
         TextMessage r3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r3);
         ProxyAssertSupport.assertEquals("kangaroo1", r3.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }

   @Test
   public void testIsSamRM() throws Exception
   {
      XAConnection conn = null;

      conn = xacf.createXAConnection();

      // Create a session
      XASession sess1 = conn.createXASession();
      XAResource res1 = sess1.getXAResource();

      // Create a session
      XASession sess2 = conn.createXASession();
      XAResource res2 = sess2.getXAResource();

      ProxyAssertSupport.assertTrue(res1.isSameRM(res2));
   }

   @Test
   public void testOneSessionTwoTransactionsRollbackSend() throws Exception
   {
      XAConnection conn = null;

      Connection conn2 = null;

      try
      {

         conn = xacf.createXAConnection();

         // Create a session
         XASession sess1 = conn.createXASession();
         XAResource res1 = sess1.getXAResource();

         MessageProducer prod1 = sess1.createProducer(queue1);

         tm.begin();

         Transaction tx1 = tm.getTransaction();
         tx1.enlistResource(res1);

         // Send a message
         prod1.send(sess1.createTextMessage("kangaroo1"));

         // suspend the tx
         Transaction suspended = tm.suspend();

         tm.begin();

         // Send another message in another tx using the same session
         Transaction tx2 = tm.getTransaction();
         tx2.enlistResource(res1);

         // Send a message
         prod1.send(sess1.createTextMessage("kangaroo2"));

         tx2.delistResource(res1, XAResource.TMSUCCESS);

         // rollback this transaction
         tm.rollback();

         // verify no messages are sent
         conn2 = cf.createConnection();
         Session sess = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn2.start();
         MessageConsumer cons = sess.createConsumer(queue1);
         TextMessage r1 = (TextMessage)cons.receive(HornetQServerTestCase.MIN_TIMEOUT);

         ProxyAssertSupport.assertNull(r1);

         // now resume the first tx and then commit it
         tm.resume(suspended);

         tx1.delistResource(res1, XAResource.TMSUCCESS);

         tm.commit();

         // verify that the first text message is received
         TextMessage r3 = (TextMessage)cons.receive(HornetQServerTestCase.MAX_TIMEOUT);
         ProxyAssertSupport.assertNotNull(r3);
         ProxyAssertSupport.assertEquals("kangaroo1", r3.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }

      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   static class DummyListener implements MessageListener
   {
      protected JmsTestLogger log = JmsTestLogger.LOGGER;

      public List<Message> messages = new ArrayList<Message>();

      public void onMessage(final Message message)
      {
         messages.add(message);
      }
   }

   static class DummyXAResource implements XAResource
   {
      boolean failOnPrepare;

      DummyXAResource()
      {
      }

      DummyXAResource(final boolean failOnPrepare)
      {
         this.failOnPrepare = failOnPrepare;
      }

      public void commit(final Xid arg0, final boolean arg1) throws XAException
      {
      }

      public void end(final Xid arg0, final int arg1) throws XAException
      {
      }

      public void forget(final Xid arg0) throws XAException
      {
      }

      public int getTransactionTimeout() throws XAException
      {
         return 0;
      }

      public boolean isSameRM(final XAResource arg0) throws XAException
      {
         return false;
      }

      public int prepare(final Xid arg0) throws XAException
      {
         if (failOnPrepare)
         {
            throw new XAException(XAException.XAER_RMFAIL);
         }
         return XAResource.XA_OK;
      }

      public Xid[] recover(final int arg0) throws XAException
      {
         return null;
      }

      public void rollback(final Xid arg0) throws XAException
      {
      }

      public boolean setTransactionTimeout(final int arg0) throws XAException
      {
         return false;
      }

      public void start(final Xid arg0, final int arg1) throws XAException
      {

      }

   }

}
