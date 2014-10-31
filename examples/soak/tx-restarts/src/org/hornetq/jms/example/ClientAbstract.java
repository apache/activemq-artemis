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
package org.hornetq.jms.example;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Logger;

import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.utils.UUIDGenerator;

/**
 * WARNING: This is not a sample on how you should handle XA.
 *          You are supposed to use a TransactionManager.
 *          This class is doing the job of a TransactionManager that fits for the purpose of this test only,
 *          however there are many more pitfalls to deal with Transactions.
 *
 *          This is just to stress and soak test Transactions with HornetQ.
 *
 *          And this is dealing with XA directly for the purpose testing only.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public abstract class ClientAbstract extends Thread
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(ClientAbstract.class.getName());

   // Attributes ----------------------------------------------------

   protected InitialContext ctx;

   protected XAConnection conn;

   protected XASession sess;

   protected XAResource activeXAResource;

   protected Xid activeXid;

   protected volatile boolean running = true;

   protected volatile int errors = 0;

   /**
    * A commit was called
    * case we don't find the Xid, means it was accepted
    */
   protected volatile boolean pendingCommit = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected InitialContext getContext(final int serverId) throws Exception
   {
      String jndiFilename = "server" + serverId + "/client-jndi.properties";
      File jndiFile = new File(jndiFilename);
      Properties props = new Properties();
      FileInputStream inStream = null;
      try
      {
         inStream = new FileInputStream(jndiFile);
         props.load(inStream);
      }
      finally
      {
         if (inStream != null)
         {
            inStream.close();
         }
      }
      return new InitialContext(props);

   }

   public XAConnection getConnection()
   {
      return conn;
   }

   public int getErrorsCount()
   {
      return errors;
   }

   public final void connect()
   {
      while (running)
      {
         try
         {
            disconnect();

            ctx = getContext(0);

            XAConnectionFactory cf = (XAConnectionFactory)ctx.lookup("/ConnectionFactory");

            conn = cf.createXAConnection();

            sess = conn.createXASession();

            activeXAResource = sess.getXAResource();

            if (activeXid != null)
            {
               synchronized (ClientAbstract.class)
               {
                  Xid[] xids = activeXAResource.recover(XAResource.TMSTARTRSCAN);
                  boolean found = false;
                  for (Xid recXid : xids)
                  {
                     if (recXid.equals(activeXid))
                     {
                        // System.out.println("Calling commit after a prepare on " + this);
                        found = true;
                        callCommit();
                     }
                  }

                  if (!found)
                  {
                     if (pendingCommit)
                     {
                        System.out.println("Doing a commit based on a pending commit on " + this);
                        onCommit();
                     }
                     else
                     {
                        System.out.println("Doing a rollback on " + this);
                        onRollback();
                     }

                     activeXid = null;
                     pendingCommit = false;
                  }
               }
            }

            connectClients();

            break;
         }
         catch (Exception e)
         {
            ClientAbstract.log.warning("Can't connect to server, retrying");
            disconnect();
            try
            {
               Thread.sleep(1000);
            }
            catch (InterruptedException ignored)
            {
               // if an interruption was sent, we will respect it and leave the loop
               break;
            }
         }
      }
   }

   @Override
   public void run()
   {
      connect();
   }

   protected void callCommit() throws Exception
   {
      pendingCommit = true;
      activeXAResource.commit(activeXid, false);
      pendingCommit = false;
      activeXid = null;
      onCommit();
   }

   protected void callPrepare() throws Exception
   {
      activeXAResource.prepare(activeXid);
   }

   public void beginTX() throws Exception
   {
      activeXid = newXID();

      activeXAResource.start(activeXid, XAResource.TMNOFLAGS);
   }

   public void endTX() throws Exception
   {
      activeXAResource.end(activeXid, XAResource.TMSUCCESS);
      callPrepare();
      callCommit();
   }

   public void setRunning(final boolean running)
   {
      this.running = running;
   }

   /**
    * @return
    */
   private XidImpl newXID()
   {
      return new XidImpl("tst".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
   }

   protected abstract void connectClients() throws Exception;

   protected abstract void onCommit();

   protected abstract void onRollback();

   public void disconnect()
   {
      try
      {
         if (conn != null)
         {
            conn.close();
         }
      }
      catch (Exception ignored)
      {
         ignored.printStackTrace();
      }

      try
      {
         if (ctx != null)
         {
            ctx.close();
         }
      }
      catch (Exception ignored)
      {
         ignored.printStackTrace();
      }

      ctx = null;
      conn = null;
      // it's not necessary to close the session as conn.close() will already take care of that
      sess = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
