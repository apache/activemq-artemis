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

package org.hornetq.spi.core.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.spi.core.remoting.Connection;

/**
 * @author Clebert Suconic
 */

public abstract class AbstractRemotingConnection implements RemotingConnection
{
   protected final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();
   protected final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();
   protected final Connection transportConnection;
   protected final Executor executor;
   protected final long creationTime;
   protected volatile boolean dataReceived;

   public AbstractRemotingConnection(final Connection transportConnection, final Executor executor)
   {
      this.transportConnection = transportConnection;
      this.executor = executor;
      this.creationTime = System.currentTimeMillis();
   }

   public List<FailureListener> getFailureListeners()
   {
      return new ArrayList<FailureListener>(failureListeners);
   }

   protected void callFailureListeners(final HornetQException me, String scaleDownTargetNodeID)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me, false, scaleDownTargetNodeID);
         }
         catch (HornetQInterruptedException interrupted)
         {
            // this is an expected behaviour.. no warn or error here
            HornetQClientLogger.LOGGER.debug("thread interrupted", interrupted);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }


   protected void callClosingListeners()
   {
      final List<CloseListener> listenersClone = new ArrayList<CloseListener>(closeListeners);

      for (final CloseListener listener : listenersClone)
      {
         try
         {
            listener.connectionClosed();
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   public void setFailureListeners(final List<FailureListener> listeners)
   {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   public Object getID()
   {
      return transportConnection.getID();
   }

   public String getRemoteAddress()
   {
      return transportConnection.getRemoteAddress();
   }

   public void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.failListenerCannotBeNull();
      }
      failureListeners.add(listener);
   }

   public boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.failListenerCannotBeNull();
      }

      return failureListeners.remove(listener);
   }

   public void addCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.closeListenerCannotBeNull();
      }

      closeListeners.add(listener);
   }

   public boolean removeCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.closeListenerCannotBeNull();
      }

      return closeListeners.remove(listener);
   }

   public List<CloseListener> removeCloseListeners()
   {
      List<CloseListener> ret = new ArrayList<CloseListener>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   public List<FailureListener> removeFailureListeners()
   {
      List<FailureListener> ret = getFailureListeners();

      failureListeners.clear();

      return ret;
   }

   public void setCloseListeners(List<CloseListener> listeners)
   {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   public HornetQBuffer createBuffer(final int size)
   {
      return transportConnection.createBuffer(size);
   }

   public Connection getTransportConnection()
   {
      return transportConnection;
   }

   public long getCreationTime()
   {
      return creationTime;
   }

   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   public void fail(final HornetQException me)
   {
      fail(me, null);
   }

   public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
   {
      dataReceived = true;
   }

}
