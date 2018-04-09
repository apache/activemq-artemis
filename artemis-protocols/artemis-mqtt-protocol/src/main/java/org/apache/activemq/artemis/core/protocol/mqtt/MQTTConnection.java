/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.mqtt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;

import javax.security.auth.Subject;

public class MQTTConnection implements RemotingConnection {

   private final Connection transportConnection;

   private final long creationTime;

   private AtomicBoolean dataReceived;

   private boolean destroyed;

   private boolean connected;

   private String clientID;

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<>();

   public MQTTConnection(Connection transportConnection) throws Exception {
      this.transportConnection = transportConnection;
      this.creationTime = System.currentTimeMillis();
      this.dataReceived = new AtomicBoolean();
      this.destroyed = false;
   }


   @Override
   public void scheduledFlush() {
      flush();
   }

   @Override
   public boolean isWritable(ReadyListener callback) {
      return transportConnection.isWritable(callback);
   }

   @Override
   public Object getID() {
      return transportConnection.getID();
   }

   @Override
   public long getCreationTime() {
      return creationTime;
   }

   @Override
   public String getRemoteAddress() {
      return transportConnection.getRemoteAddress();
   }

   @Override
   public void addFailureListener(FailureListener listener) {
      failureListeners.add(listener);
   }

   @Override
   public boolean removeFailureListener(FailureListener listener) {
      return failureListeners.remove(listener);
   }

   @Override
   public void addCloseListener(CloseListener listener) {
      closeListeners.add(listener);
   }

   @Override
   public boolean removeCloseListener(CloseListener listener) {
      return closeListeners.remove(listener);
   }

   @Override
   public List<CloseListener> removeCloseListeners() {
      List<CloseListener> deletedCloseListeners = copyCloseListeners();
      closeListeners.clear();
      return deletedCloseListeners;
   }

   @Override
   public void setCloseListeners(List<CloseListener> listeners) {
      closeListeners.clear();
      closeListeners.addAll(listeners);
   }

   @Override
   public List<FailureListener> getFailureListeners() {
      return failureListeners;
   }

   @Override
   public List<FailureListener> removeFailureListeners() {
      List<FailureListener> deletedFailureListeners = copyFailureListeners();
      failureListeners.clear();
      return deletedFailureListeners;
   }

   @Override
   public void setFailureListeners(List<FailureListener> listeners) {
      failureListeners.clear();
      failureListeners.addAll(listeners);
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(int size) {
      return transportConnection.createTransportBuffer(size);
   }

   @Override
   public void fail(ActiveMQException me) {
      List<FailureListener> copy = copyFailureListeners();
      for (FailureListener listener : copy) {
         listener.connectionFailed(me, false);
      }
   }

   private List<FailureListener> copyFailureListeners() {
      return new ArrayList<>(failureListeners);
   }

   private List<CloseListener> copyCloseListeners() {
      return new ArrayList<>(closeListeners);
   }

   @Override
   public void fail(ActiveMQException me, String scaleDownTargetNodeID) {
      synchronized (failureListeners) {
         for (FailureListener listener : failureListeners) {
            //FIXME(mtaylor) How do we check if the node has failed over?
            listener.connectionFailed(me, false);
         }
      }
   }

   @Override
   public void destroy() {
      //TODO(mtaylor) ensure this properly destroys this connection.
      destroyed = true;
      disconnect(false);
   }

   @Override
   public Connection getTransportConnection() {
      return transportConnection;
   }

   @Override
   public boolean isClient() {
      return false;
   }

   @Override
   public boolean isDestroyed() {
      return destroyed;
   }

   @Override
   public void disconnect(boolean criticalError) {
      transportConnection.forceClose();
   }

   @Override
   public void disconnect(String scaleDownNodeID, boolean criticalError) {
      transportConnection.forceClose();
   }

   protected void dataReceived() {
      dataReceived.set(true);
   }

   @Override
   public boolean checkDataReceived() {
      return dataReceived.compareAndSet(true, false);
   }

   @Override
   public void flush() {
      transportConnection.checkFlushBatchBuffer();
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
   }

   public void setConnected(boolean connected) {
      this.connected = connected;
   }

   public boolean getConnected() {
      return connected;
   }

   @Override
   public void killMessage(SimpleString nodeID) {
      //unsupported
   }

   @Override
   public boolean isSupportReconnect() {
      return false;
   }

   @Override
   public boolean isSupportsFlowControl() {
      return false;
   }

   @Override
   public Subject getSubject() {
      return null;
   }

   /**
    * Returns the name of the protocol for this Remoting Connection
    *
    * @return
    */
   @Override
   public String getProtocolName() {
      return MQTTProtocolManagerFactory.MQTT_PROTOCOL_NAME;
   }

   /**
    * Sets the client ID associated with this connection
    *
    * @param cID
    * @return
    */
   @Override
   public void setClientID(String cID) {
      this.clientID = cID;
   }

   /**
    * Returns the Client ID associated with this connection
    *
    * @return
    */
   @Override
   public String getClientID() {
      return clientID;
   }

   @Override
   public String getTransportLocalAddress() {
      return getTransportConnection().getLocalAddress();
   }

}
