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
package org.apache.activemq.artemis.quorum.zookeeper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;

import static java.util.stream.Collectors.joining;

public class CuratorDistributedPrimitiveManager implements DistributedPrimitiveManager, ConnectionStateListener {

   private static final String CONNECT_STRING_PARAM = "connect-string";
   private static final String NAMESPACE_PARAM = "namespace";
   private static final String SESSION_MS_PARAM = "session-ms";
   private static final String SESSION_PERCENT_PARAM = "session-percent";
   private static final String CONNECTION_MS_PARAM = "connection-ms";
   private static final String RETRIES_PARAM = "retries";
   private static final String RETRIES_MS_PARAM = "retries-ms";
   private static final Set<String> VALID_PARAMS = Stream.of(
      CONNECT_STRING_PARAM,
      NAMESPACE_PARAM,
      SESSION_MS_PARAM,
      SESSION_PERCENT_PARAM,
      CONNECTION_MS_PARAM,
      RETRIES_PARAM,
      RETRIES_MS_PARAM).collect(Collectors.toSet());
   private static final String VALID_PARAMS_ON_ERROR = VALID_PARAMS.stream().collect(joining(","));
   // It's 9 times the default ZK tick time ie 2000 ms
   private static final String DEFAULT_SESSION_TIMEOUT_MS = Integer.toString(18_000);
   private static final String DEFAULT_CONNECTION_TIMEOUT_MS = Integer.toString(8_000);
   private static final String DEFAULT_RETRIES = Integer.toString(1);
   private static final String DEFAULT_RETRIES_MS = Integer.toString(1000);
   // why 1/3 of the session? https://cwiki.apache.org/confluence/display/CURATOR/TN14
   private static final String DEFAULT_SESSION_PERCENT = Integer.toString(33);

   private static Map<String, String> validateParameters(Map<String, String> config) {
      config.forEach((parameterName, ignore) -> validateParameter(parameterName));
      return config;
   }

   private static void validateParameter(String parameterName) {
      if (!VALID_PARAMS.contains(parameterName)) {
         throw new IllegalArgumentException("non existent parameter " + parameterName + ": accepted list is " + VALID_PARAMS_ON_ERROR);
      }
   }

   private volatile CuratorFramework client;
   private final Map<String, CuratorDistributedLock> locks;
   private final Map<String, CuratorMutableLong> longs;
   private CopyOnWriteArrayList<UnavailableManagerListener> listeners;
   private boolean unavailable;
   private boolean handlingEvents;
   private final CuratorFrameworkFactory.Builder curatorBuilder;

   public CuratorDistributedPrimitiveManager(Map<String, String> config) {
      this(validateParameters(config), true);
   }

   private CuratorDistributedPrimitiveManager(Map<String, String> config, boolean ignore) {
      this(config.get(CONNECT_STRING_PARAM),
           config.get(NAMESPACE_PARAM),
           Integer.parseInt(config.getOrDefault(SESSION_MS_PARAM, DEFAULT_SESSION_TIMEOUT_MS)),
           Integer.parseInt(config.getOrDefault(SESSION_PERCENT_PARAM, DEFAULT_SESSION_PERCENT)),
           Integer.parseInt(config.getOrDefault(CONNECTION_MS_PARAM, DEFAULT_CONNECTION_TIMEOUT_MS)),
           Integer.parseInt(config.getOrDefault(RETRIES_PARAM, DEFAULT_RETRIES)),
           Integer.parseInt(config.getOrDefault(RETRIES_MS_PARAM, DEFAULT_RETRIES_MS)));
   }

   private CuratorDistributedPrimitiveManager(String connectString,
                                              String namespace,
                                              int sessionMs,
                                              int sessionPercent,
                                              int connectionMs,
                                              int retries,
                                              int retriesMs) {
      curatorBuilder = CuratorFrameworkFactory.builder()
         .connectString(connectString)
         .namespace(namespace)
         .sessionTimeoutMs(sessionMs)
         .connectionTimeoutMs(connectionMs)
         .retryPolicy(retries >= 0 ? new RetryNTimes(retries, retriesMs) : new RetryForever(retriesMs))
         .simulatedSessionExpirationPercent(sessionPercent);
      this.locks = new HashMap<>();
      this.longs = new HashMap<>();
      this.listeners = null;
      this.unavailable = false;
      this.handlingEvents = false;
   }

   @Override
   public synchronized boolean isStarted() {
      checkHandlingEvents();
      return client != null;
   }

   @Override
   public synchronized void addUnavailableManagerListener(UnavailableManagerListener listener) {
      checkHandlingEvents();
      if (listeners == null) {
         return;
      }
      listeners.add(listener);
      if (unavailable) {
         handlingEvents = true;
         try {
            listener.onUnavailableManagerEvent();
         } finally {
            handlingEvents = true;
         }
      }
   }

   @Override
   public synchronized void removeUnavailableManagerListener(UnavailableManagerListener listener) {
      checkHandlingEvents();
      if (listeners == null) {
         return;
      }
      listeners.remove(listener);
   }

   @Override
   public synchronized boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      checkHandlingEvents();
      if (timeout >= 0) {
         if (timeout > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("curator manager won't support too long timeout ie >" + Integer.MAX_VALUE);
         }
         Objects.requireNonNull(unit);
      }
      if (client != null) {
         return true;
      }
      final CuratorFramework client = curatorBuilder.build();
      try {
         client.start();
         if (!client.blockUntilConnected((int) timeout, unit)) {
            client.close();
            return false;
         }
         this.client = client;
         this.listeners = new CopyOnWriteArrayList<>();
         client.getConnectionStateListenable().addListener(this);
         return true;
      } catch (InterruptedException e) {
         client.close();
         throw e;
      }
   }

   @Override
   public synchronized void start() throws InterruptedException, ExecutionException {
      start(-1, null);
   }

   @Override
   public synchronized void stop() {
      checkHandlingEvents();
      final CuratorFramework client = this.client;
      if (client == null) {
         return;
      }
      this.client = null;
      unavailable = false;
      listeners.clear();
      this.listeners = null;
      client.getConnectionStateListenable().removeListener(this);
      locks.forEach((lockId, lock) -> {
         try {
            lock.close(false);
         } catch (Throwable t) {
            // TODO log?
         }
      });
      locks.clear();
      longs.forEach((id, mLong) -> {
         try {
            mLong.close(false);
         } catch (Throwable t) {
            // TODO log?
         }
      });
      longs.clear();
      client.close();
   }

   @Override()
   public synchronized DistributedLock getDistributedLock(String lockId) {
      checkHandlingEvents();
      Objects.requireNonNull(lockId);
      if (client == null) {
         throw new IllegalStateException("manager isn't started yet!");
      }
      final CuratorDistributedLock lock = locks.get(lockId);
      if (lock != null) {
         return lock;
      }
      final Consumer<CuratorDistributedLock> onCloseLock = closedLock -> {
         synchronized (this) {
            final boolean alwaysTrue = locks.remove(closedLock.getLockId(), closedLock);
            assert alwaysTrue;
         }
      };
      final CuratorDistributedLock newLock = new CuratorDistributedLock(this, lockId, new InterProcessSemaphoreV2(client, "/locks/" + lockId, 1), onCloseLock);
      locks.put(lockId, newLock);
      if (unavailable) {
         handlingEvents = true;
         try {
            newLock.onLost();
         } finally {
            handlingEvents = false;
         }
      }
      return newLock;
   }

   @Override
   public MutableLong getMutableLong(String mutableLongId) {
      checkHandlingEvents();
      Objects.requireNonNull(mutableLongId);
      if (client == null) {
         throw new IllegalStateException("manager isn't started yet!");
      }
      final CuratorMutableLong mLong = longs.get(mutableLongId);
      if (mLong != null) {
         return mLong;
      }
      final Consumer<CuratorMutableLong> onClose = closedLong -> {
         synchronized (this) {
            final boolean alwaysTrue = longs.remove(closedLong.getMutableLongId(), closedLong);
            assert alwaysTrue;
         }
      };
      final CuratorMutableLong newLong = new CuratorMutableLong(this, mutableLongId, new DistributedAtomicLong(client, "/" + mutableLongId, new RetryNTimes(0, 0)), onClose);
      longs.put(mutableLongId, newLong);
      if (unavailable) {
         handlingEvents = true;
         try {
            newLong.onLost();
         } finally {
            handlingEvents = false;
         }
      }
      return newLong;
   }

   protected void startHandlingEvents() {
      handlingEvents = true;
   }

   protected void completeHandlingEvents() {
      handlingEvents = false;
   }

   protected void checkHandlingEvents() {
      if (client == null) {
         return;
      }
      if (handlingEvents) {
         throw new IllegalStateException("UnavailableManagerListener isn't supposed to modify the manager or its primitives on event handling!");
      }
   }

   @Override
   public synchronized void stateChanged(CuratorFramework client, ConnectionState newState) {
      if (this.client != client) {
         return;
      }
      if (unavailable) {
         return;
      }
      handlingEvents = true;
      try {
         switch (newState) {
            case LOST:
               unavailable = true;
               listeners.forEach(listener -> listener.onUnavailableManagerEvent());
               locks.forEach((s, curatorDistributedLock) -> curatorDistributedLock.onLost());
               longs.forEach((s, mutableLong) -> mutableLong.onLost());
               break;
            case RECONNECTED:
               locks.forEach((s, curatorDistributedLock) -> curatorDistributedLock.onReconnected());
               longs.forEach((s, mutableLong) -> mutableLong.onReconnected());
               break;
            case SUSPENDED:
               locks.forEach((s, curatorDistributedLock) -> curatorDistributedLock.onSuspended());
               longs.forEach((s, mutableLong) -> mutableLong.onSuspended());
               break;
         }
      } finally {
         handlingEvents = false;
      }
   }
}
