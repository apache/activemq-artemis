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
package org.apache.activemq.artemis.quorum.etcd;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;
import io.grpc.stub.StreamObserver;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.jboss.logging.Logger;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * https://github.com/etcd-io/etcd/blob/main/Documentation/dev-guide/apispec/swagger/rpc.swagger.json
 *
 * Structure in Etcd:
 * /activemq/$artemis-broker-instance/members/$id
 * /activemq/$artemis-broker-instance/leader
 *
 * 1. Create a lease. The lease is a kind of unique open channel between the etcd client and etcd
 * 2. Register the member unique ID (/activemq/$artemis-broker-instance/members/$id)
 * 3. Try to acquire the leader role using compareAndSet(
 *      key = /activemq/$artemis-broker-instance/leader,
 *      old = null,
 *      new = unique ID)
 *    If succeeded, current node is the leader
 *    Else current node is a slave
 * 4. Watch for changes on the leader key: /activemq/$artemis-broker-instance/members/$id
 *
 *
 * etcd/lease $ttl.seconds
 *
 * Etcd implementation relies on:
 * - /lease/grant', {'ID': String.valueOf(int64), 'TTL': int64}: create a lease which expires if the
 *   keepalive is not received within the specified TTL
 * - /lease/keepalive, {'ID': String.valueOf(int64)}
 * - /watch, {'create_request':{'filters':{'key': clusterPrefix, 'range_end':}}}
 * - /kv/range,
 * - /kv/txn, {'compare': [compare], 'success': [{'request_delete_range': request}]}
 */
public class EtcdDistributedPrimitiveManager implements DistributedPrimitiveManager /*, EtcdLeaderElection.ElectionListener*/ {

   private static final Logger logger = Logger.getLogger(EtcdDistributedPrimitiveManager.class);
   private PersistentLease persistentLease;

   enum PrimitiveType {
      lock, mutableLong;

      static <T extends EtcdDistributedPrimitive> T validatePrimitiveInstance(T primitive) {
         if (primitive == null) {
            return null;
         }
         boolean valid = false;
         switch (primitive.getId().type) {
            case lock:
               valid = primitive instanceof EtcdDistributedLock;
               break;
            case mutableLong:
               valid = primitive instanceof EtcdMutableLong;
               break;
         }
         if (!valid) {
            throw new AssertionError("Implementation error: " + primitive.getClass() + " is wrongly considered " + primitive.getId().type);
         }
         return primitive;
      }
   }

   static final class PrimitiveId {

      final String id;
      final PrimitiveType type;

      private PrimitiveId(String id, PrimitiveType type) {
         this.id = requireNonNull(id);
         this.type = requireNonNull(type);
      }

      static PrimitiveId of(String id, PrimitiveType type) {
         return new PrimitiveId(id, type);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (o == null || getClass() != o.getClass())
            return false;

         PrimitiveId that = (PrimitiveId) o;

         if (!Objects.equals(id, that.id))
            return false;
         return type == that.type;
      }

      @Override
      public int hashCode() {
         int result = id != null ? id.hashCode() : 0;
         result = 31 * result + (type != null ? type.hashCode() : 0);
         return result;
      }
   }

   static final class Config {
      protected static final String ETCD_ENDPOINT_PARAM = "etcd-endpoints";
      protected static final String ETCD_KEYSPACE_PARAM = "etcd-keyspace";
      protected static final String ETCD_LEASE_TTL_PARAM = "etcd-lease-ttl";
      protected static final String ETCD_CONNECTION_TIMEOUT_PARAM = "etcd-connection-timeout";
      protected static final String ETCD_REQUEST_DEADLINE_PARAM = "etcd-deadline";
      protected static final String ETCD_TLS_ENABLED_PARAM = "etcd-tls-enabled";
      protected static final String ETCD_LEASE_HEARTBEAT_FHZ_PARAM = "etcd-lease-heartbeat-fhz";


      private static final String ETCD_KEYSPACE_DEFAULT = "artemis";
      private static final String ETCD_LEASE_TTL_DEFAULT = "PT30S"; // 30 seconds
      private static final String ETCD_CONNECTION_TIMEOUT_DEFAULT = "PT10S";
      private static final String ETCD_REQUEST_DEADLINE_DEFAULT = "PT20S";
      private static final String ETCD_TLS_ENABLED_DEFAULT = "false";
      private static final String ETCD_LEASE_HEARTBEAT_FHZ_DEFAULT = "PT4S"; // 4 seconds

      private static final Set<String> VALID_PARAMS = Set.of(ETCD_ENDPOINT_PARAM,
                                       ETCD_KEYSPACE_PARAM,
                                       ETCD_LEASE_TTL_PARAM,
                                       ETCD_CONNECTION_TIMEOUT_PARAM,
                                       ETCD_REQUEST_DEADLINE_PARAM,
                                       ETCD_TLS_ENABLED_PARAM,
                                       ETCD_LEASE_HEARTBEAT_FHZ_PARAM);
      private static final String VALID_PARAMS_ON_ERROR = VALID_PARAMS.stream().collect(joining(","));

      static void assertParametersAreValid(final Map<String, String> config) {
         config.forEach((parameterName, ignore) -> asseertParameterIsValid(parameterName));
      }

      static void asseertParameterIsValid(final String parameterName) {
         if (!VALID_PARAMS.contains(parameterName)) {
            throw new IllegalArgumentException("non existent parameter " + parameterName + ": accepted list is " + VALID_PARAMS_ON_ERROR);
         }
      }

      private final String connectString;
      private final String keyspace;
      private final int leaseTtl;
      private final int leaseHeartbeatFhz;
      private final Duration connectionTimeout;
      private final Duration requestDeadline;
      private final boolean isTlsEnabled;

      private EtcdClient.Builder builder;

      public Config(final Map<String, String> config) {
         assertParametersAreValid(config);
         this.connectString = config.get(ETCD_ENDPOINT_PARAM);
         this.keyspace = config.getOrDefault(ETCD_KEYSPACE_PARAM, ETCD_KEYSPACE_DEFAULT);
         this.leaseTtl = Long.valueOf(Duration.parse(config.getOrDefault(ETCD_LEASE_TTL_PARAM, ETCD_LEASE_TTL_DEFAULT)).toSeconds()).intValue();
         this.connectionTimeout = Duration.parse(config.getOrDefault(ETCD_CONNECTION_TIMEOUT_PARAM, ETCD_CONNECTION_TIMEOUT_DEFAULT));
         this.requestDeadline = Duration.parse(config.getOrDefault(ETCD_REQUEST_DEADLINE_PARAM, ETCD_REQUEST_DEADLINE_DEFAULT));
         this.isTlsEnabled = Boolean.parseBoolean(config.getOrDefault(ETCD_TLS_ENABLED_PARAM, ETCD_TLS_ENABLED_DEFAULT));
         this.leaseHeartbeatFhz = Long.valueOf(Duration.parse(config.getOrDefault(ETCD_LEASE_HEARTBEAT_FHZ_PARAM, ETCD_LEASE_HEARTBEAT_FHZ_DEFAULT)).toSeconds()).intValue();

         final EtcdClient.Builder builder = EtcdClient
            .forEndpoints(this.connectString)
            .withDefaultTimeout(this.connectionTimeout.toMillis(), TimeUnit.MILLISECONDS)
            .withSessionTimeoutSecs(this.leaseTtl);

         if (this.isTlsEnabled) {
            logger.error("TLS Not supported at the moment");
            throw new IllegalArgumentException("TLS Not supported at the moment");
         }
         else {
            builder.withPlainText();
         }
         this.builder = builder;
      }

      public ByteString buildKeyPath(final String... suffixes) {
         return KeyUtils.bs(this.keyspace + "/" +
                               Stream.of(suffixes)
                                  .collect(joining("/")));
      }

      public int getLeaseTtl() {
         return this.leaseTtl;
      }

      public int getLeaseHeartbeatFhz() {
         return this.leaseHeartbeatFhz;
      }

      public Duration getConnectionTimeout() {
         return this.connectionTimeout;
      }

      public String getKeyspace() {
         return keyspace;
      }

      public String getConnectString() {
         return connectString;
      }

      public boolean isTlsEnabled() {
         return isTlsEnabled;
      }

      public EtcdClient newClient() {
         return this.builder
            .build();
      }

      public Duration getRequestDeadline() {
         return this.requestDeadline;
      }
   }

   private Config config;

   private EtcdClient client;

   private final Map<PrimitiveId, EtcdDistributedPrimitive> primitives = new ConcurrentHashMap<>();
   private CopyOnWriteArrayList<UnavailableManagerListener> listeners;

   private volatile boolean unavailable;
   //private boolean handlingEvents;

   protected final String managerId = EtcdUtils.uuidToB64(UUID.randomUUID());

   private final Object lock = new Object();

   // need to enqueue operations to ensure they don't run in //
   private final ExecutorService fifoThreadsQueue = Executors.newFixedThreadPool(1);

   public EtcdDistributedPrimitiveManager(Map<String, String> config) {
      this.config = new Config(config);
      this.listeners = null;
      this.unavailable = false;
      //this.handlingEvents = false;
   }

   @Override
   public boolean isStarted() {
      return client != null;
   }

   @Override
   public void addUnavailableManagerListener(UnavailableManagerListener listener) {
      if (listeners == null) {
         return;
      }
      listeners.add(listener);
      if (unavailable) {
         synchronized (this.lock) {
            listener.onUnavailableManagerEvent();
         }
      }
   }

   @Override
   public void removeUnavailableManagerListener(UnavailableManagerListener listener) {
      if (listeners == null) {
         return;
      }
      synchronized (this.lock) {
         listeners.remove(listener);
      }
   }

   @Override
   public boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      if (timeout >= 0) {
         if (timeout > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("curator manager won't support too long timeout ie >" + Integer.MAX_VALUE);
         }
         requireNonNull(unit);
      }
      if (client != null) {
         return true;
      }

      synchronized (this.lock) {
         try {
            logger.infof("[%s] start: Start the ETCD quorum manager", managerId);
            final EtcdClient client = this.config.newClient();
            final LeaseClient etcdLeaseClient = client.getLeaseClient();
            // acquire the lease:
            final PersistentLease lease = etcdLeaseClient.maintain().minTtl(this.config.getLeaseTtl()).keepAliveFreq(this.config.getLeaseHeartbeatFhz()).start();

            final Long leaseId = lease.get(this.config.requestDeadline.toMillis(), TimeUnit.MILLISECONDS);
            logger.infof("[%s] start: Lease acquired: %s", managerId, leaseId);

            this.client = client;
            this.persistentLease = lease;
            this.listeners = new CopyOnWriteArrayList<>();
            lease.addStateObserver(new LeaseObserver(this), false);
            return leaseId != 0;
         }
         catch (RuntimeException | TimeoutException e) {
            EtcdUtils.logGrpcError(logger, e, "not_defined", managerId + " acquiring initial lease (" + e.getMessage() + ")");
            EtcdUtils.assertNoNestedInterruptedException(e);
            return false; //
         }
      }
   }

   PersistentLease getLease() {
      return this.persistentLease;
   }

   @Override
   public void start() throws InterruptedException, ExecutionException {
      start(-1, null);
   }

   @Override
   public void stop() {
      final EtcdClient client = this.client;
      if (client == null) {
         return;
      }
      synchronized (this.lock) {
         this.client = null;
         unavailable = false;
         listeners.clear();
         this.listeners = null;
         primitives.forEach((id, primitive) -> {
            try {
               primitive.onRemoved();
            } catch (Throwable t) {
               // TODO log?
            }
         });
         primitives.clear();
         client.close();
      }
   }

   private <T extends EtcdDistributedPrimitive> T getPrimitive(
                     PrimitiveId id,
                     Function<PrimitiveId, ? extends T> primitiveFactory) {
      requireNonNull(id);
      if (client == null) {
         throw new IllegalStateException("manager isn't started yet!");
      }
      return (T) this.primitives.compute(id, (key, current) -> {
         final EtcdDistributedPrimitive primitive = PrimitiveType.validatePrimitiveInstance(current);
         if (primitive != null) {
            return primitive;
         }
         final T newPrimitive = PrimitiveType.validatePrimitiveInstance(primitiveFactory.apply(id));
         if (unavailable) {
            logger.infof("[%s] getPrimitive: unavailable - trigger onLost()", managerId);
            newPrimitive.onLost();
         }
         return newPrimitive;
      });
   }

   @Override
   public DistributedLock getDistributedLock(String lockId) {
      return getPrimitive(PrimitiveId.of(lockId, PrimitiveType.lock),
                          id -> new EtcdDistributedLock(id, this, this.config));

   }

   @Override
   public MutableLong getMutableLong(String mutableLongId) {
      return getPrimitive(PrimitiveId.of(mutableLongId, PrimitiveType.mutableLong),
                          id -> new EtcdMutableLong(id, this, this.config));
   }

   @Override
   public void close() {
      DistributedPrimitiveManager.super.close();
   }

   private String getClientInfo() {
      if (this.client == null) { return null; }
      return this.client.toString();
   }

   /**
    * Used for testing purposes
    */
   public EtcdClient getEtcd() {
      return client;
   }

   public void remove(EtcdDistributedPrimitive primitive) {
      primitives.remove(primitive.getId());
   }


   ///////////////////// Etcd State Management Listener /////////////////////

   private static class LeaseObserver implements StreamObserver<PersistentLease.LeaseState> {

      private final EtcdDistributedPrimitiveManager manager;

      public LeaseObserver(EtcdDistributedPrimitiveManager manager) {
         this.manager = manager;
      }

      @Override
      public void onNext(PersistentLease.LeaseState leaseState) {
         logger.infof( "[%s] onNext: Lease state changed: %s, unavailable: %s", manager.managerId, leaseState.name(), manager.unavailable);
         https://github.com/IBM/etcd-java/blob/main/src/main/java/com/ibm/etcd/client/lease/PersistentLease.java
         synchronized (this.manager) {
            switch (leaseState) {
               case PENDING:
                  break;
               case ACTIVE:
                  manager.primitives.forEach((id, primitive) -> primitive.onReconnected());
                  break;
               case ACTIVE_NO_CONN:
                  logger.warnf("[%s] Connection to the ETCD cluster lost. Lease is still active.", this.manager.managerId);
                  break;
               case EXPIRED:
               case CLOSED:
                  manager.unavailable = true;
                  notifyListenersUnavailable();
                  manager.primitives.forEach((id, primitive) -> primitive.onLost());
                  break;
            }
         }
      }

      private void notifyListenersUnavailable() {
         CopyOnWriteArrayList<UnavailableManagerListener> managerListeners;
         if ((managerListeners = manager.listeners) != null) {
            managerListeners.forEach(listener -> listener.onUnavailableManagerEvent());
         }
      }

      @Override
      public void onError(Throwable throwable) {
         logger.errorf(throwable, "[%s] gRPC Lease state stream observer failed.", this.manager.managerId);
         manager.unavailable = true;
         notifyListenersUnavailable();
         manager.primitives.forEach((id, primitive) -> primitive.onLost());
      }

      @Override
      public void onCompleted() {
         logger.warnf("[%s] gRPC Lease state stream observer completed", this.manager.managerId);
         manager.unavailable = true;
         notifyListenersUnavailable();
         manager.primitives.forEach((id, primitive) -> primitive.onLost());
      }

   }

}
