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

import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.kv.KvClient;
import io.grpc.Deadline;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.jboss.logging.Logger;

final class EtcdMutableLong extends EtcdDistributedPrimitive implements MutableLong {

   private static final Logger logger = Logger.getLogger(EtcdMutableLong.class);

   private static final String ETCD_COUNTER_KEYSPACE = "counter";

   private final KvClient etcdKVClient;
   private final ByteString counterPath;
   private final EtcdDistributedPrimitiveManager.Config config;


   EtcdMutableLong(EtcdDistributedPrimitiveManager.PrimitiveId id, EtcdDistributedPrimitiveManager manager, EtcdDistributedPrimitiveManager.Config config) {
      super(id, manager);
      this.etcdKVClient = manager.getEtcd().getKvClient();
      this.counterPath = config.buildKeyPath(ETCD_COUNTER_KEYSPACE, id.id);
      this.config = config;
   }

   @Override
   public String getMutableLongId() {
      return getId().id;
   }

   @Override
   public long get() throws UnavailableStateException {
      return run(() -> {
         checkUnavailable();
         try {
            logger.infov("Get atomic long: member=" + getId().id + ", key=" + counterPath);
            //AtomicValue<Long> atomicValue = atomicLong.get();
            final RangeResponse counterResponse = this.etcdKVClient
               .get(this.counterPath)
               .deadline(Deadline.after(this.config.getRequestDeadline().toMillis(), TimeUnit.MILLISECONDS))
               .sync();
            if (counterResponse.getCount() == 0) {
               return 0L; // defaulting to 0
            }
            return Long.parseLong(counterResponse.getKvs(0).getValue().toStringUtf8());
         } catch (Throwable e) {
            EtcdUtils.logGrpcError(logger, e, this.getId().id, "getting atomic long");
            throw new UnavailableStateException(e);
         }
      });
   }

   @Override
   public void set(long value) throws UnavailableStateException {
      run(() -> {
         checkUnavailable();
         try {
            logger.infov("Store atomic long: member=" + getId().id + ", key=" + counterPath + ", val=" + value);
            this.etcdKVClient
               .put(this.counterPath, KeyUtils.bs(String.valueOf(value)))
               .deadline(Deadline.after(this.config.getRequestDeadline().toMillis(), TimeUnit.MILLISECONDS))
               .sync();
            return null;
         }
         catch (Throwable e) {
            EtcdUtils.logGrpcError(logger, e, this.getId().id, "storing atomic long");
            throw new UnavailableStateException(e);
         }
      });
   }
}
