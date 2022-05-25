package org.apache.activemq.artemis.quorum.etcd;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class EtcdConfigTest {

   private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

   @After
   public void tearDownEnv() throws Throwable {
      closeables.forEach(closeables -> {
         try {
            closeables.close();
         } catch (Throwable t) {
            // silent here
         }
      });
   }

   private DistributedPrimitiveManager createManagedDistributeManager(final boolean tlsEnabled) {
      try {
         EtcdDistributedPrimitiveManager manager = new EtcdDistributedPrimitiveManager(defaultConfig(tlsEnabled));
         closeables.add(manager);
         return manager;
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static Map<String, String> defaultConfig(final boolean tlsEnabled) {
      return Map.of(
         EtcdDistributedPrimitiveManager.Config.ETCD_ENDPOINT_PARAM, "localhost:2379",
         EtcdDistributedPrimitiveManager.Config.ETCD_LEASE_HEARTBEAT_FHZ_PARAM, "PT4S",
         EtcdDistributedPrimitiveManager.Config.ETCD_REQUEST_DEADLINE_PARAM, "PT10S",
         EtcdDistributedPrimitiveManager.Config.ETCD_LEASE_TTL_PARAM, "PT5S",
         EtcdDistributedPrimitiveManager.Config.ETCD_KEYSPACE_PARAM, "foo",
         EtcdDistributedPrimitiveManager.Config.ETCD_CONNECTION_TIMEOUT_PARAM, "PT2S",
         EtcdDistributedPrimitiveManager.Config.ETCD_TLS_ENABLED_PARAM, tlsEnabled?"true":"false"
      );
   }

   @Test
   public void allParamsOkShouldPass() {
      final Map<String, String> config = defaultConfig(false);
      EtcdDistributedPrimitiveManager.Config.assertParametersAreValid(config);
      EtcdDistributedPrimitiveManager.Config theConfig = new EtcdDistributedPrimitiveManager.Config(config);
      Assert.assertEquals("localhost:2379", theConfig.getConnectString());
      Assert.assertEquals(4, theConfig.getLeaseHeartbeatFhz());
      Assert.assertEquals(Duration.ofSeconds(10), theConfig.getRequestDeadline());
      Assert.assertEquals(5, theConfig.getLeaseTtl());
      Assert.assertEquals("foo", theConfig.getKeyspace());
      Assert.assertEquals(Duration.ofSeconds(2), theConfig.getConnectionTimeout());
      Assert.assertEquals(false, theConfig.isTlsEnabled());
   }

   /* SUPPORT OF TLS TO BE IMPLEMENTED
   @Test
   public void connectInTls() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager1 = createManagedDistributeManager(true);
      manager1.start();
      Assert.assertTrue(manager1.getDistributedLock("a").tryLock());
   }

    */

}
