package org.apache.activemq.artemis.quorum.etcd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import org.junit.rules.TemporaryFolder;

public class EtcdProcessHelper {

   public static List<Integer> reservePorts(int count) {
      assert count > 0;
      return IntStream.range(0, count)
         .mapToObj(i -> reservePort())
         .collect(Collectors.toList());
   }

   private static Integer reservePort() {
      try (final ServerSocket socket = new ServerSocket(0)) {
         return socket.getLocalPort();
      } catch (IOException e) {
         throw new IllegalStateException("unable to reserve a port!?", e);
      }
   }

   public static Path createEtcdTempFolder(final TemporaryFolder tmpFolder, final String name) throws IOException {
      Path dir = Files.createDirectory(tmpFolder.getRoot().toPath().resolve(name));
      Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
      return dir;
   }

   public static Process startProcess(String... cmdline) throws Exception {
      boolean ok = false;
      try {
         List<String> cmd = new ArrayList<>();
         cmd.add(EtcdTestSuite.etcdCommand);
         cmd.addAll(Arrays.asList(cmdline));
         System.out.println(cmd.stream().collect(Collectors.joining(" ")));
         Process etcdProcess = new ProcessBuilder(cmd).redirectErrorStream(true).start();
         waitForStartup(etcdProcess);
         if (!etcdProcess.isAlive()) {
            return null;
         }
         ok = true;
         return etcdProcess;
      } catch (IOException e) {
         System.out.println("Failed to start etcd: " + e);
         return null;
      } finally {
         if (!ok) {
            tearDown(EtcdTestSuite.etcdProcess);
         }
      }
   }

   public static void tearDown(Process process) {
      if (process != null) {
         final CountDownLatch countDown = new CountDownLatch(1);
         process.onExit().thenAccept(p -> countDown.countDown());
         process.destroy();
         try {
            countDown.await(10, TimeUnit.SECONDS);
         }
         catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }
   }

   public static void waitForStartup(Process process) throws Exception {
      if (process == null) {
         return;
      }
      ExecutorService es = Executors.newSingleThreadExecutor();
      TimeLimiter tl = SimpleTimeLimiter.create(es);
      try {
         tl.callWithTimeout(() -> {
            Reader isr = new InputStreamReader(process.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null && !line.contains("ready to serve client requests") && !line.contains("serving peer traffic")) {
               System.out.println(line);
            }
            return null;
         }, 10L, TimeUnit.SECONDS);
      } finally {
         es.shutdown();
      }
   }

   public static void cleanup(TemporaryFolder tmpFolder) {
      tmpFolder.delete();
   }

}
