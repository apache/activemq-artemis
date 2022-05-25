package org.apache.activemq.artemis.quorum.etcd;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.grpc.Status;
import org.jboss.logging.Logger;

public class EtcdUtils {

   public static void logGrpcError(Logger logger, final Throwable throwable, String memberId, String message) {
      logGrpcError(logger, throwable, memberId, message, Status.fromThrowable(throwable));
   }

   public static void logGrpcError(Logger logger, final Throwable throwable, String memberId, String message, Status status) {
      switch (status.getCode()) {
         case DEADLINE_EXCEEDED:
            logger.warnf("[%s] Etcd Request deadline exceeded, while %s", memberId, message);
            break;
         case CANCELLED:
            logger.warnf(throwable, "[%s] Etcd Request cancelled, while %s", memberId, message);
            break;
         default:
            if (status.getCode() == Status.Code.UNKNOWN) {
               logger.warnf(throwable,"[%s] Etcd Request failure %s while %s", memberId, status.getCode().name(), message);
            }
            else {
               logger.warnf("[%s] Etcd Request failure %s while %s", memberId, status.getCode().name(), message);
            }
      }
   }

   public static void assertNoNestedInterruptedException(final Throwable throwable) throws InterruptedException {
      final Optional<Throwable> interruptedException = assertNoNestedInterruptedException(throwable, Collections.emptySet())
         .stream()
         .filter(t -> t instanceof InterruptedException)
         .findFirst();
      if (interruptedException.isPresent()) {
         throw InterruptedException.class.cast(interruptedException.get());
      }
   }

   private static Set<Throwable> assertNoNestedInterruptedException(final Throwable throwable, final Set<Throwable> alreadyLookedThrough) {
      if (alreadyLookedThrough.contains(throwable) || throwable == null) {
         return alreadyLookedThrough;
      }
      else {
         return assertNoNestedInterruptedException(
            throwable.getCause(), Stream.concat(
                  alreadyLookedThrough.stream(),
                  Stream.of(throwable)
               ).collect(Collectors.toSet())
         );
      }
   }

   public static String uuidToB64(final UUID uuid) {
      final ByteBuffer bb = ByteBuffer.allocate(Long.BYTES * 2);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      byte[] array = bb.array();
      return Base64.getEncoder().encodeToString(array).replace("=", "");
   }

}
