package org.apache.activemq.artemis.quorum.etcd;

import org.junit.Assert;
import org.junit.Test;

public class EtcdUtilsTest {

   @Test
   public void assertNotInterruptedExceptionWithNullShouldPass() throws InterruptedException {
      EtcdUtils.assertNoNestedInterruptedException(null);
      Assert.assertEquals("No error should be thrown", true, true);
   }

   @Test
   public void assertNotInterruptedExceptionWithNPEShouldPass() throws InterruptedException {
      EtcdUtils.assertNoNestedInterruptedException(new NullPointerException());
      Assert.assertEquals("No error should be thrown", true, true);
   }

   @Test(expected = InterruptedException.class)
   public void assertNotInterruptedExceptionWithInterruptedShouldThrowInterrupted() throws InterruptedException {
      EtcdUtils.assertNoNestedInterruptedException(new InterruptedException("DUMMY FOR TESTING PURPOSE"));
   }

   @Test(expected = InterruptedException.class)
   public void assertNotInterruptedExceptionWithWrappedInterruptedShouldThrowInterrupted() throws InterruptedException {
      EtcdUtils.assertNoNestedInterruptedException(
         new RuntimeException(
            new InterruptedException("DUMMY FOR TESTING PURPOSE")));
   }

   @Test
   public void assertNotInterruptedExceptionWithCycleAndNoInterruptedShoudPass() throws InterruptedException {
      final TestException a = new TestException("DUMMY FOR TESTING PURPOSE 1");
      final TestException b = new TestException("DUMMY FOR TESTING PURPOSE 2");
      a.setCause(b);
      b.setCause(a);
      EtcdUtils.assertNoNestedInterruptedException(
         new RuntimeException("DUMMY FOR TESTING PURPOSE", a));
      Assert.assertEquals("Nested exception is supported", true, true);
   }

   private static class TestException extends Throwable {
      private Throwable cause;

      public TestException(String message) {
         super(message);
      }
      @Override
      public Throwable getCause() {
         return cause;
      }
      public void setCause(Throwable cause) {
         this.cause = cause;
      }
   }
}
