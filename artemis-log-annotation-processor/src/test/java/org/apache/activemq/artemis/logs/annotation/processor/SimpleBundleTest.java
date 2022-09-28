/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.logs.annotation.processor;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;

public class SimpleBundleTest {

   @Test
   public void testSimple() {
      Assert.assertEquals("TST1: Test", SimpleBundle.MESSAGES.simpleTest());
      System.out.println(SimpleBundle.MESSAGES.simpleTest());
   }

   @Test
   public void testParameters() {
      Assert.assertEquals("TST2: V1-bb", SimpleBundle.MESSAGES.parameters(1, "bb"));
   }

   @Test
   public void testException() {
      Exception ex = SimpleBundle.MESSAGES.someException();
      Assert.assertEquals("TST3: EX", ex.getMessage());
      StringWriter stringWriter = new StringWriter();
      PrintWriter writer = new PrintWriter(stringWriter);
      ex.printStackTrace(writer);
      Assert.assertEquals("The method name (someException) should not be part of the stack trace", -1, stringWriter.toString().lastIndexOf("someException"));
   }

   @Test
   public void testSomeExceptionParameter() {
      String uuid = UUID.randomUUID().toString();
      Assert.assertEquals(new Exception("TST4: EX-" + uuid).toString(), SimpleBundle.MESSAGES.someExceptionParameter(uuid).toString());
   }

   @Test
   public void testPrint() {
      SimpleBundle.MESSAGES.printMessage();
      for (int i = 0; i < 10; i++) {
         SimpleBundle.MESSAGES.printMessage(i);
      }
   }

   @Test
   public void testMultiLines() {
      SimpleBundle.MESSAGES.multiLines();
   }


   @Test
   public void testWithException() {
      Exception myCause = new Exception("this is myCause");
      String logRandomString = "" + System.currentTimeMillis();
      MyException myException = SimpleBundle.MESSAGES.someExceptionWithCause(logRandomString, myCause);
      Assert.assertEquals("TST8: EX" + logRandomString, myException.getMessage());
      Assert.assertSame(myCause, myException.getCause());
   }

   @Test
   public void testABCD() {
      System.out.println(SimpleBundle.MESSAGES.abcd("A", "B", "C", "D"));
      Assert.assertEquals("TST9: A B C D", SimpleBundle.MESSAGES.abcd("A", "B", "C", "D"));
   }

   @Test
   public void testObjectsABCD() {
      System.out.println(SimpleBundle.MESSAGES.abcd("A", "B", "C", "D"));
      Assert.assertEquals("TST10: A B C D", SimpleBundle.MESSAGES.objectsAbcd(new MyObject("A"), new MyObject("B"), new MyObject("C"), new MyObject("D")));
   }


   @Test
   public void exceptions() {
      SimpleBundle.MESSAGES.parameterException("hello", new IOException("this is an exception"));
      SimpleBundle.MESSAGES.myExceptionLogger("hello2", new MyException("this is an exception"));
   }

   @Test
   public void longList() {
      AssertionLoggerHandler.startCapture(false, true);
      try {
         SimpleBundle.MESSAGES.longParameters("1", "2", "3", "4", "5");
         Assert.assertTrue("parameter not found", AssertionLoggerHandler.findText("p1"));
         Assert.assertTrue("parameter not found", AssertionLoggerHandler.findText("p2"));
         Assert.assertTrue("parameter not found", AssertionLoggerHandler.findText("p3"));
         Assert.assertTrue("parameter not found", AssertionLoggerHandler.findText("p4"));
         Assert.assertTrue("parameter not found", AssertionLoggerHandler.findText("p5"));
         Assert.assertFalse("{}", AssertionLoggerHandler.findText("{}"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }


   @Test
   public void onlyException() {
      try {
         AssertionLoggerHandler.startCapture(false, false);

         SimpleBundle.MESSAGES.onlyException(createMyExceptionBreadcrumbMethod("MSG7777"));

         Assert.assertTrue(AssertionLoggerHandler.findText("TST14"));
         Assert.assertFalse(AssertionLoggerHandler.findText("MSG7777"));

         AssertionLoggerHandler.clear();

         AssertionLoggerHandler.startCapture(false, true);
         SimpleBundle.MESSAGES.onlyException(createMyExceptionBreadcrumbMethod("MSG7777"));
         Assert.assertTrue(AssertionLoggerHandler.findText("TST14"));
         Assert.assertTrue(AssertionLoggerHandler.findText("MSG7777"));
         Assert.assertTrue(AssertionLoggerHandler.findText("createMyExceptionBreadcrumbMethod"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }


   // I'm doing it on a method just to assert if this method will appear on the stack trace
   private MyException createMyExceptionBreadcrumbMethod(String message) {
      return new MyException(message);
   }


   @Test
   public void testGetLogger() {
      Assert.assertNotNull(SimpleBundle.MESSAGES.getLogger());
   }
}
