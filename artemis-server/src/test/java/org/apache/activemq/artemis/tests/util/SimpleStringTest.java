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
package org.apache.activemq.artemis.tests.util;

import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class SimpleStringTest extends Assert {

   /**
    * Converting back and forth between char and byte requires care as char is unsigned.
    *
    * @see SimpleString#getChars(int, int, char[], int)
    * @see SimpleString#charAt(int)
    * @see SimpleString#split(char)
    * @see SimpleString#concat(char)
    */
   @Test
   public void testGetChar() {
      SimpleString p1 = SimpleString.toSimpleString("foo");
      SimpleString p2 = SimpleString.toSimpleString("bar");
      for (int i = 0; i < 1 << 16; i++) {
         String msg = "expecting " + i;
         char c = (char) i;
         SimpleString s = SimpleString.toSimpleString(String.valueOf(c));

         // test getChars(...)
         char[] c1 = new char[1];
         s.getChars(0, 1, c1, 0);
         assertEquals(msg, c, c1[0]);

         // test charAt(int)
         assertEquals(msg, c, s.charAt(0));

         // test concat(char)
         SimpleString s2 = s.concat(c);
         assertEquals(msg, c, s2.charAt(1));

         // test splitting with chars
         SimpleString sSplit = SimpleString.toSimpleString("foo" + String.valueOf(c) + "bar");
         SimpleString[] chunks = sSplit.split(c);
         SimpleString[] split1 = p1.split(c);
         SimpleString[] split2 = p2.split(c);
         assertEquals(split1.length + split2.length, chunks.length);

         int j = 0;
         for (SimpleString iS : split1) {
            assertEquals(iS.toString(), iS, chunks[j++]);
         }
         for (SimpleString iS : split2) {
            assertEquals(iS.toString(), iS, chunks[j++]);
         }
      }
   }

   @Test
   public void testString() throws Exception {
      final String str = "hello123ABC__524`16254`6125!%^$!%$!%$!%$!%!$%!$$!\uA324";

      SimpleString s = SimpleString.toSimpleString(str);

      Assert.assertEquals(str, s.toString());

      Assert.assertEquals(2 * str.length(), s.getData().length);

      byte[] data = s.getData();

      SimpleString s2 = SimpleString.toSimpleString(data);

      Assert.assertEquals(str, s2.toString());
   }

   @Test
   public void testStartsWith() throws Exception {
      SimpleString s1 = SimpleString.toSimpleString("abcdefghi");

      Assert.assertTrue(s1.startsWith(SimpleString.toSimpleString("abc")));

      Assert.assertTrue(s1.startsWith(SimpleString.toSimpleString("abcdef")));

      Assert.assertTrue(s1.startsWith(SimpleString.toSimpleString("abcdefghi")));

      Assert.assertFalse(s1.startsWith(SimpleString.toSimpleString("abcdefghijklmn")));

      Assert.assertFalse(s1.startsWith(SimpleString.toSimpleString("aardvark")));

      Assert.assertFalse(s1.startsWith(SimpleString.toSimpleString("z")));
   }

   @Test
   public void testCharSequence() throws Exception {
      String s = "abcdefghijkl";
      SimpleString s1 = SimpleString.toSimpleString(s);

      Assert.assertEquals('a', s1.charAt(0));
      Assert.assertEquals('b', s1.charAt(1));
      Assert.assertEquals('c', s1.charAt(2));
      Assert.assertEquals('k', s1.charAt(10));
      Assert.assertEquals('l', s1.charAt(11));

      try {
         s1.charAt(-1);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      try {
         s1.charAt(-2);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      try {
         s1.charAt(s.length());
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      try {
         s1.charAt(s.length() + 1);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      Assert.assertEquals(s.length(), s1.length());

      CharSequence ss = s1.subSequence(0, s1.length());

      Assert.assertEquals(ss, s1);

      ss = s1.subSequence(1, 4);
      Assert.assertEquals(ss, SimpleString.toSimpleString("bcd"));

      ss = s1.subSequence(5, 10);
      Assert.assertEquals(ss, SimpleString.toSimpleString("fghij"));

      ss = s1.subSequence(5, 12);
      Assert.assertEquals(ss, SimpleString.toSimpleString("fghijkl"));

      try {
         s1.subSequence(-1, 2);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      try {
         s1.subSequence(-4, -2);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      try {
         s1.subSequence(0, s1.length() + 1);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      try {
         s1.subSequence(0, s1.length() + 2);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }

      try {
         s1.subSequence(5, 1);
         Assert.fail("Should throw exception");
      } catch (IndexOutOfBoundsException e) {
         // OK
      }
   }

   @Test
   public void testEquals() throws Exception {
      Assert.assertFalse(SimpleString.toSimpleString("abcdef").equals(new Object()));

      Assert.assertFalse(SimpleString.toSimpleString("abcef").equals(null));

      Assert.assertEquals(SimpleString.toSimpleString("abcdef"), SimpleString.toSimpleString("abcdef"));

      Assert.assertFalse(SimpleString.toSimpleString("abcdef").equals(SimpleString.toSimpleString("abggcdef")));
      Assert.assertFalse(SimpleString.toSimpleString("abcdef").equals(SimpleString.toSimpleString("ghijkl")));
   }

   @Test
   public void testHashcode() throws Exception {
      SimpleString str = SimpleString.toSimpleString("abcdef");
      SimpleString sameStr = SimpleString.toSimpleString("abcdef");
      SimpleString differentStr = SimpleString.toSimpleString("ghijk");

      Assert.assertTrue(str.hashCode() == sameStr.hashCode());
      Assert.assertFalse(str.hashCode() == differentStr.hashCode());
   }

   @Test
   public void testUnicode() throws Exception {
      String myString = "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";

      SimpleString s = SimpleString.toSimpleString(myString);
      byte[] data = s.getData();
      s = SimpleString.toSimpleString(data);

      Assert.assertEquals(myString, s.toString());
   }

   @Test
   public void testUnicodeWithSurrogates() throws Exception {
      String myString = "abcdef&^*&!^ghijkl\uD900\uDD00";

      SimpleString s = SimpleString.toSimpleString(myString);
      byte[] data = s.getData();
      s = SimpleString.toSimpleString(data);

      Assert.assertEquals(myString, s.toString());
   }

   @Test
   public void testSizeofString() throws Exception {
      Assert.assertEquals(DataConstants.SIZE_INT, SimpleString.sizeofString(SimpleString.toSimpleString("")));

      SimpleString str = SimpleString.toSimpleString(RandomUtil.randomString());
      Assert.assertEquals(DataConstants.SIZE_INT + str.getData().length, SimpleString.sizeofString(str));
   }

   @Test
   public void testSizeofNullableString() throws Exception {
      Assert.assertEquals(1, SimpleString.sizeofNullableString(null));

      Assert.assertEquals(1 + DataConstants.SIZE_INT, SimpleString.sizeofNullableString(SimpleString.toSimpleString("")));

      SimpleString str = SimpleString.toSimpleString(RandomUtil.randomString());
      Assert.assertEquals(1 + DataConstants.SIZE_INT + str.getData().length, SimpleString.sizeofNullableString(str));
   }

   @Test
   public void testSplitNoDelimeter() throws Exception {
      SimpleString s = SimpleString.toSimpleString("abcdefghi");
      SimpleString[] strings = s.split('.');
      Assert.assertNotNull(strings);
      Assert.assertEquals(strings.length, 1);
      Assert.assertEquals(strings[0], s);
   }

   @Test
   public void testSplit1Delimeter() throws Exception {
      SimpleString s = SimpleString.toSimpleString("abcd.efghi");
      SimpleString[] strings = s.split('.');
      Assert.assertNotNull(strings);
      Assert.assertEquals(strings.length, 2);
      Assert.assertEquals(strings[0], SimpleString.toSimpleString("abcd"));
      Assert.assertEquals(strings[1], SimpleString.toSimpleString("efghi"));
   }

   @Test
   public void testSplitmanyDelimeters() throws Exception {
      SimpleString s = SimpleString.toSimpleString("abcd.efghi.jklmn.opqrs.tuvw.xyz");
      SimpleString[] strings = s.split('.');
      Assert.assertNotNull(strings);
      Assert.assertEquals(strings.length, 6);
      Assert.assertEquals(strings[0], SimpleString.toSimpleString("abcd"));
      Assert.assertEquals(strings[1], SimpleString.toSimpleString("efghi"));
      Assert.assertEquals(strings[2], SimpleString.toSimpleString("jklmn"));
      Assert.assertEquals(strings[3], SimpleString.toSimpleString("opqrs"));
      Assert.assertEquals(strings[4], SimpleString.toSimpleString("tuvw"));
      Assert.assertEquals(strings[5], SimpleString.toSimpleString("xyz"));
   }

   @Test
   public void testContains() {
      SimpleString simpleString = SimpleString.toSimpleString("abcdefghijklmnopqrst");
      Assert.assertFalse(simpleString.contains('.'));
      Assert.assertFalse(simpleString.contains('%'));
      Assert.assertFalse(simpleString.contains('8'));
      Assert.assertFalse(simpleString.contains('.'));
      Assert.assertTrue(simpleString.contains('a'));
      Assert.assertTrue(simpleString.contains('b'));
      Assert.assertTrue(simpleString.contains('c'));
      Assert.assertTrue(simpleString.contains('d'));
      Assert.assertTrue(simpleString.contains('e'));
      Assert.assertTrue(simpleString.contains('f'));
      Assert.assertTrue(simpleString.contains('g'));
      Assert.assertTrue(simpleString.contains('h'));
      Assert.assertTrue(simpleString.contains('i'));
      Assert.assertTrue(simpleString.contains('j'));
      Assert.assertTrue(simpleString.contains('k'));
      Assert.assertTrue(simpleString.contains('l'));
      Assert.assertTrue(simpleString.contains('m'));
      Assert.assertTrue(simpleString.contains('n'));
      Assert.assertTrue(simpleString.contains('o'));
      Assert.assertTrue(simpleString.contains('p'));
      Assert.assertTrue(simpleString.contains('q'));
      Assert.assertTrue(simpleString.contains('r'));
      Assert.assertTrue(simpleString.contains('s'));
      Assert.assertTrue(simpleString.contains('t'));
   }

   @Test
   public void testConcat() {
      SimpleString start = SimpleString.toSimpleString("abcdefg");
      SimpleString middle = SimpleString.toSimpleString("hijklmnop");
      SimpleString end = SimpleString.toSimpleString("qrstuvwxyz");
      Assert.assertEquals(start.concat(middle).concat(end), SimpleString.toSimpleString("abcdefghijklmnopqrstuvwxyz"));
      Assert.assertEquals(start.concat('.').concat(end), SimpleString.toSimpleString("abcdefg.qrstuvwxyz"));
      // Testing concat of SimpleString with String
      for (int i = 0; i < 10; i++) {
         Assert.assertEquals(SimpleString.toSimpleString("abcdefg-" + i), start.concat("-" + Integer.toString(i)));

      }
   }

   @Test
   public void testMultithreadHashCode() throws Exception {
      for (int repeat = 0; repeat < 10; repeat++) {

         StringBuffer buffer = new StringBuffer();

         for (int i = 0; i < 100; i++) {
            buffer.append("Some Big String " + i);
         }
         String strvalue = buffer.toString();

         final int initialhash = SimpleString.toSimpleString(strvalue).hashCode();

         final SimpleString value = SimpleString.toSimpleString(strvalue);

         int nThreads = 100;
         final CountDownLatch latch = new CountDownLatch(nThreads);
         final CountDownLatch start = new CountDownLatch(1);

         class T extends Thread {

            boolean failed = false;

            @Override
            public void run() {
               try {
                  latch.countDown();
                  start.await();

                  int newhash = value.hashCode();

                  if (newhash != initialhash) {
                     failed = true;
                  }
               } catch (Exception e) {
                  e.printStackTrace();
                  failed = true;
               }
            }
         }

         T[] x = new T[nThreads];
         for (int i = 0; i < nThreads; i++) {
            x[i] = new T();
            x[i].start();
         }

         ActiveMQTestBase.waitForLatch(latch);
         start.countDown();

         for (T t : x) {
            t.join();
         }

         for (T t : x) {
            Assert.assertFalse(t.failed);
         }
      }
   }

}
