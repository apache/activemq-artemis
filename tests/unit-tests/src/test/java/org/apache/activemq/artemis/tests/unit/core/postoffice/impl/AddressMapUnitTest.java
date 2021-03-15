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
package org.apache.activemq.artemis.tests.unit.core.postoffice.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Address;
import org.apache.activemq.artemis.core.postoffice.impl.AddressImpl;
import org.apache.activemq.artemis.core.postoffice.impl.AddressMap;
import org.apache.activemq.artemis.core.postoffice.impl.AddressMapVisitor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AddressMapUnitTest {

   AddressMap<SimpleString> underTest = new AddressMap<>("#", "*", '.');

   @Test
   public void testAddGetRemove() throws Exception {

      SimpleString a = new SimpleString("a.b.c");

      assertTrue(isEmpty(a));

      underTest.put(a, a);

      assertFalse(isEmpty(a));

      assertEquals(1, countMatchingWildcards(a));

      underTest.remove(a, a);

      assertTrue(isEmpty(a));
   }

   private boolean isEmpty(SimpleString match) throws Exception {
      return countMatchingWildcards(match) == 0;
   }

   @Test
   public void testWildcardAddGet() throws Exception {

      SimpleString a = new SimpleString("a.*.c");

      assertTrue(isEmpty(a));

      underTest.put(a, a);

      assertFalse(isEmpty(a));

      assertEquals(1, countMatchingWildcards(a));

      underTest.remove(a, a);

      assertTrue(isEmpty(a));
   }

   @Test
   public void testWildcardAllAddGet() throws Exception {

      SimpleString a = new SimpleString("a.b.#");

      assertTrue(isEmpty(a));

      underTest.put(a, a);

      assertFalse(isEmpty(a));

      assertEquals(1, countMatchingWildcards(a));

      underTest.remove(a, a);

      assertTrue(isEmpty(a));
   }

   @Test
   public void testNoDots() throws Exception {
      SimpleString s1 = new SimpleString("abcde");
      SimpleString s2 = new SimpleString("abcde");

      underTest.put(s1, s1);
      assertEquals(1, countMatchingWildcards(s2));
   }

   @Test
   public void testDotsSameLength2() throws Exception {
      SimpleString s1 = new SimpleString("a.b");
      SimpleString s2 = new SimpleString("a.b");

      underTest.put(s1, s1);
      assertEquals(1, countMatchingWildcards(s2));
   }

   @Test
   public void testA() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c");
      SimpleString s2 = new SimpleString("a.b.c.d.e.f.g.h.i.j.k.l.m.n.*");

      underTest.put(s1, s1);
      assertEquals(0, countMatchingWildcards(s2));
   }

   @Test
   public void testB() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.*");

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));
   }

   @Test
   public void testC() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.c.x");
      SimpleString s3 = new SimpleString("a.b.*.d");

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));
   }

   @Test
   public void testD() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e");
      SimpleString s2 = new SimpleString("a.b.c.x.e");
      SimpleString s3 = new SimpleString("a.b.*.d.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));
   }

   @Test
   public void testE() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.b.*.d.*.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));

   }

   @Test
   public void testF() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countNonWildcardMatching(s3));

   }

   @Test
   public void testG() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countNonWildcardMatching(s3));

   }

   @Test
   public void testH() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("#.b.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countNonWildcardMatching(s3));
   }

   @Test
   public void testI() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#.b.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countNonWildcardMatching(s3));

   }

   @Test
   public void testJ() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#.c.d.e.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));

   }

   @Test
   public void testK() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.d.e.x");
      SimpleString s3 = new SimpleString("a.#.c.d.e.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countNonWildcardMatching(s3));

   }

   @Test
   public void testL() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.d.e.x");
      SimpleString s3 = new SimpleString("a.#.c.d.*.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));

   }

   @Test
   public void testM() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));

   }

   @Test
   public void testN() throws Exception {
      SimpleString s1 = new SimpleString("usd.stock");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("*.stock.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));
   }

   @Test
   public void testO() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countNonWildcardMatching(s3));

   }

   @Test
   public void testP() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("a.b.c#");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);

      assertEquals(0, countNonWildcardMatching(s3));

   }

   @Test
   public void testQ() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("#a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countNonWildcardMatching(s3));

   }

   @Test
   public void testR() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("#*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countNonWildcardMatching(s3));

   }

   @Test
   public void testS() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("a.b.c*");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countNonWildcardMatching(s3));

   }

   @Test
   public void testT() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countNonWildcardMatching(s3));

   }

   @Test
   public void testU() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countNonWildcardMatching(s3));

   }

   @Test
   public void testV() throws Exception {
      final SimpleString s1 = new SimpleString("a.b.d");
      final SimpleString s3 = new SimpleString("a.b.#.d");
      final Address a1 = new AddressImpl(s1);
      final Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(1, countNonWildcardMatching(s3));

      final SimpleString s2 = new SimpleString("a.b.b.b.b.d");
      underTest.put(s2, s2);
      assertEquals(2, countNonWildcardMatching(s3));
   }

   @Test
   public void testVReverse() throws Exception {
      final SimpleString s1 = new SimpleString("a.b.d");
      final SimpleString s3 = new SimpleString("a.b.#.d");
      final Address a1 = new AddressImpl(s1);
      final Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));

      underTest.put(s3, s3);
      assertEquals(1, countMatchingWildcards(s1));

   }

   @Test
   public void testHashNMatch() throws Exception {

      SimpleString addressABCF = new SimpleString("a.b.c.f");
      SimpleString addressACF = new SimpleString("a.c.f");
      SimpleString match = new SimpleString("a.#.f");

      underTest.put(addressABCF, addressABCF);
      underTest.put(addressACF, addressACF);

      assertEquals(2, countNonWildcardMatching(match));
   }

   @Test
   public void testEndHash() throws Exception {

      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressACF = new SimpleString("a.c.f");
      SimpleString addressABC = new SimpleString("a.b.c");
      SimpleString match = new SimpleString("a.b.#");

      underTest.put(addressAB, addressAB);
      underTest.put(addressACF, addressACF);

      assertEquals(1, countNonWildcardMatching(match));

      underTest.put(addressABC, addressABC);
      assertEquals(2, countNonWildcardMatching(match));
   }

   @Test
   public void testHashEndInMap() throws Exception {

      SimpleString addressABHash = new SimpleString("a.b.#");
      SimpleString addressABC = new SimpleString("a.b.c");
      SimpleString match = new SimpleString("a.b");

      underTest.put(addressABHash, addressABHash);
      underTest.put(addressABC, addressABC);

      assertEquals(1, countMatchingWildcards(match));
   }

   private int countMatchingWildcards(SimpleString plainAddress) throws Exception {

      final AtomicInteger count = new AtomicInteger();
      underTest.visitMatchingWildcards(plainAddress, value -> {
         count.incrementAndGet();
      });

      return count.get();
   }

   private int countNonWildcardMatching(SimpleString canBeWildcardAddress) throws Exception {

      final AtomicInteger count = new AtomicInteger();
      underTest.visitMatching(canBeWildcardAddress, value -> {
         count.incrementAndGet();
      });

      return count.get();
   }

   @Test
   public void testHashEndMatchMap() throws Exception {

      SimpleString match = new SimpleString("a.b.#");
      SimpleString addressABC = new SimpleString("a.b.c");
      SimpleString addressAB = new SimpleString("a.b");

      underTest.put(addressAB, addressAB);
      underTest.put(addressABC, addressABC);

      assertEquals(0, countMatchingWildcards(match));
      assertEquals(2, countNonWildcardMatching(match));

      underTest.put(match, match);
      assertEquals(1, countMatchingWildcards(match));

   }

   @Test
   public void testHashAGet() throws Exception {

      SimpleString hashA = new SimpleString("#.a");
      underTest.put(hashA, hashA);

      SimpleString matchA = new SimpleString("a");
      SimpleString matchAB = new SimpleString("a.b");

      assertEquals(1, countMatchingWildcards(matchA));
      assertEquals(0, countMatchingWildcards(matchAB));

      AddressImpl aStar = new AddressImpl(hashA);
      AddressImpl aA = new AddressImpl(matchA);
      assertTrue(aA.matches(aStar));

      AddressImpl aAB = new AddressImpl(matchAB);
      assertFalse(aAB.matches(aStar));
   }

   @Test
   public void testStarOne() throws Exception {

      SimpleString star = new SimpleString("*");
      underTest.put(star, star);

      SimpleString matchA = new SimpleString("a");
      SimpleString matchAB = new SimpleString("a.b");

      final AtomicInteger count = new AtomicInteger();
      underTest.visitMatchingWildcards(matchA, value -> count.incrementAndGet());
      assertEquals(1, count.get());

      count.set(0);

      underTest.visitMatchingWildcards(matchAB, value -> count.incrementAndGet());

      assertEquals(0, count.get());
   }

   @Test
   public void testHashOne() throws Exception {

      SimpleString hash = new SimpleString("#");
      underTest.put(hash, hash);

      SimpleString matchA = new SimpleString("a");
      SimpleString matchAB = new SimpleString("a.b");
      SimpleString matchABC = new SimpleString("a.b.c");

      final AtomicInteger count = new AtomicInteger();
      AddressMapVisitor<SimpleString> countCollector = value -> count.incrementAndGet();

      count.set(0);
      underTest.visitMatchingWildcards(matchA, countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatchingWildcards(matchAB, countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatchingWildcards(matchABC, countCollector);
      assertEquals(1, count.get());
   }

   @Test
   public void testHashAMatch() throws Exception {

      SimpleString a = new SimpleString("a");
      underTest.put(a, a);

      assertEquals(1, countNonWildcardMatching(new SimpleString("#.a")));

      assertEquals(1, countMatchingWildcards(new SimpleString("a")));
   }

   @Test
   public void testHashA() throws Exception {

      SimpleString hashA = new SimpleString("#.a");
      underTest.put(hashA, hashA);

      assertEquals(1, countMatchingWildcards(new SimpleString("a")));

      assertEquals(1, countMatchingWildcards(new SimpleString("d.f.c.a")));

      // has to end in 'a', and not being with 'a'
      SimpleString abcaS = new SimpleString("a.b.c.a");
      AddressImpl aHashA = new AddressImpl(hashA);
      AddressImpl aABCA = new AddressImpl(abcaS);
      assertFalse(aABCA.matches(aHashA));
      assertFalse(aHashA.matches(aABCA));

      assertEquals(1, countMatchingWildcards(abcaS));

      assertEquals(0, countMatchingWildcards(new SimpleString("a.b")));

      assertEquals(0, countMatchingWildcards(new SimpleString("a.b.c")));

      assertEquals(0, countMatchingWildcards(new SimpleString("a.b.c.a.d")));

      // will match a.....a
      SimpleString AHashA = new SimpleString("a.#.a");
      underTest.put(AHashA, AHashA);

      assertEquals(2, countMatchingWildcards(new SimpleString("a.b.c.a")));

      assertEquals(0, countNonWildcardMatching(new SimpleString("a.b.c.a")));

      // only now remove the #.a
      underTest.remove(hashA, hashA);

      assertEquals(1, countMatchingWildcards(new SimpleString("a.b.c.a")));

      assertEquals(1, countMatchingWildcards(new SimpleString("a.a")));

   }

   @Test
   public void testAHashA() throws Exception {

      final AtomicInteger count = new AtomicInteger();
      AddressMapVisitor<SimpleString> countCollector = value -> count.incrementAndGet();

      // will match a.....a
      SimpleString AHashA = new SimpleString("a.#.a");
      underTest.put(AHashA, AHashA);

      count.set(0);
      underTest.visitMatchingWildcards(new SimpleString("a.b.c.a"), countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatchingWildcards(new SimpleString("a.a"), countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatchingWildcards(new SimpleString("a"), countCollector);
      assertEquals(0, count.get());
   }

   @Test
   public void testStar() throws Exception {

      SimpleString star = new SimpleString("*");
      SimpleString addressA = new SimpleString("a");
      SimpleString addressAB = new SimpleString("a.b");

      underTest.put(star, star);
      underTest.put(addressAB, addressAB);

      final AtomicInteger count = new AtomicInteger();
      underTest.visitMatchingWildcards(addressA, value -> count.incrementAndGet());

      assertEquals(1, count.get());
   }

   @Test
   public void testSomeAndAny() throws Exception {

      SimpleString star = new SimpleString("test.*.some.#");
      underTest.put(star, star);

      assertEquals(0, countNonWildcardMatching(star));
      assertEquals(1, countMatchingWildcards(star));

      SimpleString addressA = new SimpleString("test.1.some.la");
      underTest.put(addressA, addressA);

      assertEquals(1, countMatchingWildcards(star));
      assertEquals(1, countNonWildcardMatching(star));

      assertEquals(2, countMatchingWildcards(addressA));
      assertEquals(1, countNonWildcardMatching(addressA));

   }

   @Test
   public void testAnyAndSome() throws Exception {

      SimpleString star = new SimpleString("test.#.some.*");
      underTest.put(star, star);

      assertEquals(1, countMatchingWildcards(star));

      // add another match
      SimpleString addressA = new SimpleString("test.1.some.la");
      underTest.put(addressA, addressA);

      assertEquals(1, countMatchingWildcards(star));

      assertEquals(1, countNonWildcardMatching(star));

      assertEquals(1, countNonWildcardMatching(addressA));

      assertEquals(2, countMatchingWildcards(addressA));

   }

   @Test
   public void testAnyAndSomeInMap() throws Exception {

      SimpleString hashHash = new SimpleString("test.#.some.#");
      underTest.put(hashHash, hashHash);

      SimpleString starStar = new SimpleString("test.*.some.*");
      underTest.put(starStar, starStar);

      SimpleString hashStar = new SimpleString("test.#.A.*");
      underTest.put(hashStar, hashStar);

      SimpleString oneHashStar = new SimpleString("test.1.#.T");
      underTest.put(oneHashStar, oneHashStar);

      assertEquals(2, countMatchingWildcards(hashHash));
      assertEquals(0, countNonWildcardMatching(hashHash));

      SimpleString reqular = new SimpleString("test.a.b.some");
      underTest.put(reqular, reqular);
      assertEquals(1, countNonWildcardMatching(hashHash));

      assertEquals(1, countNonWildcardMatching(reqular));

      assertEquals(2, countMatchingWildcards(reqular));
   }

   @Test
   public void testHashAandHashB() throws Exception {

      SimpleString hashAhash = new SimpleString("test.#.aaa.#");
      underTest.put(hashAhash, hashAhash);

      SimpleString hashBhash = new SimpleString("test.#.bbb.#");
      underTest.put(hashBhash, hashBhash);

      assertEquals(2, countMatchingWildcards(SimpleString.toSimpleString("test.aaa.bbb")));
      assertEquals(2, countMatchingWildcards(SimpleString.toSimpleString("test.bbb.aaa")));

      assertEquals(2, countMatchingWildcards(SimpleString.toSimpleString("test.bbb.aaa.ccc")));
      assertEquals(2, countMatchingWildcards(SimpleString.toSimpleString("test.aaa.bbb.ccc")));
   }

   @Test
   public void testHashNoHashNo() throws Exception {

      SimpleString hashAhash = new SimpleString("test.#.0.#.168");
      underTest.put(hashAhash, hashAhash);

      assertEquals(1, countMatchingWildcards(SimpleString.toSimpleString("test.0.168")));
   }

   @Test
   public void testHashNoHashHashNo() throws Exception {

      SimpleString v = new SimpleString("test.#.0.#.168");
      underTest.put(v, v);

      v = new SimpleString("test.#.0.#");
      underTest.put(v, v);

      v = new SimpleString("test.0.#");
      underTest.put(v, v);

      assertEquals(3, countMatchingWildcards(SimpleString.toSimpleString("test.0.168")));
   }

   @Test
   public void testHashNoHashNoWithNMatch() throws Exception {

      for (String s : new String[] {"t.#.0.#", "t.#.1.#", "t.#.2.#", "t.#.3.#", "t.#.1.2.3", "t.0.1.2.3"}) {
         SimpleString v = new SimpleString(s);
         underTest.put(v, v);
      }
      assertEquals(6, countMatchingWildcards(SimpleString.toSimpleString("t.0.1.2.3")));
   }

   @Test
   public void testSomeMoreHashPlacement() throws Exception {

      for (String s : new String[] {"t.#.0.#", "t.0.1.#", "t.0.1.2.#", "t.0.1.#.2.3", "t.*.#.1.2.3"}) {
         SimpleString v = new SimpleString(s);
         underTest.put(v, v);
      }
      assertEquals(5, countMatchingWildcards(SimpleString.toSimpleString("t.0.1.2.3")));
      assertEquals(3, countMatchingWildcards(SimpleString.toSimpleString("t.0.1.2.3.4")));
   }

   @Test
   public void testManyEntries() throws Exception {

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test." + i);
         underTest.put(star, star);
      }

      assertEquals(10, countNonWildcardMatching(new SimpleString("test.*")));

      assertEquals(10, countNonWildcardMatching(new SimpleString("test.#")));

      assertEquals(1, countMatchingWildcards(new SimpleString("test.0")));

      underTest.put(new SimpleString("test.#"), new SimpleString("test.#"));
      underTest.put(new SimpleString("test.*"), new SimpleString("test.*"));

      assertEquals(3, countMatchingWildcards(new SimpleString("test.1")));

      assertEquals(10, countNonWildcardMatching(new SimpleString("test.#")));

      assertEquals(10, countNonWildcardMatching(new SimpleString("test.*")));

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test.a." + i);
         underTest.put(star, star);
      }

      assertEquals(2, countMatchingWildcards(new SimpleString("test.a.0")));
      assertEquals(20, countNonWildcardMatching(new SimpleString("test.#")));

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test.b." + i);
         underTest.put(star, star);
      }

      assertEquals(10, countNonWildcardMatching(new SimpleString("test.b.*")));
      underTest.remove(new SimpleString("test.#"), new SimpleString("test.#"));

      assertEquals(10, countNonWildcardMatching(new SimpleString("test.b.*")));
      assertEquals(1, countMatchingWildcards(new SimpleString("test.a.0")));

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test.c." + i);
         underTest.put(star, star);
      }
      assertEquals(10, countNonWildcardMatching(new SimpleString("test.c.*")));

      SimpleString testStarStar = new SimpleString("test.*.*");
      assertEquals(30, countNonWildcardMatching(testStarStar));

      underTest.put(testStarStar, testStarStar);
      assertEquals(30, countNonWildcardMatching(testStarStar));
      assertEquals(1, countMatchingWildcards(testStarStar));

      assertEquals(1, countMatchingWildcards(new SimpleString("test.b.c")));
   }

   @Test
   public void testReset() throws Exception {
      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test." + i);
         underTest.put(star, star);
      }

      assertEquals(0, countMatchingWildcards(new SimpleString("test.*")));

      assertEquals(10, countNonWildcardMatching(new SimpleString("test.*")));
      underTest.reset();
      assertEquals(0, countNonWildcardMatching(new SimpleString("test.*")));
   }

   @Test
   public void testRemove() throws Exception {
      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test." + i);
         underTest.put(star, star);
      }

      SimpleString test1 = new SimpleString("test.1");
      assertEquals(1, countMatchingWildcards(test1));

      underTest.remove(test1, test1);
      assertEquals(0, countMatchingWildcards(test1));

      assertEquals(9, countNonWildcardMatching(new SimpleString("test.*")));

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test." + i);
         underTest.remove(star, star);
      }

      assertEquals(0, countNonWildcardMatching(new SimpleString("test.*")));
   }

   @Test
   public void testMax() throws Exception {

      underTest.put(new SimpleString("test.#.a"), new SimpleString("test.#.a"));
      underTest.put(new SimpleString("test.*.a"), new SimpleString("test.*.a"));
      underTest.put(new SimpleString("*.a"), new SimpleString("*.a"));
      underTest.put(new SimpleString("#.a"), new SimpleString("#.a"));

      assertEquals(3, countMatchingWildcards(new SimpleString("test.a")));
      assertEquals(3, countMatchingWildcards(new SimpleString("test.a.a")));
   }

}