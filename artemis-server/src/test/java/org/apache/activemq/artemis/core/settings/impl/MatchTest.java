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

package org.apache.activemq.artemis.core.settings.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.junit.jupiter.api.Test;

public class MatchTest {

   @Test
   public void predicateTestAnyChild() {

      final Match<?> underTest = new Match<>("test.#", null, new WildcardConfiguration());
      final Predicate<String> predicate = underTest.getPattern().asPredicate();

      assertTrue(predicate.test("test"));
      assertTrue(predicate.test("test.A"));
      assertTrue(predicate.test("test.A.B"));

      assertFalse(predicate.test("testing.A"));
   }

   @Test
   public void predicateTestAnyWord() {

      final Match<?> underTest = new Match<>("test.*", null, new WildcardConfiguration());
      final Predicate<String> predicate = underTest.getPattern().asPredicate();

      assertTrue(predicate.test("test.A"));

      assertFalse(predicate.test("testing.A"));
      assertFalse(predicate.test("test"));
      assertFalse(predicate.test("test.A.B"));
   }

   @Test
   public void patterDirectAnyChild() {

      final Pattern pattern = Match.createPattern("test.#", new WildcardConfiguration(), true);
      final Predicate<String> predicate = pattern.asPredicate();

      assertTrue(predicate.test("test.A"));
      assertTrue(predicate.test("test.A.B"));

      assertFalse(predicate.test("testing.A"));
      // see: org.apache.activemq.artemis.tests.integration.mqtt5.spec.controlpackets.PublishTests#testSubscriptionIdentifierMultiLevel
      assertFalse(predicate.test("test"));
   }

   @Test
   public void patterDirectAnyWord() {

      final Pattern pattern = Match.createPattern("test.*", new WildcardConfiguration(), true);
      final Predicate<String> predicate = pattern.asPredicate();

      // no change with direct = true|false
      assertTrue(predicate.test("test.A"));

      assertFalse(predicate.test("testing.A"));
      assertFalse(predicate.test("test"));
      assertFalse(predicate.test("test.A.B"));
   }

   @Test
   public void testDollarMatchingDirectTrue() {
      final Pattern pattern = Match.createPattern("$test.#", new WildcardConfiguration(), true);
      final Predicate<String> predicate = pattern.asPredicate();

      assertTrue(predicate.test("$test.A"));
      assertTrue(predicate.test("$test.A.B"));

      assertFalse(predicate.test("$testing.A"));
      assertFalse(predicate.test("$test"));
   }

   @Test
   public void testDollarMatchingDirectFalse() {
      final Pattern pattern = Match.createPattern("$test.#", new WildcardConfiguration(), false);
      final Predicate<String> predicate = pattern.asPredicate();

      assertTrue(predicate.test("$test"));
      assertTrue(predicate.test("$test.A"));
      assertTrue(predicate.test("$test.A.B"));

      assertFalse(predicate.test("$testing.A"));
   }
}