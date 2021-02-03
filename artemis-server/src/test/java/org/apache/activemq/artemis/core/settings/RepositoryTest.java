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
package org.apache.activemq.artemis.core.settings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RepositoryTest extends ActiveMQTestBase {

   HierarchicalRepository<HashSet<Role>> securityRepository;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      securityRepository = new HierarchicalObjectRepository<>();
   }

   @Test
   public void testDefault() {
      securityRepository.setDefault(new HashSet<Role>());
      HashSet<Role> roles = securityRepository.getMatch("queues.something");

      Assert.assertEquals(roles.size(), 0);
   }

   @Test
   public void testMatchingDocs() throws Throwable {
      HierarchicalObjectRepository<String> repo = new HierarchicalObjectRepository<>();

      repo.addMatch("a.b.#", "ab#");
      repo.addMatch("a.b.d.#", "abd#");
      repo.addMatch("#", "root");

      Assert.assertEquals("ab#", repo.getMatch("a.b"));
      Assert.assertEquals("ab#", repo.getMatch("a.b.c"));
      Assert.assertEquals("abd#", repo.getMatch("a.b.d.lll"));
      Assert.assertEquals("root", repo.getMatch("z.z.z.z.z"));
      Assert.assertEquals("root", repo.getMatch("a.babc"));
      Assert.assertEquals("ab#", repo.getMatch("a.b.dabc"));
      Assert.assertEquals("abd#", repo.getMatch("a.b.d"));
   }

   @Test
   public void testCacheWithWildcards() throws Throwable {
      HierarchicalObjectRepository<String> repo = new HierarchicalObjectRepository<>();

      repo.addMatch("#", "root");
      Assert.assertEquals("root", repo.getMatch("b"));

      repo.addMatch("b", "leaf");
      Assert.assertEquals("leaf", repo.getMatch("b"));
   }

   @Test
   public void testMultipleMatchesHasRightOrder() {
      HierarchicalRepository<String> repository = new HierarchicalObjectRepository<>();
      repository.addMatch("a.b.c.d.e.f", "a.b.c.d.e.f");//1
      repository.addMatch("a.b.c.d.e.*", "a.b.c.d.e.*");//2
      repository.addMatch("a.*.*.*.*.*", "a.*.*.*.*.*");//3
      repository.addMatch("*.b.c.d.*.f", "*.b.c.d.*.f");//4
      repository.addMatch("*.b.*.d.*.f", "*.b.*.d.*.f");//5
      repository.addMatch("a.b.c.d.e.#", "a.b.c.d.e.#");//6

      String val = repository.getMatch("a.b.c.d.e.f");//matches all
      Assert.assertEquals("a.b.c.d.e.f", val);
      val = repository.getMatch("a.b.c.d.e.x");//matches 2,3,6
      Assert.assertEquals("a.b.c.d.e.*", val);
      val = repository.getMatch("a.b.x.d.x.f");//matches 3,5
      Assert.assertEquals("a.*.*.*.*.*", val);
      val = repository.getMatch("x.b.c.d.e.f");//matches 4,5
      Assert.assertEquals("*.b.c.d.*.f", val);
      val = repository.getMatch("x.b.x.d.e.f");//matches 5
      Assert.assertEquals("*.b.*.d.*.f", val);
      val = repository.getMatch("a.b.c.d.e.f.g");//matches 6
      Assert.assertEquals("a.b.c.d.e.#", val);
   }

   @Test
   public void testMatchingDocsCustomUnderscorDelimiter() throws Throwable {
      WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      wildcardConfiguration.setDelimiter('_');
      HierarchicalObjectRepository<String> repo = new HierarchicalObjectRepository<>(wildcardConfiguration);

      repo.addMatch("a_b_#", "ab#");
      repo.addMatch("a_b_d_#", "abd#");
      repo.addMatch("#", "root");

      Assert.assertEquals("ab#", repo.getMatch("a_b"));
      Assert.assertEquals("ab#", repo.getMatch("a_b_c"));
      Assert.assertEquals("abd#", repo.getMatch("a_b_d_lll"));
      Assert.assertEquals("root", repo.getMatch("z_z_z_z_z"));
      Assert.assertEquals("root", repo.getMatch("a_babc"));
      Assert.assertEquals("ab#", repo.getMatch("a_b_dabc"));
      Assert.assertEquals("abd#", repo.getMatch("a_b_d"));
   }

   @Test
   public void testMatchingDocsCustomForwardSlashDelimiter() throws Throwable {
      WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      wildcardConfiguration.setDelimiter('/');
      HierarchicalObjectRepository<String> repo = new HierarchicalObjectRepository<>(wildcardConfiguration);

      repo.addMatch("a/b/#", "ab#");
      repo.addMatch("a/b/d/#", "abd#");
      repo.addMatch("#", "root");

      Assert.assertEquals("ab#", repo.getMatch("a/b"));
      Assert.assertEquals("ab#", repo.getMatch("a/b/c"));
      Assert.assertEquals("abd#", repo.getMatch("a/b/d/lll"));
      Assert.assertEquals("root", repo.getMatch("z/z/z/z/z"));
      Assert.assertEquals("root", repo.getMatch("a/babc"));
      Assert.assertEquals("ab#", repo.getMatch("a/b/dabc"));
      Assert.assertEquals("abd#", repo.getMatch("a/b/d"));
   }

   @Test
   public void testSingleMatch() {
      securityRepository.addMatch("queues.*", new HashSet<Role>());
      HashSet<Role> hashSet = securityRepository.getMatch("queues.something");
      Assert.assertEquals(hashSet.size(), 0);
   }

   @Test
   public void testSingletwo() {
      securityRepository.addMatch("queues.another.aq.*", new HashSet<Role>());
      HashSet<Role> roles = new HashSet<>(2);
      roles.add(new Role("test1", true, true, true, true, true, true, true, true, true, true));
      roles.add(new Role("test2", true, true, true, true, true, true, true, true, true, true));
      securityRepository.addMatch("queues.aq", roles);
      HashSet<Role> roles2 = new HashSet<>(2);
      roles2.add(new Role("test1", true, true, true, true, true, true, true, true, true, true));
      roles2.add(new Role("test2", true, true, true, true, true, true, true, true, true, true));
      roles2.add(new Role("test3", true, true, true, true, true, true, true, true, true, true));
      securityRepository.addMatch("queues.another.andanother", roles2);

      HashSet<Role> hashSet = securityRepository.getMatch("queues.another.andanother");
      Assert.assertEquals(hashSet.size(), 3);
   }

   @Test
   public void testWithoutWildcard() {
      securityRepository.addMatch("queues.1.*", new HashSet<Role>());
      HashSet<Role> roles = new HashSet<>(2);
      roles.add(new Role("test1", true, true, true, true, true, true, true, true, true, true));
      roles.add(new Role("test2", true, true, true, true, true, true, true, true, true, true));
      securityRepository.addMatch("queues.2.aq", roles);
      HashSet<Role> hashSet = securityRepository.getMatch("queues.2.aq");
      Assert.assertEquals(hashSet.size(), 2);
   }

   @Test
   public void testMultipleWildcards() {
      HierarchicalRepository<String> repository = new HierarchicalObjectRepository<>();
      repository.addMatch("#", "#");
      repository.addMatch("a", "a");
      repository.addMatch("a.#", "a.#");
      repository.addMatch("a.*", "a.*");
      repository.addMatch("a.b.c", "a.b.c");
      repository.addMatch("a.*.c", "a.*.c");
      repository.addMatch("a.d.c", "a.d.c");
      repository.addMatch("a.b.#", "a.b.#");
      repository.addMatch("a.b", "a.b");
      repository.addMatch("a.b.c.#", "a.b.c.#");
      repository.addMatch("a.b.c.d", "a.b.c.d");
      repository.addMatch("a.*.*.d", "a.*.*.d");
      repository.addMatch("a.*.d.#", "a.*.d.#");
      String val = repository.getMatch("a");
      Assert.assertEquals("a", val);
      val = repository.getMatch("a.b");
      Assert.assertEquals("a.b", val);
      val = repository.getMatch("a.x");
      Assert.assertEquals("a.*", val);
      val = repository.getMatch("a.b.x");
      Assert.assertEquals("a.b.#", val);
      val = repository.getMatch("a.b.c");
      Assert.assertEquals("a.b.c", val);
      val = repository.getMatch("a.d.c");
      Assert.assertEquals("a.d.c", val);
      val = repository.getMatch("a.x.c");
      Assert.assertEquals("a.*.c", val);
      val = repository.getMatch("a.b.c.d");
      Assert.assertEquals("a.b.c.d", val);
      val = repository.getMatch("a.x.c.d");
      Assert.assertEquals("a.*.*.d", val);
      val = repository.getMatch("a.b.x.d");
      Assert.assertEquals("a.*.*.d", val);
      val = repository.getMatch("a.d.x.d");
      Assert.assertEquals("a.*.*.d", val);
      val = repository.getMatch("a.d.d.g");
      Assert.assertEquals("a.*.d.#", val);
      val = repository.getMatch("zzzz.z.z.z.d.r.g.f.sd.s.fsdfd.fsdfs");
      Assert.assertEquals("#", val);
   }

   @Test
   public void testRepositoryMerge() {
      HierarchicalRepository<DummyMergeable> repository = new HierarchicalObjectRepository<>();
      repository.addMatch("#", new DummyMergeable(1));
      repository.addMatch("a.#", new DummyMergeable(2));
      repository.addMatch("b.#", new DummyMergeable(3));
      repository.addMatch("a.b.#", new DummyMergeable(4));
      repository.addMatch("b.c.#", new DummyMergeable(5));
      repository.addMatch("a.b.c.#", new DummyMergeable(6));
      repository.addMatch("a.b.*.d", new DummyMergeable(7));
      repository.addMatch("a.b.c.*", new DummyMergeable(8));
      repository.getMatch("a.b.c.d");
      Assert.assertEquals(5, DummyMergeable.timesMerged);
      Assert.assertTrue(DummyMergeable.contains(1));
      Assert.assertTrue(DummyMergeable.contains(2));
      Assert.assertTrue(DummyMergeable.contains(4));
      Assert.assertTrue(DummyMergeable.contains(7));
      Assert.assertTrue(DummyMergeable.contains(8));
      DummyMergeable.reset();
      repository.getMatch("a.b.c");
      Assert.assertEquals(3, DummyMergeable.timesMerged);
      Assert.assertTrue(DummyMergeable.contains(1));
      Assert.assertTrue(DummyMergeable.contains(2));
      Assert.assertTrue(DummyMergeable.contains(4));
      DummyMergeable.reset();
      repository.getMatch("z");
      Assert.assertEquals(0, DummyMergeable.timesMerged);
      DummyMergeable.reset();
   }

   @Test
   public void testAddListener() {
      HierarchicalRepository<String> repository = new HierarchicalObjectRepository<>();
      repository.addMatch("#", "1");
      repository.addMatch("B", "2");

      final AtomicInteger called = new AtomicInteger(0);
      repository.registerListener(new HierarchicalRepositoryChangeListener() {
         @Override
         public void onChange() {
            called.incrementAndGet();
         }
      });

      assertEquals(1, called.get());

      repository.disableListeners();

      repository.addMatch("C", "3");

      assertEquals(1, called.get());

      repository.enableListeners();

      assertEquals(2, called.get());

      repository.addMatch("D", "4");

      assertEquals(3, called.get());

      repository.removeMatch("D");

      assertEquals(4, called.get());

      repository.disableListeners();

      repository.removeMatch("C");

      assertEquals(4, called.get());
   }

   @Test
   public void testIllegalMatches() {
      HierarchicalRepository<String> repository = new HierarchicalObjectRepository<>();
      try {
         repository.addMatch("hjhjhjhjh.#.hhh", "test");
         fail("expected exception");
      } catch (IllegalArgumentException e) {
         // pass
      }
      try {
         repository.addMatch(null, "test");
         fail("expected exception");
      } catch (IllegalArgumentException e) {
         // pass
      }
   }

   static class DummyMergeable implements Mergeable {

      static int timesMerged = 0;

      static ArrayList<Integer> merged = new ArrayList<>();

      private final Integer id;

      static void reset() {
         DummyMergeable.timesMerged = 0;
         DummyMergeable.merged = new ArrayList<>();
      }

      static boolean contains(final Integer i) {
         return DummyMergeable.merged.contains(i);
      }

      DummyMergeable(final Integer id) {
         this.id = id;
      }

      @Override
      public void merge(final Object merged) {
         DummyMergeable.timesMerged++;
         DummyMergeable.merged.add(id);
         DummyMergeable.merged.add(((DummyMergeable) merged).id);
      }
   }
}
