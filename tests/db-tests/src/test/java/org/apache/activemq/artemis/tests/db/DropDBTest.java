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
package org.apache.activemq.artemis.tests.db;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.db.common.Database;
import org.apache.activemq.artemis.tests.db.common.ParameterDBTestBase;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropDBTest extends ParameterDBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Parameterized.Parameters(name = "db={0}")
   public static Collection<Object[]> parameters() {
      List<Database> dbList = Database.selectedList();
      dbList.remove(Database.DERBY); // no derby on this test
      return convertParameters(dbList);
   }

   @Override
   public void setUp() throws Exception {
      super.setUp();
      Assume.assumeTrue(database != Database.DERBY);
      dropDatabase();
   }

   @Override
   protected final String getJDBCClassName() {
      return database.getDriverClass();
   }

   @Test
   public void testSimpleDrop() throws Exception {
      ActiveMQServer server = createServer(createDefaultConfig(0, true));
      server.start();
      server.stop();

      int tablesDroppped = dropDatabase();
      if (tablesDroppped < 4) {
         logger.warn("At least 4 tables should be removed, while only {} tables were dropped", tablesDroppped);
         Assert.fail("Only " + tablesDroppped + " tables were dropped");
      }
   }
}