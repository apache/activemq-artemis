/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.embedded;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Test;

/**
 * Validate if the embedded server will start even with AIO selected
 *
 * @author Clebert Suconic
 */
public class ValidateAIOTest extends ServiceTestBase
{

   @Test
   public void testValidateAIO() throws Exception
   {
      Configuration config = createDefaultConfig(false)
         // This will force AsyncIO
         .setJournalType(JournalType.ASYNCIO);
      HornetQServer server = HornetQServers.newHornetQServer(config, true);
      try
      {
         server.start();
      }
      finally
      {
         server.stop();
      }

   }
}
