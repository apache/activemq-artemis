/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.cli.commands.tools;

import java.text.DecimalFormat;

import io.airlift.airline.Command;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.util.SyncCalculation;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.JournalType;

@Command(name = "sync", description = "Calculates the journal-buffer-timeout you should use with the current data folder")
public class SyncRecalc extends LockAbstract {

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      FileConfiguration fileConfiguration = getFileConfiguration();

      int writes = 250;
      System.out.println("");
      System.out.println("Auto tuning journal ...");

      long time = SyncCalculation.syncTest(fileConfiguration.getJournalLocation(), 4096, writes, 5, verbose, fileConfiguration.isJournalDatasync(), fileConfiguration.getJournalType() == JournalType.ASYNCIO);
      long nanosecondsWait = SyncCalculation.toNanos(time, writes, verbose);
      double writesPerMillisecond = (double) writes / (double) time;

      String writesPerMillisecondStr = new DecimalFormat("###.##").format(writesPerMillisecond);

      context.out.println("Your system can execute " + writesPerMillisecondStr + " syncs per millisecond");
      context.out.println("Your journal-buffer-timeout should be:" + nanosecondsWait);
      context.out.println("You should use this following configuration:");
      context.out.println();
      context.out.println("<journal-buffer-timeout>" + nanosecondsWait + "</journal-buffer-timeout>");

      return null;
   }
}
