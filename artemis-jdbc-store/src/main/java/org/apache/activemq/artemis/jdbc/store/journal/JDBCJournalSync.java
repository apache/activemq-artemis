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

package org.apache.activemq.artemis.jdbc.store.journal;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;

public class JDBCJournalSync extends ActiveMQScheduledComponent {

   private final JDBCJournalImpl journal;

   public JDBCJournalSync(ScheduledExecutorService scheduledExecutorService,
                          Executor executor,
                          long checkPeriod,
                          TimeUnit timeUnit,
                          JDBCJournalImpl journal) {
      super(scheduledExecutorService, executor, checkPeriod, timeUnit, true);
      this.journal = journal;
   }

   @Override
   public void run() {
      if (journal.isStarted()) {
         journal.sync();
      }
   }
}
