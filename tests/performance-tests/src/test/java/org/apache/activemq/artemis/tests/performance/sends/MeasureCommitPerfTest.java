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
package org.apache.activemq.artemis.tests.performance.sends;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

public class MeasureCommitPerfTest extends AbstractSendReceivePerfTest {

   @Override
   protected void consumeMessages(Connection c, String qName) throws Exception {
   }

   /* This will by default send non persistent messages */
   protected void sendMessages(Connection c, String qName) throws JMSException {
      Session s = c.createSession(true, Session.SESSION_TRANSACTED);

      long timeout = System.currentTimeMillis() + 30 * 1000;

      long startMeasure = System.currentTimeMillis() + 5000;
      long start = 0;
      long committs = 0;
      while (timeout > System.currentTimeMillis()) {

         if (start == 0 && System.currentTimeMillis() > startMeasure) {
            System.out.println("heat up");
            start = System.currentTimeMillis();
            committs = 0;
         }

         s.commit();
         committs++;
         if (start > 0 && committs % 1000 == 0)
            printCommitsSecond(start, committs);
      }
      printCommitsSecond(start, committs);

      s.close();
   }

   protected void printCommitsSecond(final long start, final double committs) {

      long end = System.currentTimeMillis();
      double elapsed = ((double) end - (double) start) / 1000f;

      double commitsPerSecond = committs / elapsed;

      System.out.println("end = " + end + ", start=" + start + ", numberOfMessages=" + committs + ", elapsed=" + elapsed + " msgs/sec= " + commitsPerSecond);

   }

}
