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
package org.apache.activemq.jms.tests.stress;

import org.apache.activemq.jms.tests.JmsTestLogger;

import javax.jms.Session;

/**
 *
 * A Runner.
 *
 * Base class for running components of a stress test
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class Runner implements Runnable
{
   protected JmsTestLogger log = JmsTestLogger.LOGGER;

   protected Session sess;

   protected int numMessages;

   private boolean failed;

   public Runner(final Session sess, final int numMessages)
   {
      this.sess = sess;
      this.numMessages = numMessages;
   }

   public abstract void run();

   public boolean isFailed()
   {
      return failed;
   }

   public void setFailed(final boolean failed)
   {
      this.failed = failed;
      if (failed)
      {
         log.info("Marking Runner " + this + " as failed", new Exception("trace"));
      }
   }

}
