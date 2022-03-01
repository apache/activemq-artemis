/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.routing.targets;

import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TargetMonitor extends ActiveMQScheduledComponent implements TargetListener {
   private static final Logger logger = Logger.getLogger(TargetMonitor.class);


   private final Target target;

   private final List<TargetProbe> targetProbes;

   private volatile boolean targetReady = false;


   public Target getTarget() {
      return target;
   }

   public boolean isTargetReady() {
      return targetReady;
   }


   public TargetMonitor(ScheduledExecutorService scheduledExecutorService, int checkPeriod, Target target, List<TargetProbe> targetProbes) {
      super(scheduledExecutorService, 0, checkPeriod, TimeUnit.MILLISECONDS, false);

      this.target = target;
      this.targetProbes = targetProbes;
   }

   @Override
   public synchronized void start() {
      target.setListener(this);

      super.start();
   }

   @Override
   public synchronized void stop() {
      super.stop();

      targetReady = false;

      target.setListener(null);

      try {
         target.disconnect();
      } catch (Exception e) {
         logger.debug("Error on disconnecting target " + target, e);
      }
   }

   @Override
   public void run() {
      try {
         if (!target.isConnected()) {
            if (logger.isDebugEnabled()) {
               logger.debug("Connecting to " + target);
            }

            target.connect();
         }

         targetReady = target.checkReadiness() &&  checkTargetProbes();

         if (logger.isDebugEnabled()) {
            if (targetReady) {
               logger.debug(target + " is ready");
            } else {
               logger.debug(target + " is not ready");
            }
         }
      } catch (Exception e) {
         logger.warn("Error monitoring " + target, e);

         targetReady = false;
      }
   }

   private boolean checkTargetProbes() {
      for (TargetProbe targetProbe : targetProbes) {
         if (!targetProbe.check(target)) {
            logger.info(targetProbe.getName() + " has failed on " + target);
            return false;
         }
      }

      return true;
   }

   @Override
   public void targetConnected() {

   }

   @Override
   public void targetDisconnected() {
      targetReady = false;
   }


   @Override
   public String toString() {
      return this.getClass().getSimpleName() + " [target=" + target + ", targetReady=" + targetReady + "]";
   }
}
