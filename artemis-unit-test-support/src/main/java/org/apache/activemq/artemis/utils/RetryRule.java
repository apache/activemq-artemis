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

package org.apache.activemq.artemis.utils;

import org.jboss.logging.Logger;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * Please use this only if you have to.
 * Try to fix the test first.
 */
public class RetryRule implements MethodRule {

   public static final String PROPERTY_NAME = "org.apache.activemq.artemis.utils.RetryRule.retry";

   private static Logger logger = Logger.getLogger(RetryRule.class);

   final int defaultNumberOfRetries;
   final boolean retrySuccess;

   public RetryRule() {
      this(0);
   }

   public RetryRule(int defaultNumberOfRetries) {
      this.defaultNumberOfRetries = defaultNumberOfRetries;
      this.retrySuccess = false;
   }

   /**
    *
    * @param defaultNumberOfRetries
    * @param retrySuccess if true, it will keep retrying until it failed or the number of retries have reached an end, opposite rule.
    */
   public RetryRule(int defaultNumberOfRetries, boolean retrySuccess) {
      this.defaultNumberOfRetries = defaultNumberOfRetries;
      this.retrySuccess = retrySuccess;
   }

   private int getNumberOfRetries(final FrameworkMethod method) {

      if (!Boolean.parseBoolean(System.getProperty(PROPERTY_NAME))) {
         return 0;
      }
      RetryMethod retry = method.getAnnotation(RetryMethod.class);
      if (retry != null) {
         return retry.retries();
      } else {
         return defaultNumberOfRetries;
      }
   }

   @Override
   public Statement apply(final Statement base, final FrameworkMethod method, Object target) {
      if (retrySuccess) {
         return retrySuccess(base, method, target);
      } else {
         return retryIfFailed(base, method, target);
      }
   }

   protected Statement retrySuccess(Statement base, FrameworkMethod method, Object target) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            int retries = getNumberOfRetries(method) + 1;
            for (int i = 0; i < retries; i++) {
               logger.warn("LOOP " + i);
               base.evaluate();
            }
         }
      };
   }


   protected Statement retryIfFailed(Statement base, FrameworkMethod method, Object target) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            Throwable currentException = null;
            try {
               base.evaluate();
            } catch (Throwable t) {

               if (t instanceof AssumptionViolatedException) {
                  throw t;
               }

               currentException = t;

               int retries = getNumberOfRetries(method);

               for (int retryNr = 0; retryNr < retries; retryNr++) {
                  logger.warn("RETRY " + (retryNr + 1) + " of " + retries + " on " + target.getClass() + "::" + method.getName(), currentException);
                  currentException = null;
                  try {
                     base.evaluate();
                     logger.warn("RETRY " + (retryNr + 1)  + " of " + retries + " on " + target.getClass() + "::" + method.getName() + " succeeded");
                     break;
                  } catch (Throwable t2) {
                     logger.warn("RETRY " + (retryNr + 1)  + " of " + retries + " on " + target.getClass() + "::" + method.getName() + " failed ", t2);
                     currentException = t2;
                  }
               }
               if (currentException != null) {
                  throw currentException;
               }
            }
         }
      };
   }
}