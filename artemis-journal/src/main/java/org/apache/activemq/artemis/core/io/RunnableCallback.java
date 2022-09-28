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

package org.apache.activemq.artemis.core.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunnableCallback implements IOCallback {
   private static final Logger logger = LoggerFactory.getLogger(RunnableCallback.class);

   Runnable okCallback;
   Runnable errorCallback;

   public RunnableCallback(Runnable ok, Runnable error) {
      if (ok == null) {
         throw new NullPointerException("ok = null");
      }
      if (ok == null) {
         throw new NullPointerException("error = null");
      }
      okCallback = ok;
      errorCallback = error;
   }

   public RunnableCallback(Runnable ok) {
      if (ok == null) {
         throw new NullPointerException("ok = null");
      }
      okCallback = ok;
      errorCallback = ok;
   }

   @Override
   public void done() {
      try {
         okCallback.run();
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public void onError(int errorCode, String errorMessage) {
      try {
         errorCallback.run();
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }
}
