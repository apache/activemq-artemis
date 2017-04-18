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

package org.apache.activemq.artemis.core.remoting.impl.netty;

import org.apache.activemq.artemis.utils.Env;
import org.jboss.logging.Logger;

/**
 * Tells if <a href="http://netty.io/wiki/native-transports.html">{@code netty-transport-native-epoll}</a> is supported.
 */
public final class Epoll {

   private static final Logger logger = Logger.getLogger(Epoll.class);
   private static final boolean IS_AVAILABLE_EPOLL = isIsAvailableEpoll();

   private static boolean isIsAvailableEpoll() {
      try {
         if (Env.is64BitJvm() && Env.isLinuxOs()) {
            return io.netty.channel.epoll.Epoll.isAvailable();
         } else {
            return false;
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         return false;
      }

   }

   private Epoll() {

   }

   public static boolean isAvailable() {
      return IS_AVAILABLE_EPOLL;
   }
}
