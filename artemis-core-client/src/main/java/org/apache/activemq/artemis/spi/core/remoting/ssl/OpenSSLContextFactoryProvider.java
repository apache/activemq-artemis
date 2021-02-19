/*
 * Copyright 2021 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.spi.core.remoting.ssl;

import java.util.ServiceLoader;

/**
 * Provider that loads all registered {@link OpenSSLContextFactory} services and returns the one with the highest priority.
 */
public class OpenSSLContextFactoryProvider {

   private static final OpenSSLContextFactory FACTORY;
   static {
      OpenSSLContextFactory factoryWithHighestPrio = null;
      for (OpenSSLContextFactory factory : ServiceLoader.load(OpenSSLContextFactory.class)) {
         if (factoryWithHighestPrio == null || factory.getPriority() > factoryWithHighestPrio.getPriority()) {
            factoryWithHighestPrio = factory;
         }
      }

      if (factoryWithHighestPrio == null)
         throw new IllegalStateException("No OpenSSLContextFactory registered!");

      FACTORY = factoryWithHighestPrio;
   }

   /**
    * @return the {@link OpenSSLContextFactory} with the higher priority.
    */
   public static OpenSSLContextFactory getOpenSSLContextFactory() {
      return FACTORY;
   }
}
