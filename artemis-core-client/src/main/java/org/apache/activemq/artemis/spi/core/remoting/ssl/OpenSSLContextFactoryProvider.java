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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Provider that loads the {@link OpenSSLContextFactory} services and return the one with the highest priority.
 */
public class OpenSSLContextFactoryProvider {
   private static final OpenSSLContextFactory factory;
   static {
      final ServiceLoader<OpenSSLContextFactory> loader = ServiceLoader.load(OpenSSLContextFactory.class, Thread.currentThread().getContextClassLoader());
      final List<OpenSSLContextFactory> factories = new ArrayList<>();
      loader.forEach(factories::add);
      if (factories.isEmpty())
         throw new IllegalStateException("No OpenSSLContextFactory registered!");
      Collections.sort(factories);
      factory = factories.get(factories.size() - 1);
   }

   /**
    * @return the {@link OpenSSLContextFactory} with the higher priority.
    */
   public static OpenSSLContextFactory getOpenSSLContextFactory() {
      return factory;
   }
}
