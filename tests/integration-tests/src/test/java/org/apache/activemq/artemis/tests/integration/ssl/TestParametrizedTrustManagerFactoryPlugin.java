/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.ssl;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.activemq.artemis.api.core.TrustManagerFactoryPlugin;

import javax.net.ssl.TrustManagerFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TestParametrizedTrustManagerFactoryPlugin implements TrustManagerFactoryPlugin {

   public static AtomicBoolean wasInvokedWithParameters = new AtomicBoolean(false);
   public static AtomicBoolean wasInvokedWithoutParameters = new AtomicBoolean(false);
   public static AtomicReference<Parameters> receivedParameters = new AtomicReference<>(null);

   public static void reset() {
      wasInvokedWithParameters.set(false);
      wasInvokedWithoutParameters.set(false);
      receivedParameters.set(null);
   }

   @Override
   public TrustManagerFactory getTrustManagerFactory() {
      wasInvokedWithoutParameters.set(true);
      return InsecureTrustManagerFactory.INSTANCE;
   }

   @Override
   public TrustManagerFactory getTrustManagerFactory(Parameters parameters) {
      wasInvokedWithParameters.set(true);
      receivedParameters.set(parameters);
      return InsecureTrustManagerFactory.INSTANCE;
   }
}
