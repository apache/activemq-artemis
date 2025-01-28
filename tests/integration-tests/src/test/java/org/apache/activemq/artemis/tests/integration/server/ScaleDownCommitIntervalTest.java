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
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;

/**
 * This test doesn't specifically verify that the underlying transactions are committed at the specified interval as
 * there's no straight-forward way to verify this. Instead the test simply repeats the behavior from
 * {@link ScaleDownTest} with a commit interval of 1 to ensure that even if the transaction is commit <b>every time</b>
 * it still works as expected.
 */
public class ScaleDownCommitIntervalTest extends ScaleDownTest {

   @Override
   protected ScaleDownConfiguration getScaleDownConfiguration() {
      return new ScaleDownConfiguration().setCommitInterval(1);
   }
}
