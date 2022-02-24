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
package org.apache.activemq.artemis.cli.commands.messages.perf;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.netty.util.concurrent.OrderedEventExecutor;
import org.HdrHistogram.SingleWriterRecorder;

public final class ProducerTargetRateLoadGenerator extends SkeletalProducerLoadGenerator {

   private final long usPeriod;
   private long fireTimeMicros;
   private boolean started;

   public ProducerTargetRateLoadGenerator(final AsyncJms2ProducerFacade producer,
                                          final OrderedEventExecutor executor,
                                          final MicrosTimeProvider timeProvider,
                                          final BooleanSupplier keepOnSending,
                                          final long nsPeriod,
                                          final String group,
                                          final byte[] msgContent,
                                          final SingleWriterRecorder sendCompletedLatencies,
                                          final SingleWriterRecorder waitLatencies) {
      super(producer, executor, timeProvider, keepOnSending, group, msgContent, sendCompletedLatencies, waitLatencies);
      this.fireTimeMicros = 0;
      this.usPeriod = TimeUnit.NANOSECONDS.toMicros(nsPeriod);
      this.started = false;
   }

   @Override
   public void run() {
      if (closed || stopLoad) {
         return;
      }
      final long now = timeProvider.now();
      if (!started) {
         started = true;
         fireTimeMicros = now;
      }
      if (!trySend(fireTimeMicros, now)) {
         return;
      }
      if (!keepOnSending.getAsBoolean()) {
         producer.requestClose();
         stopLoad = true;
         return;
      }
      fireTimeMicros += usPeriod;
      final long delay = fireTimeMicros - timeProvider.now();
      final long usToNextFireTime = Math.max(0, delay);
      asyncContinue(usToNextFireTime);
   }
}
