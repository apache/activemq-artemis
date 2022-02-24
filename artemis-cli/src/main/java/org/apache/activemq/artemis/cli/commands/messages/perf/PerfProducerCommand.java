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

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import org.apache.activemq.artemis.cli.commands.ActionContext;

@Command(name = "producer", description = "It will send messages to a broker instance")
public class PerfProducerCommand extends PerfCommand {
   @Option(name = "--persistent", description = "It will send messages persistently. Default is non persistent")
   protected boolean persistent = false;

   @Option(name = "--message-size", description = "Size of each byteMessage (Default is 1024)")
   protected int messageSize = 1024;

   @Option(name = "--rate", description = "Expected total message rate. (Default is unbounded)")
   protected Long rate = null;

   @Option(name = "--ttl", description = "TTL for each message")
   protected long ttl = 0L;

   @Option(name = "--group", description = "Message Group to be used")
   protected String msgGroupID = null;

   @Option(name = "--shared-connections", description = "It create --threads shared connections among producers (Default: not shared)")
   protected boolean sharedConnections = false;

   @Option(name = "--tx-size", description = "TX Size")
   protected long txSize;

   @Option(name = "--producers", description = "Number of producers to use for each generated destination (Default: 1)")
   protected int producersPerDestination = 1;

   @Option(name = "--threads", description = "Number of worker threads to schedule producer load tasks (Default: 1)")
   protected int threads = 1;

   @Option(name = "--max-pending", description = "How many not yet completed messages can exists  (Default is 1)")
   protected long maxPending = 1;

   @Option(name = "--enable-msg-id", description = "Enable setting JMS messageID per-message (Default: disabled)")
   protected boolean enableMessageID;

   @Option(name = "--enable-timestamp", description = "Enable setting JMS timestamp per-message (Default: disabled)")
   protected boolean enableTimestamp;

   protected volatile BenchmarkService benchmark;

   @Override
   protected void onExecuteBenchmark(final ConnectionFactory factory,
                                     final Destination[] jmsDestinations,
                                     final ActionContext context) throws Exception {
      if (getClientID() != null) {
         context.err.println("ClientID configuration is not supported");
      }
      MicrosTimeProvider timeProvider = () -> TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
      if (MicrosClock.isAvailable()) {
         timeProvider = MicrosClock::now;
      } else {
         context.err.println("Microseconds wall-clock time not available: using System::currentTimeMillis. Add --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED to the JVM parameters to enable it.");
      }

      final int totalProducers = producersPerDestination * jmsDestinations.length;

      if (threads >= totalProducers) {
         if (threads > totalProducers) {
            context.err.println("Doesn't make sense to set workers > producers: auto-adjusting it to be the same as the producer count");
            threads = totalProducers;
         }
      }

      final DefaultEventLoopGroup eventLoopGroup = new DefaultEventLoopGroup(threads) {
         @Override
         protected EventLoop newChild(final Executor executor, final Object... args) {
            return new DefaultEventLoop(this, executor) {
               @Override
               protected Queue<Runnable> newTaskQueue(final int maxPendingTasks) {
                  return new LinkedTransferQueue<>();
               }
            };
         }
      };
      boolean warmingUp = warmup != 0;
      final LiveStatistics statistics;
      final StringBuilder skratchBuffer = new StringBuilder();
      try (ProducerBenchmark benchmark = new ProducerBenchmarkBuilder()
         .setPersistent(persistent)
         .setDestinations(jmsDestinations)
         .setFactory(factory)
         .setTtl(ttl)
         .setTransactionCapacity(txSize)
         .setGroup(msgGroupID)
         .setProducers(producersPerDestination)
         .setMessageRate(rate)
         .setMessageCount(messageCount)
         .setMessageSize(messageSize)
         .setTimeProvider(timeProvider)
         .setLoopGroup(eventLoopGroup)
         .setMaxPending(maxPending)
         .setSharedConnections(sharedConnections)
         .setEnableMessageID(enableMessageID)
         .setEnableTimestamp(enableTimestamp)
            .createProducerBenchmark()) {
         this.benchmark = benchmark;
         benchmark.start();
         final long now = System.currentTimeMillis();
         final long endWarmup = warmup > 0 ? now + TimeUnit.SECONDS.toMillis(warmup) : 0;
         final long end = duration > 0 ? now + TimeUnit.SECONDS.toMillis(duration) : 0;
         statistics = new LiveStatistics(reportFileName, hdrFileName, benchmark.getGenerators(), null);
         LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
         warmingUp = collectAndReportStatisticsWhileRunning(warmingUp, statistics, skratchBuffer, endWarmup, end, benchmark);
      }
      // last sample must be collected while the whole benchmark is complete
      statistics.sampleMetrics(warmingUp);
      skratchBuffer.setLength(0);
      statistics.outSummary(skratchBuffer);
      if (!isSilentInput()) {
         context.out.println(skratchBuffer);
      }
      eventLoopGroup.shutdownGracefully();
      statistics.close();
   }

   @Override
   protected void onInterruptBenchmark() {
      final BenchmarkService benchmark = this.benchmark;
      if (benchmark != null) {
         benchmark.close();
      }
   }
}
