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

@Command(name = "client", description = "It will produce and consume messages to a broker instance")
public class PerfClientCommand extends PerfCommand {

   @Option(name = "--tx", description = "Perform Message::acknowledge per each message received (Default: disabled)")
   protected boolean transaction;

   @Option(name = "--shared", description = "Create shared subscription (Default: 0)")
   protected int sharedSubscription = 0;

   @Option(name = "--durable", description = "Enabled durable subscription (Default: disabled)")
   protected boolean durableSubscription = false;

   @Option(name = "--consumer-connections", description = "Number of consumer connections to be used. Default is same as the total number of consumers")
   protected int consumerConnections = 0;

   @Option(name = "--consumers", description = "Number of consumer to use for each generated destination (Default: 1)")
   protected int consumersPerDestination = 1;

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

   @Option(name = "--consumer-url", description = "Setup the url used for MessageListener(s) connections. Default is same as --url")
   protected String consumerUrl = null;

   @Option(name = "--consumer-protocol", description = "Setup the protocol used for MessageListener(s) connections. Default is same as --protocol")
   protected String consumerProtocol = null;

   @Option(name = "--enable-msg-id", description = "Enable setting JMS messageID per-message (Default: disabled)")
   protected boolean enableMessageID;

   @Option(name = "--enable-timestamp", description = "Enable setting JMS timestamp per-message (Default: disabled)")
   protected boolean enableTimestamp;

   private volatile BenchmarkService producerBenchmark;

   @Override
   protected void onExecuteBenchmark(final ConnectionFactory producerConnectionFactory, final Destination[] jmsDestinations, final ActionContext context) throws Exception {
      final String listenerProtocol = this.consumerProtocol != null ? this.consumerProtocol : getProtocol();
      final String listenerUrl = this.consumerUrl != null ? this.consumerUrl : brokerURL;
      final ConnectionFactory consumerConnectionFactory = createConnectionFactory(listenerUrl, user, password, null, listenerProtocol);
      if (consumerConnections == 0) {
         if (sharedSubscription > 0) {
            if (getClientID() == null) {
               consumerConnections = sharedSubscription * consumersPerDestination * jmsDestinations.length;
            } else {
               consumerConnections = sharedSubscription * jmsDestinations.length;
            }
         } else {
            consumerConnections = consumersPerDestination * jmsDestinations.length;
         }
      }
      final int totalProducers = producersPerDestination * jmsDestinations.length;

      if (threads >= totalProducers) {
         if (threads > totalProducers) {
            context.err.println("Doesn't make sense to set workers > producers: auto-adjusting it to be the same as the producer count");
            threads = totalProducers;
         }
      }

      boolean warmingUp = warmup != 0;
      final LiveStatistics statistics;
      final StringBuilder skratchBuffer = new StringBuilder();
      try (MessageListenerBenchmark consumerBenchmark = new MessageListenerBenchmarkBuilder()
         .setClientID(getClientID())
         .setDestinations(consumerProtocol != null ? lookupDestinations(consumerConnectionFactory) : jmsDestinations)
         .setFactory(consumerConnectionFactory)
         .setTransacted(transaction)
         .setConsumers(consumersPerDestination)
         .setConnections(consumerConnections)
         .setTimeProvider(() -> TimeUnit.NANOSECONDS.toMicros(System.nanoTime()))
         .setCanDelayMessageCount(true)
         .setSharedSubscription(sharedSubscription)
         .setDurableSubscription(durableSubscription)
            .createMessageListenerBenchmark()) {

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
         try (ProducerBenchmark producerBenchmark = new ProducerBenchmarkBuilder()
            .setPersistent(persistent)
            .setDestinations(jmsDestinations)
            .setFactory(producerConnectionFactory)
            .setTtl(ttl)
            .setTransactionCapacity(txSize)
            .setGroup(msgGroupID)
            .setProducers(producersPerDestination)
            .setMessageRate(rate)
            .setMessageCount(messageCount)
            .setMessageSize(messageSize)
            .setTimeProvider(() -> TimeUnit.NANOSECONDS.toMicros(System.nanoTime()))
            .setLoopGroup(eventLoopGroup)
            .setMaxPending(maxPending)
            .setSharedConnections(sharedConnections)
            .setEnableMessageID(enableMessageID)
            .setEnableTimestamp(enableTimestamp)
               .createProducerBenchmark()) {
            this.producerBenchmark = producerBenchmark;
            consumerBenchmark.start();
            producerBenchmark.start();
            final long now = System.currentTimeMillis();
            final long endWarmup = warmup > 0 ? now + TimeUnit.SECONDS.toMillis(warmup) : 0;
            final long end = duration > 0 ? now + TimeUnit.SECONDS.toMillis(duration) : 0;
            statistics = new LiveStatistics(reportFileName, hdrFileName, producerBenchmark.getGenerators(), consumerBenchmark.getListeners());
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            warmingUp = collectAndReportStatisticsWhileRunning(warmingUp, statistics, skratchBuffer, endWarmup, end, producerBenchmark);
            final boolean producerFatalError = producerBenchmark.anyError();
            producerBenchmark.asyncClose();
            if (!producerFatalError) {
               consumerBenchmark.setMessageCount(producerBenchmark.expectedTotalMessageCountToReceive(sharedSubscription, consumersPerDestination));
               // we don't care about duration here, but just on the expected messages to receive
               warmingUp = collectAndReportStatisticsWhileRunning(warmingUp, statistics, skratchBuffer, endWarmup, 0, consumerBenchmark);
            }
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
   }

   @Override
   protected void onInterruptBenchmark() {
      final BenchmarkService benchmark = this.producerBenchmark;
      if (benchmark != null) {
         benchmark.close();
      }
   }
}
