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

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.utils.JsonLoader;

public final class LiveStatistics {

   public enum ReportInterval {
      ns(1),
      us(TimeUnit.MICROSECONDS.toNanos(1)),
      ms(TimeUnit.MILLISECONDS.toNanos(1)),
      sec(TimeUnit.SECONDS.toNanos(1));

      public final long nanoseconds;

      ReportInterval(long value) {
         this.nanoseconds = value;
      }
   }

   private final ProducerLoadGenerator[] producers;
   private final Histogram[] waitLatencies;
   private final Histogram intervalWaitLatencies;
   private final Histogram[] sentLatencies;
   private final Histogram intervalSentLatencies;
   private final RecordingMessageListener[] listeners;
   private final Histogram[] endToEndLatencies;
   private final Histogram intervalEndToEndLatencies;
   private final RateSampler sentMsg;
   private final RateSampler blockedMsg;
   private final RateSampler completedMsg;
   private final RateSampler receivedMsg;
   private final Histogram accumulatedWaitLatencies;
   private final Histogram accumulatedSentLatencies;
   private final Histogram accumulatedEndToEndLatencies;
   private final DecimalFormat jsonLatencyFormat;
   private final PaddingDecimalFormat latencyFormat;
   private final PaddingDecimalFormat rateFormat;
   private final PaddingDecimalFormat countFormat;
   private long sampleTime;
   private final FileWriter jsonWriter;
   private long jsonSamples;
   private final HistogramLogWriter latenciesLogWriter;

   public LiveStatistics(final String jsonOutput, final String hdrOutput, final ProducerLoadGenerator[] producers, final RecordingMessageListener[] listeners) throws IOException {
      if (producers != null && producers.length > 0) {
         this.producers = producers;
         sentLatencies = new Histogram[producers.length];
         intervalSentLatencies = new Histogram(2);
         accumulatedSentLatencies = new Histogram(2);
         if (producers[0] instanceof ProducerTargetRateLoadGenerator) {
            waitLatencies = new Histogram[producers.length];
            intervalWaitLatencies = new Histogram(2);
            accumulatedWaitLatencies = new Histogram(2);
         } else {
            waitLatencies = null;
            intervalWaitLatencies = null;
            accumulatedWaitLatencies = null;
         }
         sentMsg = RateSampler.of(() -> {
            long sum = 0;
            for (ProducerLoadGenerator producer : producers) {
               sum += producer.getProducer().getMessageSent();
            }
            return sum;
         });
         blockedMsg = RateSampler.of(() -> {
            long sum = 0;
            for (ProducerLoadGenerator producer : producers) {
               sum += producer.getProducer().getNotAvailable();
            }
            return sum;
         });
         completedMsg = RateSampler.of(() -> {
            long sum = 0;
            for (ProducerLoadGenerator producer : producers) {
               sum += producer.getProducer().getMessageCompleted();
            }
            return sum;
         });
      } else {
         this.producers = null;
         sentLatencies = null;
         intervalSentLatencies = null;
         accumulatedSentLatencies = null;
         waitLatencies = null;
         intervalWaitLatencies = null;
         accumulatedWaitLatencies = null;
         sentMsg = null;
         blockedMsg = null;
         completedMsg = null;
      }
      if (listeners != null) {
         this.listeners = listeners;
         endToEndLatencies = new Histogram[listeners.length];
         intervalEndToEndLatencies = new Histogram(2);
         accumulatedEndToEndLatencies = new Histogram(2);
         receivedMsg = RateSampler.of(() -> {
            long sum = 0;
            for (RecordingMessageListener listener : listeners) {
               sum += listener.getReceivedMessages();
            }
            return sum;
         });
      } else {
         this.listeners = null;
         endToEndLatencies = null;
         intervalEndToEndLatencies = null;
         accumulatedEndToEndLatencies = null;
         receivedMsg = null;
      }
      this.sampleTime = System.currentTimeMillis();
      this.jsonLatencyFormat = new DecimalFormat("0.00");
      this.latencyFormat = new PaddingDecimalFormat("0.00", 9);
      this.rateFormat = new PaddingDecimalFormat("0", 8);
      this.countFormat = new PaddingDecimalFormat("0", 12);
      this.jsonSamples = 0;
      if (jsonOutput != null) {
         this.jsonWriter = new FileWriter(jsonOutput);
         this.jsonWriter.write("[\n");
      } else {
         this.jsonWriter = null;
      }
      if (hdrOutput != null) {
         this.latenciesLogWriter = new HistogramLogWriter(hdrOutput);
         this.latenciesLogWriter.outputLogFormatVersion();
         this.latenciesLogWriter.outputLegend();
      } else {
         this.latenciesLogWriter = null;
      }
   }

   private boolean anyFatalError() {
      if (producers != null) {
         for (ProducerLoadGenerator producer : producers) {
            if (producer.getFatalException() != null) {
               return true;
            }
         }
      }
      if (listeners != null) {
         for (RecordingMessageListener listener : listeners) {
            if (listener.anyFatalException()) {
               return true;
            }
         }
      }
      return false;
   }

   public void sampleMetrics(final boolean warmup) {
      final long lastSampleTime = this.sampleTime;
      sampleTime = System.currentTimeMillis();
      if (receivedMsg != null) {
         receivedMsg.run();
      }
      if (completedMsg != null) {
         completedMsg.run();
      }
      if (blockedMsg != null) {
         blockedMsg.run();
      }
      if (sentMsg != null) {
         sentMsg.run();
      }
      if (endToEndLatencies != null) {
         for (int i = 0, size = listeners.length; i < size; i++) {
            endToEndLatencies[i] = listeners[i].getReceiveLatencyRecorder().getIntervalHistogram(endToEndLatencies[i]);
         }
      }
      if (sentLatencies != null) {
         for (int i = 0, size = producers.length; i < size; i++) {
            sentLatencies[i] = producers[i].getSendCompletedLatencies().getIntervalHistogram(sentLatencies[i]);
         }
      }
      if (waitLatencies != null) {
         for (int i = 0, size = producers.length; i < size; i++) {
            waitLatencies[i] = producers[i].getWaitLatencies().getIntervalHistogram(waitLatencies[i]);
         }
      }
      aggregateLatencies(warmup, lastSampleTime, sampleTime, intervalEndToEndLatencies, endToEndLatencies, accumulatedEndToEndLatencies);
      aggregateLatencies(warmup, lastSampleTime, sampleTime, intervalSentLatencies, sentLatencies, accumulatedSentLatencies);
      aggregateLatencies(warmup, lastSampleTime, sampleTime, intervalWaitLatencies, waitLatencies, accumulatedWaitLatencies);
   }

   private static void aggregateLatencies(final boolean warmup,
                                          final long lastSampleTime,
                                          final long sampleTime,
                                          final Histogram intervalSentLatencies,
                                          final Histogram[] sentLatencies,
                                          final Histogram accumulatedSentLatencies) {
      if (intervalSentLatencies != null) {
         intervalSentLatencies.reset();
         intervalSentLatencies.setStartTimeStamp(lastSampleTime);
         intervalSentLatencies.setEndTimeStamp(sampleTime);
         for (Histogram histogram : sentLatencies) {
            intervalSentLatencies.add(histogram);
         }
         if (!warmup) {
            accumulatedSentLatencies.add(intervalSentLatencies);
         }
      }
   }

   public void outAtInterval(final boolean warmup, final StringBuilder out, final ReportInterval interval, final boolean includeLatencies) throws IOException {
      // space after "true" is to ensure length to be the same as "false": don't remove it!
      out.append("\n--- warmup ").append(warmup ? "true " : "false");
      appendRateOf(out, "\n--- sent:     ", sentMsg, rateFormat, interval, "msg");
      appendRateOf(out, "\n--- blocked:  ", blockedMsg, rateFormat, interval, "msg");
      appendRateOf(out, "\n--- completed:", completedMsg, rateFormat, interval, "msg");
      appendRateOf(out, "\n--- received: ", receivedMsg, rateFormat, interval, "msg");
      if (includeLatencies) {
         outPercentiles(out, "\n--- send delay time:", intervalWaitLatencies, latencyFormat);
         outPercentiles(out, "\n--- send ack time:  ", intervalSentLatencies, latencyFormat);
         outPercentiles(out, "\n--- transfer time:  ", intervalEndToEndLatencies, latencyFormat);
      }
      appendJsonIntervalSampleOnFile(warmup, interval);
      appendTaggedHdrHistograms(warmup);
   }

   private void appendJsonIntervalSampleOnFile(final boolean warmup, final ReportInterval interval) throws IOException {
      if (jsonWriter == null) {
         return;
      }
      final JsonObjectBuilder jsonBuilder = JsonLoader.createObjectBuilder();
      jsonBuilder.add("sampleType", "interval");
      jsonBuilder.add("warmup", warmup);
      jsonBuilder.add("time", sampleTime);
      addRate(jsonBuilder, "sent", sentMsg, interval);
      addRate(jsonBuilder, "delayed", blockedMsg, interval);
      addRate(jsonBuilder, "completed", completedMsg, interval);
      addRate(jsonBuilder, "received", receivedMsg, interval);
      addPercentiles(jsonBuilder, "delaySendTime", intervalWaitLatencies);
      addPercentiles(jsonBuilder, "sendTime", intervalSentLatencies);
      addPercentiles(jsonBuilder, "transferTime", intervalEndToEndLatencies);
      final String jsonSample = jsonBuilder.build().toString();
      if (jsonSamples > 0) {
         jsonWriter.write(",\n");
      }
      jsonSamples++;
      jsonWriter.write(jsonSample);
      jsonWriter.flush();
   }

   private void appendJsonSummarySampleOnFile(final boolean failedBenchmark) throws IOException {
      if (jsonWriter == null) {
         return;
      }
      final JsonObjectBuilder jsonBuilder = JsonLoader.createObjectBuilder();
      jsonBuilder.add("sampleType", "summary");
      jsonBuilder.add("time", sampleTime);
      jsonBuilder.add("result", failedBenchmark ? "fail" : "success");
      if (sentMsg != null) {
         jsonBuilder.add("totalSent", sentMsg.getLastSample());
      }
      if (blockedMsg != null) {
         jsonBuilder.add("totalBlocked", blockedMsg.getLastSample());
      }
      if (completedMsg != null) {
         jsonBuilder.add("totalCompleted", completedMsg.getLastSample());
      }
      if (receivedMsg != null) {
         jsonBuilder.add("totalReceived", receivedMsg.getLastSample());
      }
      addPercentiles(jsonBuilder, "totalDelaySendTime", accumulatedWaitLatencies);
      addPercentiles(jsonBuilder, "totalSendTime", accumulatedSentLatencies);
      addPercentiles(jsonBuilder, "totalTransferTime", accumulatedEndToEndLatencies);
      final String jsonSample = jsonBuilder.build().toString();
      if (jsonSamples > 0) {
         jsonWriter.write(",\n");
      }
      jsonSamples++;
      jsonWriter.write(jsonSample);
      jsonWriter.flush();
   }

   private void appendTaggedHdrHistograms(final boolean warmup) {
      if (latenciesLogWriter == null) {
         return;
      }
      if (intervalWaitLatencies != null) {
         intervalWaitLatencies.setTag(warmup ? "warmup delay send" : "delay send");
         latenciesLogWriter.outputIntervalHistogram(intervalWaitLatencies);
      }
      if (intervalSentLatencies != null) {
         intervalSentLatencies.setTag(warmup ? "warmup send" : "send");
         latenciesLogWriter.outputIntervalHistogram(intervalSentLatencies);
      }
      if (intervalEndToEndLatencies != null) {
         intervalEndToEndLatencies.setTag(warmup ? "warmup transfer" : "transfer");
         latenciesLogWriter.outputIntervalHistogram(intervalEndToEndLatencies);
      }
   }

   private static JsonObjectBuilder addRate(final JsonObjectBuilder obj,
                                            final String metric,
                                            final RateSampler rate,
                                            final ReportInterval interval) {
      if (rate == null) {
         return obj;
      }
      return obj.add(metric, rate.reportRate(interval.nanoseconds));
   }

   private JsonObjectBuilder addPercentiles(final JsonObjectBuilder obj,
                                                   final String metric,
                                                   final Histogram distribution) {
      if (distribution == null) {
         return obj;
      }
      return obj
         .add(metric, JsonLoader.createObjectBuilder()
         .add("mean", jsonLatencyFormat.format(distribution.getMean()))
         .add("50", distribution.getValueAtPercentile(50.0d))
         .add("90", distribution.getValueAtPercentile(90.0d))
         .add("99", distribution.getValueAtPercentile(99.0d))
         .add("99.9", distribution.getValueAtPercentile(99.9d))
         .add("99.99", distribution.getValueAtPercentile(99.99d))
         .add("max", distribution.getMaxValue())
         .add("count", distribution.getTotalCount()));
   }

   public void outSummary(final StringBuilder out) throws IOException {
      out.append("\n--- SUMMARY");
      final boolean failedBenchmark = anyFatalError();
      out.append("\n--- result:         ").append(failedBenchmark ? "        fail" : "     success");
      if (sentMsg != null) {
         out.append("\n--- total sent:     ").append(countFormat.format(sentMsg.getLastSample()));
      }
      if (blockedMsg != null) {
         out.append("\n--- total blocked:  ").append(countFormat.format(blockedMsg.getLastSample()));
      }
      if (completedMsg != null) {
         out.append("\n--- total completed:").append(countFormat.format(completedMsg.getLastSample()));
      }
      if (receivedMsg != null) {
         out.append("\n--- total received: ").append(countFormat.format(receivedMsg.getLastSample()));
      }
      outPercentiles(out, "\n--- aggregated delay send time:", accumulatedWaitLatencies, latencyFormat);
      outPercentiles(out, "\n--- aggregated send time:      ", accumulatedSentLatencies, latencyFormat);
      outPercentiles(out, "\n--- aggregated transfer time:  ", accumulatedEndToEndLatencies, latencyFormat);
      appendJsonSummarySampleOnFile(failedBenchmark);
   }

   public void close() throws IOException {
      if (jsonWriter != null) {
         jsonWriter.write("\n]");
         jsonWriter.close();
      }
      if (latenciesLogWriter != null) {
         latenciesLogWriter.close();
      }
   }

   private static void outPercentiles(final StringBuilder out,
                                      final String metric,
                                      final Histogram histogram,
                                      final DecimalFormat latencyFormat) {
      if (histogram == null) {
         return;
      }
      out.append(' ').append(metric);
      out.append(' ').append("mean: ").append(latencyFormat.format(histogram.getMean())).append(" us");
      out.append(" - ").append("50.00%: ").append(latencyFormat.format(histogram.getValueAtPercentile(50.0d))).append(" us");
      out.append(" - ").append("90.00%: ").append(latencyFormat.format(histogram.getValueAtPercentile(90.0d))).append(" us");
      out.append(" - ").append("99.00%: ").append(latencyFormat.format(histogram.getValueAtPercentile(99.0d))).append(" us");
      out.append(" - ").append("99.90%: ").append(latencyFormat.format(histogram.getValueAtPercentile(99.9d))).append(" us");
      out.append(" - ").append("99.99%: ").append(latencyFormat.format(histogram.getValueAtPercentile(99.99d))).append(" us");
      out.append(" - ").append("max:    ").append(latencyFormat.format(histogram.getMaxValue())).append(" us");
   }

   private static StringBuilder appendRateOf(final StringBuilder out,
                                             final String metric,
                                             final RateSampler sampler,
                                             final DecimalFormat rateFormat,
                                             final ReportInterval outInterval,
                                             final String unit) {
      if (sampler == null) {
         return out;
      }
      return out.append(' ').append(metric)
         .append(' ').append(rateFormat.format(sampler.reportRate(outInterval.nanoseconds)))
         .append(' ').append(unit).append('/').append(outInterval.name());
   }
}
