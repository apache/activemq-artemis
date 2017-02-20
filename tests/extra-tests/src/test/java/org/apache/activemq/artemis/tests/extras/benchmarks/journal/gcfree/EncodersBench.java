/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.extras.benchmarks.journal.gcfree;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.journal.EncoderPersister;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalAddRecord;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalInternalRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(value = {Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class EncodersBench {

   private static final int expectedEncoderSize = JournalRecordHeader.BYTES + AddJournalRecordEncoder.expectedSize(0);
   private JournalInternalRecord record;
   private ByteBuffer byteBuffer;
   private AddJournalRecordEncoder addJournalRecordEncoder;
   private ActiveMQBuffer outBuffer;

   public static void main(String[] args) throws RunnerException {
      final Options opt = new OptionsBuilder().include(EncodersBench.class.getSimpleName()).addProfiler(GCProfiler.class).warmupIterations(5).measurementIterations(5).forks(1).build();
      new Runner(opt).run();
   }

   @Setup
   public void init() {
      this.byteBuffer = ByteBuffer.allocateDirect(expectedEncoderSize);
      this.byteBuffer.order(ByteOrder.nativeOrder());
      this.addJournalRecordEncoder = new AddJournalRecordEncoder();

      this.record = new JournalAddRecord(true, 1, (byte) 1, EncoderPersister.getInstance(), ZeroEncodingSupport.Instance);
      this.record.setFileID(1);
      this.record.setCompactCount((short) 1);
      this.outBuffer = new ChannelBufferWrapper(Unpooled.directBuffer(this.record.getEncodeSize(), this.record.getEncodeSize()).order(ByteOrder.nativeOrder()));
   }

   @Benchmark
   public int encodeAligned() {
      //Header
      final long header = JournalRecordHeader.makeHeader(JournalRecordTypes.ADD_JOURNAL, expectedEncoderSize);
      this.byteBuffer.putLong(0, header);
      //FileId<CompactCount<Id<RecordType<RecordBytes
      return addJournalRecordEncoder.on(byteBuffer, JournalRecordHeader.BYTES).fileId(1).compactCount(1).id(1L).recordType(1).noRecord().encodedLength();
   }

   @Benchmark
   public int encodeUnaligned() {
      outBuffer.clear();
      record.encode(outBuffer);
      return record.getEncodeSize();
   }

   @Benchmark
   public int encodeUnalignedWithGarbage() {
      outBuffer.clear();
      final JournalAddRecord addRecord = new JournalAddRecord(true, 1, (byte) 1, EncoderPersister.getInstance(), ZeroEncodingSupport.Instance);
      addRecord.setFileID(1);
      addRecord.setCompactCount((short) 1);
      addRecord.encode(outBuffer);
      return addRecord.getEncodeSize();
   }

   public enum ZeroEncodingSupport implements EncodingSupport {
      Instance;

      @Override
      public int getEncodeSize() {
         return 0;
      }

      @Override
      public void encode(ActiveMQBuffer buffer) {
      }

      @Override
      public void decode(ActiveMQBuffer buffer) {
      }
   }

}
