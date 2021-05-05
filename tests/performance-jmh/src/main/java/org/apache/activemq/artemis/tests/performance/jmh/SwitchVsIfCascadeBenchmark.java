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
package org.apache.activemq.artemis.tests.performance.jmh;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Fork(2)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 100, timeUnit = TimeUnit.MILLISECONDS)
public class SwitchVsIfCascadeBenchmark {

   public static final int SEED = 0;
   private byte[] inputs;
   @Param({"40", "60", "80"})
   private int mostSeenProb;

   @Param({"8192"})
   private int size;
   private int nextInput;

   @Setup
   public void initDataSet() {
      inputs = new byte[size];
      SplittableRandom random = new SplittableRandom(SEED);
      for (int i = 0; i < size; i++) {
         if (random.nextInt(1, 101) <= mostSeenProb) {
            inputs[i] = PacketImpl.REPLICATION_APPEND;
         } else {
            while (true) {
               inputs[i] = (byte) random.nextInt(PacketImpl.REPLICATION_APPEND_TX, PacketImpl.REPLICATION_SCHEDULED_FAILOVER + 1);
               if (switchOn(inputs[i]) != -1) {
                  break;
               }
            }
         }
      }
      nextInput = 0;
   }

   private byte nextInput() {
      final byte input = inputs[nextInput];
      nextInput++;
      if (nextInput == size) {
         nextInput = 0;
      }
      return input;
   }

   @Benchmark
   @CompilerControl(CompilerControl.Mode.DONT_INLINE)
   public int testSwitch() {
      return switchOn(nextInput());
   }

   @Benchmark
   @CompilerControl(CompilerControl.Mode.DONT_INLINE)
   public int testIf() {
      return ifOn(nextInput());
   }

   private static int ifOn(byte type) {
      if (type == PacketImpl.REPLICATION_APPEND) {
         return foo(1);
      } else if (type == PacketImpl.REPLICATION_APPEND_TX) {
         return foo(2);
      } else if (type == PacketImpl.REPLICATION_DELETE) {
         return foo(3);
      } else if (type == PacketImpl.REPLICATION_DELETE_TX) {
         return foo(4);
      } else if (type == PacketImpl.REPLICATION_PREPARE) {
         return foo(5);
      } else if (type == PacketImpl.REPLICATION_COMMIT_ROLLBACK) {
         return foo(6);
      } else if (type == PacketImpl.REPLICATION_PAGE_WRITE) {
         return foo(7);
      } else if (type == PacketImpl.REPLICATION_PAGE_EVENT) {
         return foo(8);
      } else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN) {
         return foo(9);
      } else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE) {
         return foo(10);
      } else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_END) {
         return foo(11);
      } else if (type == PacketImpl.REPLICATION_START_FINISH_SYNC) {
         return foo(12);
      } else if (type == PacketImpl.REPLICATION_SYNC_FILE) {
         return foo(13);
      } else if (type == PacketImpl.REPLICATION_SCHEDULED_FAILOVER) {
         return foo(14);
      } else if (type == PacketImpl.BACKUP_REGISTRATION_FAILED) {
         return foo(15);
      } else {
         return -1;
      }
   }

   private static int switchOn(byte type) {
      switch (type) {
         case PacketImpl.REPLICATION_APPEND:
            return foo(1);
         case PacketImpl.REPLICATION_APPEND_TX:
            return foo(2);
         case PacketImpl.REPLICATION_DELETE:
            return foo(3);
         case PacketImpl.REPLICATION_DELETE_TX:
            return foo(4);
         case PacketImpl.REPLICATION_PREPARE:
            return foo(5);
         case PacketImpl.REPLICATION_COMMIT_ROLLBACK:
            return foo(6);
         case PacketImpl.REPLICATION_PAGE_WRITE:
            return foo(7);
         case PacketImpl.REPLICATION_PAGE_EVENT:
            return foo(8);
         case PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN:
            return foo(9);
         case PacketImpl.REPLICATION_LARGE_MESSAGE_END:
            return foo(10);
         case PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE:
            return foo(11);
         case PacketImpl.REPLICATION_START_FINISH_SYNC:
            return foo(12);
         case PacketImpl.REPLICATION_SYNC_FILE:
            return foo(13);
         case PacketImpl.REPLICATION_SCHEDULED_FAILOVER:
            return foo(14);
         case PacketImpl.BACKUP_REGISTRATION_FAILED:
            return foo(15);
         default:
            return -1;
      }
   }

   @CompilerControl(CompilerControl.Mode.DONT_INLINE)
   private static int foo(int v) {
      return v;
   }

}
