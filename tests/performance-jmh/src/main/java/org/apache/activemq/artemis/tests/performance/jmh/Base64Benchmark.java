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
package org.apache.activemq.artemis.tests.performance.utils;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
public class Base64Benchmark {

   @Param({"1", "10", "100"})
   private static int length;

   @State(Scope.Benchmark)
   public static class Input {
      public byte[] bytesToEncode = RandomUtil.randomString().repeat(length).getBytes();
      public String stringToDecode = java.util.Base64.getUrlEncoder().encodeToString(bytesToEncode);
   }

   @Benchmark
   public void benchmarkCustomBase64Encoding(Blackhole blackhole, Input input) {
      blackhole.consume(Base64.encodeBytes(input.bytesToEncode));
   }

   @Benchmark
   public void benchmarkJavaBase64Encoding(Blackhole blackhole, Input input) {
      blackhole.consume(java.util.Base64.getUrlEncoder().encodeToString(input.bytesToEncode));
   }

   @Benchmark
   public void benchmarkCustomBase64Decoding(Blackhole blackhole, Input input) {
      blackhole.consume(Base64.decode(input.stringToDecode));
   }

   @Benchmark
   public void benchmarkJavaBase64Decoding(Blackhole blackhole, Input input) {
      blackhole.consume(java.util.Base64.getUrlDecoder().decode(input.stringToDecode));
   }
}
