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
package org.apache.activemq.artemis.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import static org.apache.activemq.artemis.utils.PowerOf2Util.align;

public class PowerOf2UtilTest {

   @Test
   public void shouldAlignToNextMultipleOfAlignment() {
      final int alignment = 512;
      assertThat(align(0, alignment), is(0));
      assertThat(align(1, alignment), is(alignment));
      assertThat(align(alignment, alignment), is(alignment));
      assertThat(align(alignment + 1, alignment), is(alignment * 2));

      final int remainder = Integer.MAX_VALUE % alignment;
      final int alignedMax = Integer.MAX_VALUE - remainder;
      assertThat(align(alignedMax, alignment), is(alignedMax));
      //given that Integer.MAX_VALUE is the max value that can be represented with int
      //the aligned value would be > 2^32, but (int)(2^32) = Integer.MIN_VALUE due to the sign bit
      assertThat(align(Integer.MAX_VALUE, alignment), is(Integer.MIN_VALUE));
   }

}
