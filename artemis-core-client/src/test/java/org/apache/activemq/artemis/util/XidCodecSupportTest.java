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

package org.apache.activemq.artemis.util;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.artemis.utils.XidCodecSupport;
import org.apache.activemq.artemis.api.core.ActiveMQInvalidBufferException;
import org.junit.Test;

import javax.transaction.xa.Xid;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

public class XidCodecSupportTest {

   private static final Xid VALID_XID =
         new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

   @Test
   public void testEncodeDecode() {
      final ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(0);
      XidCodecSupport.encodeXid(VALID_XID, buffer);

      assertThat(buffer.readableBytes(), equalTo(51)); // formatId(4) + branchQualLength(4) + branchQual(3) +
      // globalTxIdLength(4) + globalTx(36)

      final Xid readXid = XidCodecSupport.decodeXid(buffer);
      assertThat(readXid, equalTo(VALID_XID));
   }

   @Test
   public void testNegativeLength() {
      final ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(0);
      XidCodecSupport.encodeXid(VALID_XID, buffer);
      // Alter branchQualifierLength to be negative
      buffer.setByte(4, (byte) 0xFF);
      try {
         XidCodecSupport.decodeXid(buffer);
         fail("Should have thrown");
      } catch (ActiveMQInvalidBufferException ex) {
         return;
      }

      fail("should have thrown exception");
   }

   @Test(expected = ActiveMQInvalidBufferException.class)
   public void testOverflowLength() {
      final ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(0);
      XidCodecSupport.encodeXid(VALID_XID, buffer);
      // Alter globalTxIdLength to be too big
      buffer.setByte(11, (byte) 0x0C);

      XidCodecSupport.decodeXid(buffer);
   }
}