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
package org.apache.activemq.artemis.core.postoffice.impl;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostOfficeImplTest {

   private static final int EXPIRATION_DELTA = 5000;

   @Test
   public void testNeverExpireWhenExpirationSetLow() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(1L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setNeverExpire(true));
      Mockito.verify(mockMessage).setExpiration(0);
   }

   @Test
   public void testNeverExpireWhenExpirationSetHigh() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(Long.MAX_VALUE);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setNeverExpire(true));
      Mockito.verify(mockMessage).setExpiration(0);
   }

   @Test
   public void testNeverExpireWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setNeverExpire(true));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      final long expiryDelay = 123456L;
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setExpiryDelay(expiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + expiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(1L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setExpiryDelay(9999L));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testMinExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      final long minExpiryDelay = 123456L;
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(minExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + minExpiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testMinExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      long origExpiration = 1234L;
      Mockito.when(mockMessage.getExpiration()).thenReturn(origExpiration);
      final long minExpiryDelay = 123456L;
      assertTrue(minExpiryDelay > origExpiration);
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(minExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + minExpiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testMaxExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      final long maxExpiryDelay = 123456L;
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMaxExpiryDelay(maxExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + maxExpiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testMaxExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(Long.MAX_VALUE);
      final long maxExpiryDelay = 123456L;
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMaxExpiryDelay(maxExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + maxExpiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testMinAndMaxExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      long origExpiration = 0L;
      Mockito.when(mockMessage.getExpiration()).thenReturn(origExpiration);
      final long minExpiryDelay = 100_000L;
      final long maxExpiryDelay = 300_000L;
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(minExpiryDelay).setMaxExpiryDelay(maxExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + maxExpiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testMinAndMaxExpiryDelayWhenExpirationSetInbetween() {
      Message mockMessage = Mockito.mock(Message.class);
      final long startTime = System.currentTimeMillis();
      long origExpiration = startTime + 200_000L;
      Mockito.when(mockMessage.getExpiration()).thenReturn(origExpiration);
      final long minExpiryDelay = 100_000L;
      final long maxExpiryDelay = 300_000L;

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(minExpiryDelay).setMaxExpiryDelay(maxExpiryDelay));

      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testMinAndMaxExpiryDelayWhenExpirationSetAbove() {
      Message mockMessage = Mockito.mock(Message.class);
      final long startTime = System.currentTimeMillis();
      long origExpiration = startTime + 400_000L;
      Mockito.when(mockMessage.getExpiration()).thenReturn(origExpiration);
      final long minExpiryDelay = 100_000L;
      final long maxExpiryDelay = 300_000L;

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(minExpiryDelay).setMaxExpiryDelay(maxExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + maxExpiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testMinAndMaxExpiryDelayWhenExpirationSetBelow() {
      Message mockMessage = Mockito.mock(Message.class);
      final long startTime = System.currentTimeMillis();
      long origExpiration = startTime + 50_000;
      Mockito.when(mockMessage.getExpiration()).thenReturn(origExpiration);
      final long minExpiryDelay = 100_000L;
      final long maxExpiryDelay = 300_000L;

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(minExpiryDelay).setMaxExpiryDelay(maxExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + minExpiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   private void assertExpirationSetAsExpected(final long expectedExpirationLow, final long expectedExpirationHigh, final Long actualExpirationSet) {
      assertNotNull(actualExpirationSet);

      assertTrue(actualExpirationSet >= expectedExpirationLow, () -> "Expected set expiration of at least " + expectedExpirationLow + ", but was: " + actualExpirationSet);
      assertTrue(actualExpirationSet < expectedExpirationHigh, "Expected set expiration less than " + expectedExpirationHigh + ", but was: " + actualExpirationSet);
   }

   @Test
   public void testPrecedencNeverExpireOverExpiryDelay() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setNeverExpire(true).setExpiryDelay(10L));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testPrecedencNeverExpireOverMaxExpiryDelay() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setNeverExpire(true).setMaxExpiryDelay(10L));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testPrecedencNeverExpireOverMinExpiryDelay() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setNeverExpire(true).setMinExpiryDelay(10L));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testPrecedencExpiryDelayOverMaxExpiryDelay() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      final long expiryDelay = 1000L;
      final long maxExpiryDelay = 999999999L;
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setExpiryDelay(expiryDelay).setMaxExpiryDelay(maxExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + expiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }

   @Test
   public void testPrecedencExpiryDelayOverMinExpiryDelay() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      final long expiryDelay = 1000L;
      final long minExpiryDelay = 999999999L;
      final long startTime = System.currentTimeMillis();

      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setExpiryDelay(expiryDelay).setMinExpiryDelay(minExpiryDelay));

      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());

      final long expectedExpirationLow = startTime + expiryDelay;
      final long expectedExpirationHigh = expectedExpirationLow + EXPIRATION_DELTA; // Allowing a delta
      final Long actualExpirationSet = captor.getValue();

      assertExpirationSetAsExpected(expectedExpirationLow, expectedExpirationHigh, actualExpirationSet);
   }
}
