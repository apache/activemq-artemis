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

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostOfficeImplTest {

   @Test
   public void testZeroMaxExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMaxExpiryDelay(0L));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testZeroMaxExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(Long.MAX_VALUE);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMaxExpiryDelay(0L));
      Mockito.verify(mockMessage).setExpiration(0L);
   }

   @Test
   public void testNonZeroMaxExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMaxExpiryDelay(1234L));
      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());
      assertTrue(captor.getValue() > 0L);
   }

   @Test
   public void testNonZeroMaxExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(Long.MAX_VALUE);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMaxExpiryDelay(1234L));
      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());
      assertTrue(captor.getValue() < Long.MAX_VALUE);
   }

   @Test
   public void testZeroMinExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(0L));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }

   @Test
   public void testZeroMinExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(1L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(0L));
      Mockito.verify(mockMessage).setExpiration(0L);
   }

   @Test
   public void testNonZeroMinExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(1234L));
      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());
      assertTrue(captor.getValue() > 1234L);
   }

   @Test
   public void testNonZeroMinExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(1234L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setMinExpiryDelay(9999L));
      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());
      assertTrue(captor.getValue() > 9999L);
   }

   @Test
   public void testExpiryDelayWhenExpirationNotSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(0L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setExpiryDelay(9999L));
      final ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
      Mockito.verify(mockMessage).setExpiration(captor.capture());
      assertTrue(captor.getValue() > System.currentTimeMillis());
   }

   @Test
   public void testExpiryDelayWhenExpirationSet() {
      Message mockMessage = Mockito.mock(Message.class);
      Mockito.when(mockMessage.getExpiration()).thenReturn(1L);
      PostOfficeImpl.applyExpiryDelay(mockMessage, new AddressSettings().setExpiryDelay(9999L));
      Mockito.verify(mockMessage, Mockito.never()).setExpiration(Mockito.anyLong());
   }
}
