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
package org.apache.activemq.artemis.protocol.amqp.proton;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.junit.Test;

public class ProtonServerReceiverContextTest {

   @Test
   public void testOnMessageWithAbortedDelivery() throws Exception {
      doOnMessageWithAbortedDeliveryTestImpl(false);
   }

   @Test
   public void testOnMessageWithAbortedDeliveryDrain() throws Exception {
      doOnMessageWithAbortedDeliveryTestImpl(true);
   }

   private void doOnMessageWithAbortedDeliveryTestImpl(boolean drain) throws ActiveMQAMQPException {
      Receiver mockReceiver = mock(Receiver.class);
      AMQPConnectionContext mockConnContext = mock(AMQPConnectionContext.class);

      when(mockConnContext.getAmqpCredits()).thenReturn(100);
      when(mockConnContext.getAmqpLowCredits()).thenReturn(30);

      ProtonServerReceiverContext rc = new ProtonServerReceiverContext(null, mockConnContext, null, mockReceiver);

      Delivery mockDelivery = mock(Delivery.class);
      when(mockDelivery.isAborted()).thenReturn(true);
      when(mockDelivery.isPartial()).thenReturn(true);
      when(mockDelivery.getLink()).thenReturn(mockReceiver);

      when(mockReceiver.current()).thenReturn(mockDelivery);

      if (drain) {
         when(mockReceiver.getDrain()).thenReturn(true);
      }

      rc.onMessage(mockDelivery);

      verify(mockReceiver, times(1)).current();
      verify(mockReceiver, times(1)).advance();
      verify(mockDelivery, times(1)).settle();

      verify(mockReceiver, times(1)).getDrain();
      if (!drain) {
         verify(mockReceiver, times(1)).flow(1);
      }
      verifyNoMoreInteractions(mockReceiver);
   }

}
