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
package org.apache.activemq.artemis.tests.unit.ra;

import static org.junit.Assert.assertEquals;

import javax.jms.Session;
import javax.resource.spi.InvalidPropertyException;

import org.apache.activemq.artemis.ra.inflow.ActiveMQActivationValidationUtils;
import org.junit.Test;

public class ActiveMQActivationsSpecTest {

   @Test(expected = InvalidPropertyException.class)
   public void nullDestinationName() throws InvalidPropertyException {
      ActiveMQActivationValidationUtils.validate(null, "destinationType", false, "subscriptionName");
   }

   @Test(expected = InvalidPropertyException.class)
   public void emptyDestinationName() throws InvalidPropertyException {
      ActiveMQActivationValidationUtils.validate(null, "destinationType", false, "subscriptionName");
   }

   public void nullDestinationType() throws InvalidPropertyException {
      ActiveMQActivationValidationUtils.validate("destinationName", null, false, "subscriptionName");
   }

   @Test(expected = InvalidPropertyException.class)
   public void emptyDestinationType() throws InvalidPropertyException {
      ActiveMQActivationValidationUtils.validate("destinationName", "", false, "subscriptionName");
   }

   @Test(expected = InvalidPropertyException.class)
   public void subscriptionDurableButNoName() throws InvalidPropertyException {
      ActiveMQActivationValidationUtils.validate("", "", true, "subscriptionName");
   }

   @Test(expected = IllegalArgumentException.class)
   public void validateAcknowledgeMode() {
      assertEquals(ActiveMQActivationValidationUtils.validateAcknowledgeMode("DUPS_OK_ACKNOWLEDGE"), Session.DUPS_OK_ACKNOWLEDGE);
      assertEquals(ActiveMQActivationValidationUtils.validateAcknowledgeMode("Dups-ok-acknowledge"), Session.DUPS_OK_ACKNOWLEDGE);
      assertEquals(ActiveMQActivationValidationUtils.validateAcknowledgeMode("AUTO_ACKNOWLEDGE"), Session.AUTO_ACKNOWLEDGE);
      assertEquals(ActiveMQActivationValidationUtils.validateAcknowledgeMode("Auto-acknowledge"), Session.AUTO_ACKNOWLEDGE);
      ActiveMQActivationValidationUtils.validateAcknowledgeMode("Invalid Acknowledge Mode");
   }
}
