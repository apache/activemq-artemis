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
package org.apache.activemq.artemis.core.settings.impl;

public enum SlowConsumerThresholdMeasurementUnit {
   MESSAGES_PER_SECOND(1), MESSAGES_PER_MINUTE(60), MESSAGES_PER_HOUR(3600), MESSAGES_PER_DAY(3600 * 24);

   private final int measurementUnitInSeconds;

   SlowConsumerThresholdMeasurementUnit(int measurementUnitInSeconds) {
      this.measurementUnitInSeconds = measurementUnitInSeconds;
   }

   public static SlowConsumerThresholdMeasurementUnit valueOf(int measurementUnitInSeconds) {
      switch (measurementUnitInSeconds) {
         case 1:
            return MESSAGES_PER_SECOND;
         case 60:
            return MESSAGES_PER_MINUTE;
         case 3600:
            return MESSAGES_PER_HOUR;
         case 3600 * 24:
            return MESSAGES_PER_DAY;
         default:
            return null;
      }
   }

   public int getValue() {
      return measurementUnitInSeconds;
   }
}
