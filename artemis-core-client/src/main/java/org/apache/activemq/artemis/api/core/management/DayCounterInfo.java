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
package org.apache.activemq.artemis.api.core.management;

import java.util.Arrays;

import org.apache.activemq.artemis.utils.json.JSONArray;
import org.apache.activemq.artemis.utils.json.JSONException;
import org.apache.activemq.artemis.utils.json.JSONObject;

/**
 * Helper class to create Java Objects from the
 * JSON serialization returned by {@link QueueControl#listMessageCounterHistory()}.
 */
public final class DayCounterInfo {

   private final String date;

   private final int[] counters;

   // Static --------------------------------------------------------

   public static String toJSON(final DayCounterInfo[] infos) throws JSONException {
      JSONObject json = new JSONObject();
      JSONArray counters = new JSONArray();
      for (DayCounterInfo info : infos) {
         JSONObject counter = new JSONObject();
         counter.put("date", info.getDate());
         counter.put("counters", Arrays.asList(info.getCounters()));
         counters.put(counter);
      }
      json.put("dayCounters", counters);
      return json.toString();
   }

   /**
    * Returns an array of RoleInfo corresponding to the JSON serialization returned
    * by {@link QueueControl#listMessageCounterHistory()}.
    */
   public static DayCounterInfo[] fromJSON(final String jsonString) throws JSONException {
      JSONObject json = new JSONObject(jsonString);
      JSONArray dayCounters = json.getJSONArray("dayCounters");
      DayCounterInfo[] infos = new DayCounterInfo[dayCounters.length()];
      for (int i = 0; i < dayCounters.length(); i++) {

         JSONObject counter = (JSONObject) dayCounters.get(i);
         JSONArray hour = (JSONArray) counter.getJSONArray("counters").get(0);
         int[] hourCounters = new int[24];
         for (int j = 0; j < 24; j++) {
            hourCounters[j] = hour.getInt(j);
         }
         DayCounterInfo info = new DayCounterInfo(counter.getString("date"), hourCounters);
         infos[i] = info;
      }
      return infos;
   }

   // Constructors --------------------------------------------------

   public DayCounterInfo(final String date, final int[] counters) {
      this.date = date;
      this.counters = counters;
   }

   // Public --------------------------------------------------------

   /**
    * Returns the date of the counter.
    */
   public String getDate() {
      return date;
   }

   /**
    * Returns a 24-length array corresponding to the number of messages added to the queue
    * for the given hour of the day.
    */
   public int[] getCounters() {
      return counters;
   }
}
