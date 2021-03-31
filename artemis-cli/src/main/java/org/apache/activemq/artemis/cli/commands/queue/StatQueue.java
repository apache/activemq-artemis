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
package org.apache.activemq.artemis.cli.commands.queue;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.cli.commands.ActionContext;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@Command(name = "stat", description = "prints out basic stats associated with queues. Output includes CONSUMER_COUNT (number of consumers), MESSAGE_COUNT (current message count on the queue, including scheduled, paged and in-delivery messages), MESSAGES_ADDED (messages added to the queue), DELIVERING_COUNT (messages broker is currently delivering to consumer(s)), MESSAGES_ACKED (messages acknowledged from the consumer(s))." + " Queues can be filtered using EITHER '--queueName X' where X is contained in the queue name OR using a full filter '--field NAME --operation EQUALS --value X'."

)
public class StatQueue extends AbstractAction {

   public enum FIELD {
      NAME("name"), ADDRESS("address"), CONSUMER_COUNT("consumerCount"), MESSAGE_COUNT("messageCount"), MESSAGES_ADDED("messagesAdded"), DELIVERING_COUNT("deliveringCount"), MESSAGES_ACKED("messagesAcked"), SCHEDULED_COUNT("scheduledCount"), ROUTING_TYPE("routingType");

      private static final Map<String, FIELD> lookup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

      static {
         for (FIELD e: values()) {
            lookup.put(e.jsonId, e);
         }
      }

      private String jsonId;

      FIELD(String jsonId) {
         this.jsonId = jsonId;
      }

      String getJsonId() {
         return this.jsonId;
      }

      public static FIELD valueOfJsonId(String jsonId) {
         return lookup.get(jsonId);
      }
   }

   public enum OPERATION {
      CONTAINS, EQUALS, GREATER_THAN, LESS_THAN
   }

   public static final int DEFAULT_MAX_ROWS = 50;

   @Option(name = "--queueName", description = "display queue stats for queue(s) with names containing this string.")
   private String queueName;

   @Option(name = "--field", description = "field to use in filter. Possible values NAME, ADDRESS, MESSAGE_COUNT, MESSAGES_ADDED, DELIVERING_COUNT, MESSAGES_ACKED, SCHEDULED_COUNT, ROUTING_TYPE.")
   private String fieldName;

   @Option(name = "--operation", description = "operation to use in filter. Possible values CONTAINS, EQUALS, GREATER_THAN, LESS_THAN.")
   private String operationName;

   @Option(name = "--value", description = "value to use in the filter.")
   private String value;

   @Option(name = "--maxRows", description = "max number of queues displayed. Default is 50.")
   private int maxRows = DEFAULT_MAX_ROWS;

   //easier for testing
   public StatQueue setQueueName(String queueName) {
      this.queueName = queueName;
      return this;
   }

   public StatQueue setOperationName(String operationName) {
      this.operationName = operationName;
      return this;
   }

   public StatQueue setFieldName(String fieldName) {
      this.fieldName = fieldName;
      return this;
   }

   public StatQueue setValue(String value) {
      this.value = value;
      return this;
   }

   public StatQueue setMaxRows(int maxRows) {
      this.maxRows = maxRows;
      return this;
   }

   public StatQueue setverbose(boolean verbose) {
      this.verbose = verbose;
      return this;
   }

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      String filter = createFilter();

      //should always get an initialised filter back when values correct
      if (filter == null) {
         return null;
      }

      if (verbose) {
         context.out.println("filter is '" + filter + "'");
         context.out.println("maxRows='" + maxRows + "'");
      }
      printStats(context, filter);
      return null;
   }

   private void printStats(final ActionContext context, final String filter) throws Exception {
      performCoreManagement(new ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            ManagementHelper.putOperationInvocation(message, "broker", "listQueues", filter, 1, maxRows);
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            final String result = (String) ManagementHelper.getResult(reply, String.class);
            printStats(result);
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to get Stats for Queues. Reason: " + errMsg);
         }
      });
   }

   private void printStats(String result) {
      printHeadings();

      //should not happen but...
      if (result == null) {
         if (verbose) {
            context.err.println("printStats(): got NULL result string.");
         }
         return;
      }

      JsonObject queuesAsJsonObject = JsonUtil.readJsonObject(result);
      int count = queuesAsJsonObject.getInt("count");
      JsonArray array = queuesAsJsonObject.getJsonArray("data");

      for (int i = 0; i < array.size(); i++) {
         printQueueStats(array.getJsonObject(i));
      }

      if (count > maxRows) {
         context.out.println(String.format("WARNING: the displayed queues are %d/%d, set maxRows to display more queues.", maxRows, count));
      }
   }

   private void printHeadings() {

      StringBuilder stringBuilder = new StringBuilder(134).append('|').append(paddingString(new StringBuilder(FIELD.NAME.toString()), 25)).append('|').append(paddingString(new StringBuilder(FIELD.ADDRESS.toString()), 25)).append('|').append(paddingString(new StringBuilder(FIELD.CONSUMER_COUNT.toString() + " "), 15)).append('|').append(paddingString(new StringBuilder(FIELD.MESSAGE_COUNT.toString() + " "), 14)).append('|').append(paddingString(new StringBuilder(FIELD.MESSAGES_ADDED.toString() + " "), 15)).append('|').append(paddingString(new StringBuilder(FIELD.DELIVERING_COUNT.toString() + " "), 17)).append('|').append(paddingString(new StringBuilder(FIELD.MESSAGES_ACKED.toString() + " "), 15)).append('|').append(paddingString(new StringBuilder(FIELD.SCHEDULED_COUNT.toString() + " "), 16)).append('|').append(paddingString(new StringBuilder(FIELD.ROUTING_TYPE.toString() + " "), 13)).append('|');

      context.out.println(stringBuilder);
   }

   private void printQueueStats(JsonObject jsonObject) {

      //should not happen but just in case..
      if (jsonObject == null) {
         if (verbose) {
            context.err.println("printQueueStats(): jsonObject is null");
         }
         return;
      }

      StringBuilder stringBuilder = new StringBuilder(134).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.NAME.getJsonId())), 25)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.ADDRESS.getJsonId())), 25)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.CONSUMER_COUNT.getJsonId())), 15)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.MESSAGE_COUNT.getJsonId())), 14)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.MESSAGES_ADDED.getJsonId())), 15)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.DELIVERING_COUNT.getJsonId())), 17)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.MESSAGES_ACKED.getJsonId())), 15)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.SCHEDULED_COUNT.getJsonId())), 16)).append('|').append(paddingString(new StringBuilder(jsonObject.getString(FIELD.ROUTING_TYPE.getJsonId())), 13)).append('|');

      context.out.println(stringBuilder);
   }

   private StringBuilder paddingString(StringBuilder value, int size) {

      //should not happen but just in case ...
      if (value == null) {
         return new StringBuilder(size);
      }

      //would expect to have some data
      if (value.length() == 0) {
         value.append("NO DATA");
      }

      int length = value.length();
      if (length >= size) {
         //no padding required
         return value;
      }

      for (int i = 1; (i + length) <= size; i++) {
         value.append(' ');
      }

      return value;
   }

   //creates filter used for listQueues()
   private String createFilter() {

      HashMap<String, Object> filterMap = new HashMap<>();

      if (((fieldName != null) && (fieldName.trim().length() > 0)) && ((queueName != null && queueName.trim().length() > 0))) {
         context.err.println("'--field' and '--queueName' cannot be specified together.");
         return null;
      }

      if ((fieldName != null) && (fieldName.trim().length() > 0)) {
         try {
            FIELD field = FIELD.valueOfJsonId(fieldName);

            //for backward compatibility
            if (field == null) {
               field = FIELD.valueOf(fieldName);
            }

            filterMap.put("field", field.toString());
         } catch (IllegalArgumentException ex) {
            context.err.println("'--field' must be set to one of the following " + Arrays.toString(FIELD.values()));
            return null;
         }

         //full filter being set ensure value is set
         if (value == null || value.trim().length() == 0) {
            context.err.println("'--value' needs to be set when '--field' is specified");
            return null;
         }
         filterMap.put("value", value);

         if (operationName == null) {
            context.err.println("'--operation' must be set when '--field' is specified " + Arrays.toString(OPERATION.values()));
            return null;
         }

         try {
            OPERATION operation = OPERATION.valueOf(operationName);
            filterMap.put("operation", operation.toString());
         } catch (IllegalArgumentException ex) {
            context.err.println("'--operation' must be set to one of the following " + Arrays.toString(OPERATION.values()));
            return null;
         }

      } else if (queueName != null && queueName.trim().length() > 0) {
         filterMap.put("field", FIELD.NAME.toString());
         filterMap.put("value", queueName);
         filterMap.put("operation", OPERATION.CONTAINS.toString());
      } else {
         filterMap.put("field", "");
         filterMap.put("value", "");
         filterMap.put("operation", "");
      }

      JsonObject filterJsonObject = JsonUtil.toJsonObject(filterMap);
      return filterJsonObject.toString();
   }
}