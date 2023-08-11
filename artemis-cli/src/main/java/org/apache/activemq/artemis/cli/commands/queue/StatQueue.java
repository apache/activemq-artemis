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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.Terminal;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.messages.ConnectionAbstract;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stat", description = "Print basic stats of a queue. Output includes CONSUMER_COUNT (number of consumers), MESSAGE_COUNT (current message count on the queue, including scheduled, paged and in-delivery messages), MESSAGES_ADDED (messages added to the queue), DELIVERING_COUNT (messages broker is currently delivering to consumer(s)), MESSAGES_ACKED (messages acknowledged from the consumer(s))." + " Queues can be filtered using EITHER '--queueName X' where X is contained in the queue name OR using a full filter '--field NAME --operation EQUALS --value X'.")
public class StatQueue extends ConnectionAbstract {

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
      CONTAINS, NOT_CONTAINS, EQUALS, GREATER_THAN, LESS_THAN
   }

   public static final int DEFAULT_MAX_ROWS = 50;

   public static final int DEFAULT_MAX_COLUMN_SIZE = 25;

   @Option(names = "--queueName", description = "Display queue stats for queue(s) with names containing this string.")
   private String queueName;

   @Option(names = "--field", description = "The field to filter. Possible values: NAME, ADDRESS, MESSAGE_COUNT, MESSAGES_ADDED, DELIVERING_COUNT, MESSAGES_ACKED, SCHEDULED_COUNT, ROUTING_TYPE.")
   private String fieldName;

   @Option(names = "--operation", description = "The operation to filter. Possible values: CONTAINS, NOT_CONTAINS, EQUALS, GREATER_THAN, LESS_THAN.")
   private String operationName;

   @Option(names = "--value", description = "The value to filter.")
   private String value;

   @Option(names = "--maxRows", description = "The max number of queues displayed. Default is 50.")
   private int maxRows = DEFAULT_MAX_ROWS;

   @Option(names = "--maxColumnSize", description = "The max width of data column. Set to -1 for no limit. Default is 25.")
   private int maxColumnSize = DEFAULT_MAX_COLUMN_SIZE;

   @Option(names = "--clustered", description = "Expands the report for all nodes on the topology")
   private boolean clustered = false;

   private int statCount = 0;

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

   public StatQueue setMaxColumnSize(int maxColumnSize) {
      int maxFieldSize = 0;
      for (FIELD e : FIELD.values()) {
         if (e.jsonId.length() > maxFieldSize) {
            maxFieldSize = e.jsonId.length();
         }
      }
      if (maxColumnSize != -1 && maxColumnSize < maxFieldSize) {
         throw new IllegalArgumentException("maxColumnSize must be " + maxFieldSize + " or greater or -1 (i.e. no limit).");
      }
      this.maxColumnSize = maxColumnSize;
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
         getActionContext().out.println("filter is '" + filter + "'");
         getActionContext().out.println("maxRows='" + maxRows + "'");
      }
      createConnectionFactory();

      try (SimpleManagement simpleManagement = new SimpleManagement(brokerURL, user, password).open()) {
         String nodeID = simpleManagement.getNodeID();
         JsonArray topology = simpleManagement.listNetworkTopology();

         if (clustered && topology.size() > 1) {
            context.out.println(Terminal.YELLOW_UNICODE + "*******************************************************************************************************************************");
            context.out.println(">>> Queue stats on node " + nodeID + ", url=" + brokerURL + Terminal.CLEAR_UNICODE);
            printStats(brokerURL, filter);

            for (int i = 0; i < topology.size(); i++) {
               JsonObject node = topology.getJsonObject(i);
               if (node.getString("nodeID").equals(nodeID) || node.getJsonString("live") == null) {
                  continue;
               }

               String url = "tcp://" + node.getString("live");

               context.out.println(Terminal.YELLOW_UNICODE + "*******************************************************************************************************************************");
               context.out.println(">>> Queue stats on node " + node.getString("nodeID") + ", url=" + url + Terminal.CLEAR_UNICODE);

               printStats(url, filter);
            }
         } else {
            printStats(brokerURL, filter);
            if (topology.size() > 1) {
               context.out.println();
               context.out.println("Note: Use " + Terminal.RED_UNICODE + "--clustered" + Terminal.CLEAR_UNICODE + " to expand the report to other nodes in the topology.");
               context.out.println();
            }
         }
      }


      return statCount;
   }

   private void printStats(String uri, final String filter) throws Exception {
      performCoreManagement(uri, user, password, message -> {
         ManagementHelper.putOperationInvocation(message, "broker", "listQueues", filter, 1, maxRows);
      }, reply -> {
         final String result = (String) ManagementHelper.getResult(reply, String.class);
         printStats(result);
      }, reply -> {
         String errMsg = (String) ManagementHelper.getResult(reply, String.class);
         getActionContext().err.println("Failed to get Stats for Queues. Reason: " + errMsg);
      });
   }

   private void printStats(String result) {

      //should not happen but...
      if (result == null) {
         if (verbose) {
            getActionContext().err.println("printStats(): got NULL result string.");
         }
         return;
      }

      JsonObject queuesAsJsonObject = JsonUtil.readJsonObject(result);
      int count = queuesAsJsonObject.getInt("count");
      JsonArray array = queuesAsJsonObject.getJsonArray("data");

      int[] columnSizes = new int[FIELD.values().length];

      FIELD[] fields = FIELD.values();
      for (int i = 0; i < fields.length; i++) {
         columnSizes[i] = fields[i].toString().length();
      }

      for (int i = 0; i < array.size(); i++) {
         getColumnSizes(array.getJsonObject(i), columnSizes);
      }

      printHeadings(columnSizes);

      for (int i = 0; i < array.size(); i++) {
         printQueueStats(array.getJsonObject(i), columnSizes);
         statCount++;
      }

      if (count > maxRows) {
         getActionContext().out.println(String.format("WARNING: the displayed queues are %d/%d, set maxRows to display more queues.", maxRows, count));
      }
   }

   private void getColumnSizes(JsonObject jsonObject, int[] columnSizes) {
      int i = 0;
      for (FIELD e: FIELD.values()) {
         if (jsonObject.getString(e.jsonId).length() > columnSizes[i]) {
            columnSizes[i] = jsonObject.getString(e.jsonId).length();
         }
         // enforce max
         if (columnSizes[i] > maxColumnSize && maxColumnSize != -1) {
            columnSizes[i] = maxColumnSize;
         }
         i++;
      }
   }

   private void printHeadings(int[] columnSizes) {
      // add 10 for the various '|' characters
      StringBuilder stringBuilder = new StringBuilder(Arrays.stream(columnSizes).sum() + FIELD.values().length + 1).append('|');

      int i = 0;
      for (FIELD e: FIELD.values()) {
         stringBuilder.append(paddingString(new StringBuilder(e.toString()), columnSizes[i++])).append('|');
      }

      getActionContext().out.println(stringBuilder);
   }

   private void printQueueStats(JsonObject jsonObject, int[] columnSizes) {

      //should not happen but just in case..
      if (jsonObject == null) {
         if (verbose) {
            getActionContext().err.println("printQueueStats(): jsonObject is null");
         }
         return;
      }

      // add 10 for the various '|' characters
      StringBuilder stringBuilder = new StringBuilder(Arrays.stream(columnSizes).sum() + FIELD.values().length + 1).append('|');

      int i = 0;
      for (FIELD e: FIELD.values()) {
         stringBuilder.append(paddingString(new StringBuilder(jsonObject.getString(e.jsonId)), columnSizes[i++])).append('|');
      }

      getActionContext().out.println(stringBuilder);
   }

   private StringBuilder paddingString(StringBuilder value, int maxColumnSize) {

      //should not happen but just in case ...
      if (value == null) {
         return new StringBuilder(maxColumnSize);
      }

      //would expect to have some data
      if (value.length() == 0) {
         value.append("NO DATA");
      }

      int length = value.length();

      if (length > maxColumnSize && this.maxColumnSize != -1) {
         // truncate if necessary
         return new StringBuilder(value.substring(0, maxColumnSize - 3) + "...");
      }

      for (int i = 1; (i + length) <= maxColumnSize; i++) {
         value.append(' ');
      }

      return value;
   }

   //creates filter used for listQueues()
   private String createFilter() {

      HashMap<String, Object> filterMap = new HashMap<>();

      if (((fieldName != null) && (fieldName.trim().length() > 0)) && ((queueName != null && queueName.trim().length() > 0))) {
         getActionContext().err.println("'--field' and '--queueName' cannot be specified together.");
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
            getActionContext().err.println("'--field' must be set to one of the following " + Arrays.toString(FIELD.values()));
            return null;
         }

         //full filter being set ensure value is set
         if (value == null || value.trim().length() == 0) {
            getActionContext().err.println("'--value' needs to be set when '--field' is specified");
            return null;
         }
         filterMap.put("value", value);

         if (operationName == null) {
            getActionContext().err.println("'--operation' must be set when '--field' is specified " + Arrays.toString(OPERATION.values()));
            return null;
         }

         try {
            OPERATION operation = OPERATION.valueOf(operationName);
            filterMap.put("operation", operation.toString());
         } catch (IllegalArgumentException ex) {
            getActionContext().err.println("'--operation' must be set to one of the following " + Arrays.toString(OPERATION.values()));
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