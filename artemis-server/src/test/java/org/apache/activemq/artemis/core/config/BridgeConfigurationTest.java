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
package org.apache.activemq.artemis.core.config;

import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.junit.Assert;
import org.junit.Test;

import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.spi.JsonProvider;
import java.io.StringReader;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class BridgeConfigurationTest {

   @Test
   public void testFromJSON() {
      String jsonString = createFullJsonObject().toString();

      BridgeConfiguration bridgeConfiguration = BridgeConfiguration.fromJSON(jsonString);

      Assert.assertNotNull(bridgeConfiguration);
      Assert.assertEquals("name", bridgeConfiguration.getName());
      Assert.assertEquals("queue-name", bridgeConfiguration.getQueueName());
      Assert.assertEquals("forwarding-address", bridgeConfiguration.getForwardingAddress());
      Assert.assertEquals("filter-string", bridgeConfiguration.getFilterString());
      Assert.assertArrayEquals(new String[]{"connector1", "connector2"},
            bridgeConfiguration.getStaticConnectors().toArray());
      Assert.assertEquals("dg", bridgeConfiguration.getDiscoveryGroupName());
      Assert.assertTrue(bridgeConfiguration.isHA());
      Assert.assertEquals("ClassName", bridgeConfiguration.getTransformerConfiguration().getClassName());
      Assert.assertThat(bridgeConfiguration.getTransformerConfiguration().getProperties().keySet(), containsInAnyOrder("prop1", "prop2"));
      Assert.assertEquals("val1", bridgeConfiguration.getTransformerConfiguration().getProperties().get("prop1"));
      Assert.assertEquals("val2", bridgeConfiguration.getTransformerConfiguration().getProperties().get("prop2"));
      Assert.assertEquals(1, bridgeConfiguration.getRetryInterval());
      Assert.assertEquals(2.0, bridgeConfiguration.getRetryIntervalMultiplier(), 0);
      Assert.assertEquals(3, bridgeConfiguration.getInitialConnectAttempts());
      Assert.assertEquals(4, bridgeConfiguration.getReconnectAttempts());
      Assert.assertEquals(5, bridgeConfiguration.getReconnectAttemptsOnSameNode());
      Assert.assertTrue(bridgeConfiguration.isUseDuplicateDetection());
      Assert.assertEquals(6, bridgeConfiguration.getConfirmationWindowSize());
      Assert.assertEquals(7, bridgeConfiguration.getProducerWindowSize());
      Assert.assertEquals(8, bridgeConfiguration.getClientFailureCheckPeriod());
      Assert.assertEquals("user", bridgeConfiguration.getUser());
      Assert.assertEquals("password", bridgeConfiguration.getPassword());
      Assert.assertEquals(9, bridgeConfiguration.getConnectionTTL());
      Assert.assertEquals(10, bridgeConfiguration.getMaxRetryInterval());
      Assert.assertEquals(11, bridgeConfiguration.getMinLargeMessageSize());
      Assert.assertEquals(12, bridgeConfiguration.getCallTimeout());
      Assert.assertEquals(ComponentConfigurationRoutingType.MULTICAST, bridgeConfiguration.getRoutingType());
      Assert.assertEquals(1, bridgeConfiguration.getConcurrency());
   }

   @Test
   public void testToJSON() {
      // create bc instance from a JSON object, all attributes are set
      JsonObject jsonObject = createFullJsonObject();
      BridgeConfiguration bridgeConfiguration = BridgeConfiguration.fromJSON(jsonObject.toString());
      Assert.assertNotNull(bridgeConfiguration);

      // serialize it back to JSON
      String serializedBridgeConfiguration = bridgeConfiguration.toJSON();
      JsonObject serializedBridgeConfigurationJsonObject = JsonLoader.readObject(new StringReader(serializedBridgeConfiguration));

      // verify that the original JSON object is identical to the one serialized via the toJSON() method
      Assert.assertEquals(jsonObject, serializedBridgeConfigurationJsonObject);
   }

   @Test
   public void testDefaultsToJson() {
      // create and serialize BridgeConfiguration instance without modifying any fields
      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration();
      String jsonString = bridgeConfiguration.toJSON();
      JsonObject jsonObject = JsonLoader.readObject(new StringReader(jsonString));

      // the serialized JSON string should contain default values of primitive type fields
      Assert.assertEquals("2000", jsonObject.get(BridgeConfiguration.RETRY_INTERVAL).toString());
      Assert.assertEquals("1.0", jsonObject.get(BridgeConfiguration.RETRY_INTERVAL_MULTIPLIER).toString());
      Assert.assertEquals("-1", jsonObject.get(BridgeConfiguration.INITIAL_CONNECT_ATTEMPTS).toString());
      Assert.assertEquals("-1", jsonObject.get(BridgeConfiguration.RECONNECT_ATTEMPTS).toString());
      Assert.assertEquals("10", jsonObject.get(BridgeConfiguration.RECONNECT_ATTEMPTS_ON_SAME_NODE).toString());
      Assert.assertEquals("true", jsonObject.get(BridgeConfiguration.USE_DUPLICATE_DETECTION).toString());
      Assert.assertEquals("10485760", jsonObject.get(BridgeConfiguration.CONFIRMATION_WINDOW_SIZE).toString());
      Assert.assertEquals("-1", jsonObject.get(BridgeConfiguration.PRODUCER_WINDOW_SIZE).toString());
      Assert.assertEquals("30000", jsonObject.get(BridgeConfiguration.CLIENT_FAILURE_CHECK_PERIOD).toString());
      Assert.assertEquals("2000", jsonObject.get(BridgeConfiguration.MAX_RETRY_INTERVAL).toString());
      Assert.assertEquals("102400", jsonObject.get(BridgeConfiguration.MIN_LARGE_MESSAGE_SIZE).toString());
      Assert.assertEquals("30000", jsonObject.get(BridgeConfiguration.CALL_TIMEOUT).toString());
      Assert.assertEquals("1", jsonObject.get(BridgeConfiguration.CONCURRENCY).toString());

      // also should contain default non-null values of string fields
      Assert.assertEquals("\"ACTIVEMQ.CLUSTER.ADMIN.USER\"", jsonObject.get(BridgeConfiguration.USER).toString());
      Assert.assertEquals("\"CHANGE ME!!\"", jsonObject.get(BridgeConfiguration.PASSWORD).toString());
   }

   @Test
   public void testDefaultsFromJson() {
      // create BridgeConfiguration instance from empty JSON string
      final String jsonString = "{\"name\": \"name\"}"; // name field is required
      BridgeConfiguration deserializedConfiguration = BridgeConfiguration.fromJSON(jsonString);
      Assert.assertNotNull(deserializedConfiguration);

      // the deserialized object should return the same default values as a newly instantiated object
      Assert.assertEquals(deserializedConfiguration, new BridgeConfiguration("name"));
   }

   @Test
   public void testNullableFieldsFromJson() {
      // set string fields which default value is not null to null
      JsonObjectBuilder builder = JsonLoader.createObjectBuilder();
      builder.add(BridgeConfiguration.NAME, "name"); // required field
      builder.addNull(BridgeConfiguration.USER);
      builder.addNull(BridgeConfiguration.PASSWORD);

      BridgeConfiguration configuration = BridgeConfiguration.fromJSON(builder.build().toString());

      // in deserialized object the fields should still remain null
      Assert.assertNotNull(configuration);
      Assert.assertEquals("name", configuration.getName());
      Assert.assertNull(configuration.getUser());
      Assert.assertNull(configuration.getPassword());
   }

   @Test
   public void testNullableFieldsToJson() {
      // set string fields which default value is not null to null
      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("name");
      bridgeConfiguration.setUser(null);
      bridgeConfiguration.setPassword(null);

      String jsonString = bridgeConfiguration.toJSON();
      JsonObject jsonObject = JsonLoader.readObject(new StringReader(jsonString));

      // after serialization the fields value should remain null
      Assert.assertEquals(JsonValue.ValueType.NULL, jsonObject.get(BridgeConfiguration.USER).getValueType());
      Assert.assertEquals(JsonValue.ValueType.NULL, jsonObject.get(BridgeConfiguration.PASSWORD).getValueType());
   }

   private static JsonObject createFullJsonObject() {
      JsonObjectBuilder objectBuilder = JsonLoader.createObjectBuilder();

      objectBuilder.add(BridgeConfiguration.NAME, "name");
      objectBuilder.add(BridgeConfiguration.QUEUE_NAME, "queue-name");
      objectBuilder.add(BridgeConfiguration.FORWARDING_ADDRESS, "forwarding-address");
      objectBuilder.add(BridgeConfiguration.FILTER_STRING, "filter-string");
      objectBuilder.add(BridgeConfiguration.STATIC_CONNECTORS,
            JsonProvider.provider().createArrayBuilder()
                  .add("connector1")
                  .add("connector2"));
      objectBuilder.add(BridgeConfiguration.DISCOVERY_GROUP_NAME, "dg");
      objectBuilder.add(BridgeConfiguration.HA, true);
      objectBuilder.add(BridgeConfiguration.TRANSFORMER_CONFIGURATION,
            JsonLoader.createObjectBuilder()
                  .add("class-name", "ClassName")
                  .add("properties",
                        JsonLoader.createObjectBuilder()
                              .add("prop1", "val1")
                              .add("prop2", "val2")));
      objectBuilder.add(BridgeConfiguration.RETRY_INTERVAL, 1);
      objectBuilder.add(BridgeConfiguration.RETRY_INTERVAL_MULTIPLIER, 2.0);
      objectBuilder.add(BridgeConfiguration.INITIAL_CONNECT_ATTEMPTS, 3);
      objectBuilder.add(BridgeConfiguration.RECONNECT_ATTEMPTS, 4);
      objectBuilder.add(BridgeConfiguration.RECONNECT_ATTEMPTS_ON_SAME_NODE, 5);
      objectBuilder.add(BridgeConfiguration.USE_DUPLICATE_DETECTION, true);
      objectBuilder.add(BridgeConfiguration.CONFIRMATION_WINDOW_SIZE, 6);
      objectBuilder.add(BridgeConfiguration.PRODUCER_WINDOW_SIZE, 7);
      objectBuilder.add(BridgeConfiguration.CLIENT_FAILURE_CHECK_PERIOD, 8);
      objectBuilder.add(BridgeConfiguration.USER, "user");
      objectBuilder.add(BridgeConfiguration.PASSWORD, "password");
      objectBuilder.add(BridgeConfiguration.CONNECTION_TTL, 9);
      objectBuilder.add(BridgeConfiguration.MAX_RETRY_INTERVAL, 10);
      objectBuilder.add(BridgeConfiguration.MIN_LARGE_MESSAGE_SIZE, 11);
      objectBuilder.add(BridgeConfiguration.CALL_TIMEOUT, 12);
      objectBuilder.add(BridgeConfiguration.ROUTING_TYPE, "MULTICAST");
      objectBuilder.add(BridgeConfiguration.CONCURRENCY, 1);

      return objectBuilder.build();
   }
}
