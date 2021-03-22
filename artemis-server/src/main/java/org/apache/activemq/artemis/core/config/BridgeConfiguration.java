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

import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.io.Serializable;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.utils.JsonLoader;

public final class BridgeConfiguration implements Serializable {

   private static final long serialVersionUID = -1057244274380572226L;

   public static String NAME = "name";
   public static String QUEUE_NAME = "queue-name";
   public static String FORWARDING_ADDRESS = "forwarding-address";
   public static String FILTER_STRING = "filter-string";
   public static String STATIC_CONNECTORS = "static-connectors";
   public static String DISCOVERY_GROUP_NAME = "discovery-group-name";
   public static String HA = "ha";
   public static String TRANSFORMER_CONFIGURATION = "transformer-configuration";
   public static String RETRY_INTERVAL = "retry-interval";
   public static String RETRY_INTERVAL_MULTIPLIER = "retry-interval-multiplier";
   public static String INITIAL_CONNECT_ATTEMPTS = "initial-connect-attempts";
   public static String RECONNECT_ATTEMPTS = "reconnect-attempts";
   public static String RECONNECT_ATTEMPTS_ON_SAME_NODE = "reconnect-attempts-on-same-node";
   public static String USE_DUPLICATE_DETECTION = "use-duplicate-detection";
   public static String CONFIRMATION_WINDOW_SIZE = "confirmation-window-size";
   public static String PRODUCER_WINDOW_SIZE = "producer-window-size";
   public static String CLIENT_FAILURE_CHECK_PERIOD = "client-failure-check-period";
   public static String USER = "user";
   public static String PASSWORD = "password";
   public static String CONNECTION_TTL = "connection-ttl";
   public static String MAX_RETRY_INTERVAL = "max-retry-interval";
   public static String MIN_LARGE_MESSAGE_SIZE = "min-large-message-size";
   public static String CALL_TIMEOUT = "call-timeout";
   public static String ROUTING_TYPE = "routing-type";
   public static String CONCURRENCY = "concurrency";

   private String name = null;

   private String queueName = null;

   private String forwardingAddress = null;

   private String filterString = null;

   private List<String> staticConnectors = null;

   private String discoveryGroupName = null;

   private boolean ha = false;

   private TransformerConfiguration transformerConfiguration = null;

   private long retryInterval = ActiveMQClient.DEFAULT_RETRY_INTERVAL;

   private double retryIntervalMultiplier = ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

   private int initialConnectAttempts = ActiveMQDefaultConfiguration.getDefaultBridgeInitialConnectAttempts();

   private int reconnectAttempts = ActiveMQDefaultConfiguration.getDefaultBridgeReconnectAttempts();

   private int reconnectAttemptsOnSameNode = ActiveMQDefaultConfiguration.getDefaultBridgeConnectSameNode();

   private boolean useDuplicateDetection = ActiveMQDefaultConfiguration.isDefaultBridgeDuplicateDetection();

   private int confirmationWindowSize = ActiveMQDefaultConfiguration.getDefaultBridgeConfirmationWindowSize();

   // disable flow control
   private int producerWindowSize = ActiveMQDefaultConfiguration.getDefaultBridgeProducerWindowSize();

   private long clientFailureCheckPeriod = ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

   private String user = ActiveMQDefaultConfiguration.getDefaultClusterUser();

   private String password = ActiveMQDefaultConfiguration.getDefaultClusterPassword();

   private long connectionTTL = ActiveMQClient.DEFAULT_CONNECTION_TTL;

   private long maxRetryInterval = ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL;

   private int minLargeMessageSize = ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   // At this point this is only changed on testcases
   // The bridge shouldn't be sending blocking anyways
   private long callTimeout = ActiveMQClient.DEFAULT_CALL_TIMEOUT;

   private ComponentConfigurationRoutingType routingType = ComponentConfigurationRoutingType.valueOf(ActiveMQDefaultConfiguration.getDefaultBridgeRoutingType());

   private int concurrency = ActiveMQDefaultConfiguration.getDefaultBridgeConcurrency();

   public BridgeConfiguration() {
   }

   public BridgeConfiguration(String name) {
      setName(name);
   }

   /**
    * Set the value of a parameter based on its "key" {@code String}. Valid key names and corresponding {@code static}
    * {@code final} are:
    * <p><ul>
    * <li>name: {@link #NAME}
    * <li>queue-name: {@link #QUEUE_NAME}
    * <li>forwarding-address: {@link #FORWARDING_ADDRESS}
    * <li>filter-string: {@link #FILTER_STRING}
    * <li>static-connectors: {@link #STATIC_CONNECTORS}
    * <li>discovery-group-name: {@link #DISCOVERY_GROUP_NAME}
    * <li>ha: {@link #HA}
    * <li>transformer-configuration: {@link #TRANSFORMER_CONFIGURATION}
    * <li>retry-interval: {@link #RETRY_INTERVAL}
    * <li>RETRY-interval-multiplier: {@link #RETRY_INTERVAL_MULTIPLIER}
    * <li>initial-connect-attempts: {@link #INITIAL_CONNECT_ATTEMPTS}
    * <li>reconnect-attempts: {@link #RECONNECT_ATTEMPTS}
    * <li>reconnect-attempts-on-same-node: {@link #RECONNECT_ATTEMPTS_ON_SAME_NODE}
    * <li>use-duplicate-detection: {@link #USE_DUPLICATE_DETECTION}
    * <li>confirmation-window-size: {@link #CONFIRMATION_WINDOW_SIZE}
    * <li>producer-window-size: {@link #PRODUCER_WINDOW_SIZE}
    * <li>client-failure-check-period: {@link #CLIENT_FAILURE_CHECK_PERIOD}
    * <li>user: {@link #USER}
    * <li>password: {@link #PASSWORD}
    * <li>connection-ttl: {@link #CONNECTION_TTL}
    * <li>max-retry-interval: {@link #MAX_RETRY_INTERVAL}
    * <li>min-large-message-size: {@link #MIN_LARGE_MESSAGE_SIZE}
    * <li>call-timeout: {@link #CALL_TIMEOUT}
    * <li>routing-type: {@link #ROUTING_TYPE}
    * <li>concurrency: {@link #CONCURRENCY}
    * </ul><p>
    * The {@code String}-based values will be converted to the proper value types based on the underlying property. For
    * example, if you pass the value "TRUE" for the key "auto-created" the {@code String} "TRUE" will be converted to
    * the {@code Boolean} {@code true}.
    *
    * @param key the key to set to the value
    * @param value the value to set for the key
    * @return this {@code BridgeConfiguration}
    */
   public BridgeConfiguration set(String key, String value) {
      if (key != null) {
         if (key.equals(NAME)) {
            setName(value);
         } else if (key.equals(QUEUE_NAME)) {
            setQueueName(value);
         } else if (key.equals(FORWARDING_ADDRESS)) {
            setForwardingAddress(value);
         } else if (key.equals(FILTER_STRING)) {
            setFilterString(value);
         } else if (key.equals(STATIC_CONNECTORS)) {
            // convert JSON array to string list
            List<String> stringList = JsonLoader.readArray(new StringReader(value)).stream()
                    .map(v -> ((JsonString) v).getString())
                    .collect(Collectors.toList());
            setStaticConnectors(stringList);
         } else if (key.equals(DISCOVERY_GROUP_NAME)) {
            setDiscoveryGroupName(value);
         } else if (key.equals(HA)) {
            setHA(Boolean.parseBoolean(value));
         } else if (key.equals(TRANSFORMER_CONFIGURATION)) {
            // create a transformer instance from a JSON string
            TransformerConfiguration transformerConfiguration = TransformerConfiguration.fromJSON(value);
            if (transformerConfiguration != null) {
               setTransformerConfiguration(transformerConfiguration);
            }
         } else if (key.equals(RETRY_INTERVAL)) {
            setRetryInterval(Long.parseLong(value));
         } else if (key.equals(RETRY_INTERVAL_MULTIPLIER)) {
            setRetryIntervalMultiplier(Double.parseDouble(value));
         } else if (key.equals(INITIAL_CONNECT_ATTEMPTS)) {
            setInitialConnectAttempts(Integer.parseInt(value));
         } else if (key.equals(RECONNECT_ATTEMPTS)) {
            setReconnectAttempts(Integer.parseInt(value));
         } else if (key.equals(RECONNECT_ATTEMPTS_ON_SAME_NODE)) {
            setReconnectAttemptsOnSameNode(Integer.parseInt(value));
         } else if (key.equals(USE_DUPLICATE_DETECTION)) {
            setUseDuplicateDetection(Boolean.parseBoolean(value));
         } else if (key.equals(CONFIRMATION_WINDOW_SIZE)) {
            setConfirmationWindowSize(Integer.parseInt(value));
         } else if (key.equals(PRODUCER_WINDOW_SIZE)) {
            setProducerWindowSize(Integer.parseInt(value));
         } else if (key.equals(CLIENT_FAILURE_CHECK_PERIOD)) {
            setClientFailureCheckPeriod(Long.parseLong(value));
         } else if (key.equals(USER)) {
            setUser(value);
         } else if (key.equals(PASSWORD)) {
            setPassword(value);
         } else if (key.equals(CONNECTION_TTL)) {
            setConnectionTTL(Long.parseLong(value));
         } else if (key.equals(MAX_RETRY_INTERVAL)) {
            setMaxRetryInterval(Long.parseLong(value));
         } else if (key.equals(MIN_LARGE_MESSAGE_SIZE)) {
            setMinLargeMessageSize(Integer.parseInt(value));
         } else if (key.equals(CALL_TIMEOUT)) {
            setCallTimeout(Long.parseLong(value));
         } else if (key.equals(ROUTING_TYPE)) {
            setRoutingType(ComponentConfigurationRoutingType.valueOf(value));
         } else if (key.equals(CONCURRENCY)) {
            setConcurrency(Integer.parseInt(value));
         }
      }
      return this;
   }

   public String getName() {
      return name;
   }

   /**
    * @param name the name to set
    */
   public BridgeConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   public String getQueueName() {
      return queueName;
   }

   /**
    * @param queueName the queueName to set
    */
   public BridgeConfiguration setQueueName(final String queueName) {
      this.queueName = queueName;
      return this;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL() {
      return connectionTTL;
   }

   public BridgeConfiguration setConnectionTTL(long connectionTTL) {
      this.connectionTTL = connectionTTL;
      return this;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval() {
      return maxRetryInterval;
   }

   public BridgeConfiguration setMaxRetryInterval(long maxRetryInterval) {
      this.maxRetryInterval = maxRetryInterval;
      return this;
   }

   public String getForwardingAddress() {
      return forwardingAddress;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public BridgeConfiguration setForwardingAddress(final String forwardingAddress) {
      this.forwardingAddress = forwardingAddress;
      return this;
   }

   public String getFilterString() {
      return filterString;
   }

   /**
    * @param filterString the filterString to set
    */
   public BridgeConfiguration setFilterString(final String filterString) {
      this.filterString = filterString;
      return this;
   }

   public TransformerConfiguration getTransformerConfiguration() {
      return transformerConfiguration;
   }

   /**
    * @param transformerConfiguration the transformerConfiguration to set
    */
   public BridgeConfiguration setTransformerConfiguration(final TransformerConfiguration transformerConfiguration) {
      this.transformerConfiguration = transformerConfiguration;
      return this;
   }

   public List<String> getStaticConnectors() {
      return staticConnectors;
   }

   /**
    * @param staticConnectors the staticConnectors to set
    */
   public BridgeConfiguration setStaticConnectors(final List<String> staticConnectors) {
      this.staticConnectors = staticConnectors;
      return this;
   }

   public String getDiscoveryGroupName() {
      return discoveryGroupName;
   }

   /**
    * @param discoveryGroupName the discoveryGroupName to set
    */
   public BridgeConfiguration setDiscoveryGroupName(final String discoveryGroupName) {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }

   public boolean isHA() {
      return ha;
   }

   /**
    * @param ha is the bridge supporting HA?
    */
   public BridgeConfiguration setHA(final boolean ha) {
      this.ha = ha;
      return this;
   }

   public long getRetryInterval() {
      return retryInterval;
   }

   /**
    * @param retryInterval the retryInterval to set
    */
   public BridgeConfiguration setRetryInterval(final long retryInterval) {
      this.retryInterval = retryInterval;
      return this;
   }

   public double getRetryIntervalMultiplier() {
      return retryIntervalMultiplier;
   }

   /**
    * @param retryIntervalMultiplier the retryIntervalMultiplier to set
    */
   public BridgeConfiguration setRetryIntervalMultiplier(final double retryIntervalMultiplier) {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      return this;
   }

   public int getInitialConnectAttempts() {
      return initialConnectAttempts;
   }

   /**
    * @param initialConnectAttempts the initialConnectAttempts to set
    */
   public BridgeConfiguration setInitialConnectAttempts(final int initialConnectAttempts) {
      this.initialConnectAttempts = initialConnectAttempts;
      return this;
   }

   public int getReconnectAttempts() {
      return reconnectAttempts;
   }

   /**
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public BridgeConfiguration setReconnectAttempts(final int reconnectAttempts) {
      this.reconnectAttempts = reconnectAttempts;
      return this;
   }

   public boolean isUseDuplicateDetection() {
      return useDuplicateDetection;
   }

   /**
    * @param useDuplicateDetection the useDuplicateDetection to set
    */
   public BridgeConfiguration setUseDuplicateDetection(final boolean useDuplicateDetection) {
      this.useDuplicateDetection = useDuplicateDetection;
      return this;
   }

   public int getConfirmationWindowSize() {
      return confirmationWindowSize;
   }

   /**
    * @param confirmationWindowSize the confirmationWindowSize to set
    */
   public BridgeConfiguration setConfirmationWindowSize(final int confirmationWindowSize) {
      this.confirmationWindowSize = confirmationWindowSize;
      return this;
   }

   public int getProducerWindowSize() {
      return producerWindowSize;
   }

   /**
    * @param producerWindowSize the producerWindowSize to set
    */
   public BridgeConfiguration setProducerWindowSize(final int producerWindowSize) {
      this.producerWindowSize = producerWindowSize;
      return this;
   }

   public long getClientFailureCheckPeriod() {
      return clientFailureCheckPeriod;
   }

   public BridgeConfiguration setClientFailureCheckPeriod(long clientFailureCheckPeriod) {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      return this;
   }

   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize() {
      return minLargeMessageSize;
   }

   public BridgeConfiguration setMinLargeMessageSize(int minLargeMessageSize) {
      this.minLargeMessageSize = minLargeMessageSize;
      return this;
   }

   public String getUser() {
      return user;
   }

   public BridgeConfiguration setUser(String user) {
      this.user = user;
      return this;
   }

   public String getPassword() {
      return password;
   }

   public BridgeConfiguration setPassword(String password) {
      this.password = password;
      return this;
   }

   /**
    * @return the callTimeout
    */
   public long getCallTimeout() {
      return callTimeout;
   }

   public int getReconnectAttemptsOnSameNode() {
      return reconnectAttemptsOnSameNode;
   }

   public BridgeConfiguration setReconnectAttemptsOnSameNode(int reconnectAttemptsOnSameNode) {
      this.reconnectAttemptsOnSameNode = reconnectAttemptsOnSameNode;
      return this;
   }

   public ComponentConfigurationRoutingType getRoutingType() {
      return routingType;
   }

   public BridgeConfiguration setRoutingType(ComponentConfigurationRoutingType routingType) {
      this.routingType = routingType;
      return this;
   }

   /**
    * @return the bridge concurrency
    */
   public int getConcurrency() {
      return concurrency;
   }

   /**
    * @param concurrency the bridge concurrency to set
    */
   public BridgeConfiguration setConcurrency(int concurrency) {
      this.concurrency = concurrency;
      return this;
   }

   /**
    * At this point this is only changed on testcases
    * The bridge shouldn't be sending blocking anyways
    *
    * @param callTimeout the callTimeout to set
    */
   public BridgeConfiguration setCallTimeout(long callTimeout) {
      this.callTimeout = callTimeout;
      return this;
   }

   /**
    * This method returns a JSON-formatted {@code String} representation of this {@code BridgeConfiguration}. It is a
    * simple collection of key/value pairs. The keys used are referenced in {@link #set(String, String)}.
    *
    * @return a JSON-formatted {@code String} representation of this {@code BridgeConfiguration}
    */
   public String toJSON() {
      JsonObjectBuilder builder = JsonLoader.createObjectBuilder();

      // string fields which default to null (only serialize if value is not null)

      if (getName() != null) {
         builder.add(NAME, getName());
      }
      if (getQueueName() != null) {
         builder.add(QUEUE_NAME, getQueueName());
      }
      if (getForwardingAddress() != null) {
         builder.add(FORWARDING_ADDRESS, getForwardingAddress());
      }
      if (getFilterString() != null) {
         builder.add(FILTER_STRING, getFilterString());
      }
      if (getDiscoveryGroupName() != null) {
         builder.add(DISCOVERY_GROUP_NAME, getDiscoveryGroupName());
      }

      // string fields which default to non-null values (always serialize)

      addNullable(builder, USER, getUser());
      addNullable(builder, PASSWORD, getPassword());

      // primitive data type fields (always serialize)

      builder.add(HA, isHA());
      builder.add(RETRY_INTERVAL, getRetryInterval());
      builder.add(RETRY_INTERVAL_MULTIPLIER, getRetryIntervalMultiplier());
      builder.add(INITIAL_CONNECT_ATTEMPTS, getInitialConnectAttempts());
      builder.add(RECONNECT_ATTEMPTS, getReconnectAttempts());
      builder.add(RECONNECT_ATTEMPTS_ON_SAME_NODE, getReconnectAttemptsOnSameNode());
      builder.add(USE_DUPLICATE_DETECTION, isUseDuplicateDetection());
      builder.add(CONFIRMATION_WINDOW_SIZE, getConfirmationWindowSize());
      builder.add(PRODUCER_WINDOW_SIZE, getProducerWindowSize());
      builder.add(CLIENT_FAILURE_CHECK_PERIOD, getClientFailureCheckPeriod());
      builder.add(CONNECTION_TTL, getConnectionTTL());
      builder.add(MAX_RETRY_INTERVAL, getMaxRetryInterval());
      builder.add(MIN_LARGE_MESSAGE_SIZE, getMinLargeMessageSize());
      builder.add(CALL_TIMEOUT, getCallTimeout());
      builder.add(CONCURRENCY, getConcurrency());

      // complex fields (only serialize if value is not null)

      if (getRoutingType() != null) {
         builder.add(ROUTING_TYPE, getRoutingType().name());
      }

      final List<String> staticConnectors = getStaticConnectors();
      if (staticConnectors != null) {
         JsonArrayBuilder arrayBuilder = JsonLoader.createArrayBuilder();
         staticConnectors.forEach(arrayBuilder::add);
         builder.add(STATIC_CONNECTORS, arrayBuilder);
      }

      TransformerConfiguration tc = getTransformerConfiguration();
      if (tc != null) {
         JsonObjectBuilder tcBuilder = JsonLoader.createObjectBuilder().add(TransformerConfiguration.CLASS_NAME, tc.getClassName());
         if (tc.getProperties() != null && tc.getProperties().size() > 0) {
            JsonObjectBuilder propBuilder = JsonLoader.createObjectBuilder();
            tc.getProperties().forEach(propBuilder::add);
            tcBuilder.add(TransformerConfiguration.PROPERTIES, propBuilder);
         }
         builder.add(TRANSFORMER_CONFIGURATION, tcBuilder);
      }

      return builder.build().toString();
   }

   private static void addNullable(JsonObjectBuilder builder, String key, String value) {
      if (value == null) {
         builder.addNull(key);
      } else {
         builder.add(key, value);
      }
   }

   /**
    * This method returns a {@code BridgeConfiguration} created from the JSON-formatted input {@code String}. The input
    * should be a simple object of key/value pairs. Valid keys are referenced in {@link #set(String, String)}.
    *
    * @param jsonString json string
    * @return the {@code BridgeConfiguration} created from the JSON-formatted input {@code String}
    */
   public static BridgeConfiguration fromJSON(String jsonString) {
      JsonObject json = JsonLoader.readObject(new StringReader(jsonString));

      // name is the only required value
      if (!json.containsKey(NAME)) {
         return null;
      }
      BridgeConfiguration result = new BridgeConfiguration(json.getString(NAME));

      for (Map.Entry<String, JsonValue> entry : json.entrySet()) {
         if (entry.getValue().getValueType() == JsonValue.ValueType.STRING) {
            result.set(entry.getKey(), ((JsonString) entry.getValue()).getString());
         } else if (entry.getValue().getValueType() == JsonValue.ValueType.NULL) {
            result.set(entry.getKey(), null);
         } else {
            result.set(entry.getKey(), entry.getValue().toString());
         }
      }

      return result;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (callTimeout ^ (callTimeout >>> 32));
      result = prime * result + (int) (clientFailureCheckPeriod ^ (clientFailureCheckPeriod >>> 32));
      result = prime * result + confirmationWindowSize;
      result = prime * result + producerWindowSize;
      result = prime * result + (int) (connectionTTL ^ (connectionTTL >>> 32));
      result = prime * result + ((discoveryGroupName == null) ? 0 : discoveryGroupName.hashCode());
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((forwardingAddress == null) ? 0 : forwardingAddress.hashCode());
      result = prime * result + (ha ? 1231 : 1237);
      result = prime * result + (int) (maxRetryInterval ^ (maxRetryInterval >>> 32));
      result = prime * result + minLargeMessageSize;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((password == null) ? 0 : password.hashCode());
      result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
      result = prime * result + initialConnectAttempts;
      result = prime * result + reconnectAttempts;
      result = prime * result + (int) (retryInterval ^ (retryInterval >>> 32));
      long temp;
      temp = Double.doubleToLongBits(retryIntervalMultiplier);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      result = prime * result + ((staticConnectors == null) ? 0 : staticConnectors.hashCode());
      result = prime * result + ((transformerConfiguration == null) ? 0 : transformerConfiguration.hashCode());
      result = prime * result + (useDuplicateDetection ? 1231 : 1237);
      result = prime * result + ((user == null) ? 0 : user.hashCode());
      result = prime * result + concurrency;
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      BridgeConfiguration other = (BridgeConfiguration) obj;
      if (callTimeout != other.callTimeout)
         return false;
      if (clientFailureCheckPeriod != other.clientFailureCheckPeriod)
         return false;
      if (confirmationWindowSize != other.confirmationWindowSize)
         return false;
      if (producerWindowSize != other.producerWindowSize)
         return false;
      if (connectionTTL != other.connectionTTL)
         return false;
      if (discoveryGroupName == null) {
         if (other.discoveryGroupName != null)
            return false;
      } else if (!discoveryGroupName.equals(other.discoveryGroupName))
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      } else if (!filterString.equals(other.filterString))
         return false;
      if (forwardingAddress == null) {
         if (other.forwardingAddress != null)
            return false;
      } else if (!forwardingAddress.equals(other.forwardingAddress))
         return false;
      if (ha != other.ha)
         return false;
      if (maxRetryInterval != other.maxRetryInterval)
         return false;
      if (minLargeMessageSize != other.minLargeMessageSize)
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (password == null) {
         if (other.password != null)
            return false;
      } else if (!password.equals(other.password))
         return false;
      if (queueName == null) {
         if (other.queueName != null)
            return false;
      } else if (!queueName.equals(other.queueName))
         return false;
      if (initialConnectAttempts != other.initialConnectAttempts)
         return false;
      if (reconnectAttempts != other.reconnectAttempts)
         return false;
      if (retryInterval != other.retryInterval)
         return false;
      if (Double.doubleToLongBits(retryIntervalMultiplier) != Double.doubleToLongBits(other.retryIntervalMultiplier))
         return false;
      if (staticConnectors == null) {
         if (other.staticConnectors != null)
            return false;
      } else if (!staticConnectors.equals(other.staticConnectors))
         return false;
      if (transformerConfiguration == null) {
         if (other.transformerConfiguration != null)
            return false;
      } else if (!transformerConfiguration.equals(other.transformerConfiguration))
         return false;
      if (useDuplicateDetection != other.useDuplicateDetection)
         return false;
      if (user == null) {
         if (other.user != null)
            return false;
      } else if (!user.equals(other.user))
         return false;
      if (concurrency != other.concurrency)
         return false;
      return true;
   }

}
