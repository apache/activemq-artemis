/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.junit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Map;

/**
 * A JUnit Rule that embeds an dynamic (i.e. unbound) ActiveMQ Artemis ClientProducer into a test.
 * <p>
 * This JUnit Rule is designed to simplify using ActiveMQ Artemis clients in unit tests.  Adding the rule to a test will startup
 * an unbound ClientProducer, which can then be used to feed messages to any address on the ActiveMQ Artemis server.
 *
 * <pre><code>
 * public class SimpleTest {
 *     &#64;Rule
 *     public ActiveMQDynamicProducerResource producer = new ActiveMQDynamicProducerResource( "vm://0");
 *
 *     &#64;Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded ClientProducer here
 *         producer.sendMessage( "test.address", "String Body" );
 *     }
 * }
 * </code></pre>
 */
public class ActiveMQDynamicProducerExtension implements BeforeAllCallback, AfterAllCallback {

    private ActiveMQDynamicProducerDelegate activeMQDynamicProducerDelegate;

    public ActiveMQDynamicProducerExtension(String url, String username, String password) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(url, username, password);
    }

    public ActiveMQDynamicProducerExtension(String url) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(url);
    }

    public ActiveMQDynamicProducerExtension(ServerLocator serverLocator, String username, String password) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(serverLocator, username, password);
    }

    public ActiveMQDynamicProducerExtension(ServerLocator serverLocator) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(serverLocator);
    }

    public ActiveMQDynamicProducerExtension(String url, SimpleString address, String username, String password) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(url, address, username, password);
    }

    public ActiveMQDynamicProducerExtension(String url, SimpleString address) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(url, address);
    }

    public ActiveMQDynamicProducerExtension(ServerLocator serverLocator, SimpleString address, String username, String password) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(serverLocator, address, username, password);
    }

    public ActiveMQDynamicProducerExtension(ServerLocator serverLocator, SimpleString address) {
        this.activeMQDynamicProducerDelegate = new ActiveMQDynamicProducerDelegate(serverLocator, address);
    }

    /**
     * Invoked by JUnit to setup the resource - start the embedded ActiveMQ Artemis server
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        activeMQDynamicProducerDelegate.start();
    }

    /**
     * Invoked by JUnit to tear down the resource - stops the embedded ActiveMQ Artemis server
     */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        activeMQDynamicProducerDelegate.stop();
    }

    public void createClient() {
        activeMQDynamicProducerDelegate.createClient();
    }

    public void sendMessage(ClientMessage message) {
        activeMQDynamicProducerDelegate.sendMessage(message);
    }

    public void sendMessage(SimpleString targetAddress, ClientMessage message) {
        activeMQDynamicProducerDelegate.sendMessage(targetAddress, message);
    }

    public ClientMessage sendMessage(SimpleString targetAddress, byte[] body) {
        return activeMQDynamicProducerDelegate.sendMessage(targetAddress, body);
    }

    public ClientMessage sendMessage(SimpleString targetAddress, String body) {
        return activeMQDynamicProducerDelegate.sendMessage(targetAddress, body);
    }

    public ClientMessage sendMessage(SimpleString targetAddress, Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.sendMessage(targetAddress, properties);
    }

    public ClientMessage sendMessage(SimpleString targetAddress, byte[] body, Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.sendMessage(targetAddress, body, properties);
    }

    public ClientMessage sendMessage(SimpleString targetAddress, String body, Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.sendMessage(targetAddress, body, properties);
    }

    public boolean isUseDurableMessage() {
        return activeMQDynamicProducerDelegate.isUseDurableMessage();
    }

    public void setUseDurableMessage(boolean useDurableMessage) {
        activeMQDynamicProducerDelegate.setUseDurableMessage(useDurableMessage);
    }

    public void stopClient() {
        activeMQDynamicProducerDelegate.stopClient();
    }

    public ClientMessage createMessage() {
        return activeMQDynamicProducerDelegate.createMessage();
    }

    public ClientMessage createMessage(byte[] body) {
        return activeMQDynamicProducerDelegate.createMessage(body);
    }

    public ClientMessage createMessage(String body) {
        return activeMQDynamicProducerDelegate.createMessage(body);
    }

    public ClientMessage createMessage(Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.createMessage(properties);
    }

    public ClientMessage createMessage(byte[] body, Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.createMessage(body, properties);
    }

    public ClientMessage createMessage(String body, Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.createMessage(body, properties);
    }

    public ClientMessage sendMessage(byte[] body) {
        return activeMQDynamicProducerDelegate.sendMessage(body);
    }

    public ClientMessage sendMessage(String body) {
        return activeMQDynamicProducerDelegate.sendMessage(body);
    }

    public ClientMessage sendMessage(Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.sendMessage(properties);
    }

    public ClientMessage sendMessage(byte[] body, Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.sendMessage(body, properties);
    }

    public ClientMessage sendMessage(String body, Map<String, Object> properties) {
        return activeMQDynamicProducerDelegate.sendMessage(body, properties);
    }

    public static void addMessageProperties(ClientMessage message, Map<String, Object> properties) {
        AbstractActiveMQClientDelegate.addMessageProperties(message, properties);
    }

    public boolean isAutoCreateQueue() {
        return activeMQDynamicProducerDelegate.isAutoCreateQueue();
    }

    public void setAutoCreateQueue(boolean autoCreateQueue) {
        activeMQDynamicProducerDelegate.setAutoCreateQueue(autoCreateQueue);
    }
}
